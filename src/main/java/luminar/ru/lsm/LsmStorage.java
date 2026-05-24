package luminar.ru.lsm;

import luminar.ru.wal.WalReader;
import luminar.ru.wal.WalWriter;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Основной движок LSM-хранилища (Log-Structured Merge-Tree).
 * Обеспечивает потокобезопасные операции записи, чтения и удаления.
 *
 * Удаление реализовано через маркер-удаления (tombstone):
 * вместо физического удаления записи в MemTable и WAL сохраняется
 * специальное значение TOMBSTONE. При чтении наличие tombstone
 * означает отсутствие ключа. Compaction вычищает tombstone-записи.
 */
public class LsmStorage {

    private static final Logger log = Logger.getLogger(LsmStorage.class.getName());

    /**
     * Маркер удаления (tombstone). Хранится как значение для удалённых ключей.
     * При чтении наличие этого маркера трактуется как «ключ отсутствует».
     */
    public static final String TOMBSTONE = "\u0000TOMBSTONE\u0000";

    private static final int COMPACTION_THRESHOLD = 4;
    private static final int COMPACTION_BATCH_SIZE = 3;
    private static final long FLUSH_TIMEOUT_SEC = 5;
    private static final long COMPACT_TIMEOUT_SEC = 10;

    private final AtomicReference<ConcurrentSkipListMap<String, String>> memTable = new AtomicReference<>(new ConcurrentSkipListMap<>());
    private final AtomicReference<ConcurrentSkipListMap<String, String>> immutableMemTable = new AtomicReference<>(null);
    private final AtomicReference<List<SstFile>> sstFiles = new AtomicReference<>(new ArrayList<>());

    private final AtomicInteger memTableSize = new AtomicInteger(0);
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final AtomicLong fileCounter = new AtomicLong(0);
    private final AtomicBoolean flushing = new AtomicBoolean(false);

    private final ExecutorService flushExecutor;
    private final ExecutorService compactExecutor;
    private final Path storageDir;
    private final int flushThreshold;

    private WalWriter walWriter;
    private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    public LsmStorage(Path storageDir, int threshold) {
        this.storageDir = storageDir;
        this.flushThreshold = threshold;

        this.flushExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lsm-flush-thread");
            t.setDaemon(true);
            return t;
        });

        this.compactExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lsm-compact-thread");
            t.setDaemon(true);
            return t;
        });

        init();
    }

    /**
     * Инициализация хранилища: создание директорий, восстановление данных из WAL
     * после сбоя и загрузка существующих SSTable в память.
     */
    private void init() {
        try {
            if (!Files.exists(storageDir)) {
                Files.createDirectories(storageDir);
            }

            Path walPath = storageDir.resolve("wal.log");
            if (Files.exists(walPath) && Files.size(walPath) > 0) {
                log.info("WAL file found. Starting recovery process...");
                try (WalReader reader = new WalReader(walPath)) {
                    ConcurrentSkipListMap<String, String> recovered = reader.recover();
                    if (!recovered.isEmpty()) {
                        memTable.get().putAll(recovered);
                        memTableSize.set(recovered.size());
                        log.info("Successfully recovered " + recovered.size() + " records from WAL.");
                    }
                } catch (Exception e) {
                    log.log(Level.WARNING, "Error during WAL recovery", e);
                }
            }

            this.walWriter = new WalWriter(walPath);

            List<Path> dbFiles;
            try (Stream<Path> stream = Files.list(storageDir)) {
                dbFiles = stream
                        .filter(p -> p.toString().endsWith(".db"))
                        .sorted(Comparator.reverseOrder())
                        .toList();
            }

            List<SstFile> loaded = new ArrayList<>();
            for (Path path : dbFiles) {
                try {
                    loaded.add(SstFile.open(path));
                    log.fine("Loaded SSTable: " + path.getFileName());
                } catch (Exception e) {
                    log.log(Level.WARNING, "Error loading file " + path, e);
                }
            }
            sstFiles.set(loaded);
            log.info("Storage initialized. Loaded SSTables: " + loaded.size());

        } catch (IOException e) {
            throw new RuntimeException("Critical storage initialization error", e);
        }
    }

    /**
     * Записывает пару ключ-значение в базу.
     * Сначала сохраняет в WAL для надежности, затем в MemTable.
     */
    public void put(String key, String value) {
        if (isClosed.get()) throw new IllegalStateException("Storage is closed");
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");
        if (TOMBSTONE.equals(value)) throw new IllegalArgumentException("Value matches internal tombstone marker");

        writeToWalAndMemTable(key, value);
    }

    /**
     * Удаляет ключ из хранилища посредством записи маркера-удаления (tombstone).
     * Физическое удаление происходит при следующем compaction.
     *
     * Вызов delete на несуществующем ключе безопасен и не является ошибкой.
     */
    public void delete(String key) {
        if (isClosed.get()) throw new IllegalStateException("Storage is closed");
        Objects.requireNonNull(key, "Key cannot be null");

        writeToWalAndMemTable(key, TOMBSTONE);
    }

    private void writeToWalAndMemTable(String key, String value) {
        try {
            walWriter.append(key, value);
        } catch (IOException e) {
            throw new RuntimeException("Error writing to WAL", e);
        }

        memTable.get().put(key, value);
        int newSize = memTableSize.incrementAndGet();

        if (newSize >= flushThreshold) {
            if (flushing.compareAndSet(false, true)) {
                triggerFlush();
            }
        }
    }

    /**
     * Ищет значение по ключу согласно иерархии LSM-дерева:
     * 1. Актуальная MemTable
     * 2. Immutable MemTable (в процессе сброса)
     * 3. SSTables (от новых к старым)
     *
     * Если найден tombstone — возвращает null (ключ удалён).
     */
    public String get(String key) {
        if (isClosed.get()) throw new IllegalStateException("Storage is closed");
        Objects.requireNonNull(key);

        String val = memTable.get().get(key);
        if (val != null) return isTombstone(val) ? null : val;

        ConcurrentSkipListMap<String, String> imm = immutableMemTable.get();
        if (imm != null) {
            val = imm.get(key);
            if (val != null) return isTombstone(val) ? null : val;
        }

        for (SstFile sst : sstFiles.get()) {
            try {
                val = sst.get(key);
                if (val != null) return isTombstone(val) ? null : val;
            } catch (IOException e) {
                log.log(Level.WARNING, "Error reading from SSTable " + sst.getPath(), e);
            }
        }
        return null;
    }

    /**
     * Проверяет, является ли значение маркером удаления.
     */
    public static boolean isTombstone(String value) {
        return TOMBSTONE.equals(value);
    }

    /**
     * Безопасное завершение работы (Graceful Shutdown).
     * Сбрасывает все данные из памяти на диск и закрывает файлы.
     */
    public void close() {
        if (!isClosed.compareAndSet(false, true)) return;

        closeLock.writeLock().lock();
        try {
            ConcurrentSkipListMap<String, String> lastMem = memTable.getAndSet(new ConcurrentSkipListMap<>());
            ConcurrentSkipListMap<String, String> lastImm = immutableMemTable.getAndSet(null);

            if (lastImm != null && !lastImm.isEmpty()) flushSync(lastImm);
            if (lastMem != null && !lastMem.isEmpty()) flushSync(lastMem);

            if (walWriter != null) {
                walWriter.truncate();
                walWriter.close();
            }

            flushExecutor.shutdown();
            compactExecutor.shutdown();

            if (!flushExecutor.awaitTermination(FLUSH_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
            if (!compactExecutor.awaitTermination(COMPACT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                compactExecutor.shutdownNow();
            }

            for (SstFile sst : sstFiles.get()) {
                try { sst.close(); } catch (IOException ignored) {}
            }
            log.info("LsmStorage successfully stopped.");
        } catch (InterruptedException e) {
            flushExecutor.shutdownNow();
            compactExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error closing storage", e);
        } finally {
            closeLock.writeLock().unlock();
        }
    }

    private void triggerFlush() {
        ConcurrentSkipListMap<String, String> oldTable = memTable.getAndSet(new ConcurrentSkipListMap<>());
        immutableMemTable.set(oldTable);
        int oldSize = oldTable.size();
        memTableSize.addAndGet(-oldSize);

        flushExecutor.submit(() -> {
            try {
                flushToFile(oldTable);
            } catch (Exception e) {
                log.log(Level.SEVERE, "MemTable flush failure", e);
            } finally {
                immutableMemTable.set(null);
                flushing.set(false);

                if (memTableSize.get() >= flushThreshold) {
                    if (flushing.compareAndSet(false, true)) {
                        triggerFlush();
                    }
                }
            }
        });
    }

    private void flushToFile(ConcurrentSkipListMap<String, String> table) throws IOException {
        if (table == null || table.isEmpty()) return;

        long timestamp = System.currentTimeMillis();
        long cnt = fileCounter.incrementAndGet();
        Path path = storageDir.resolve("sstable_" + timestamp + "_" + cnt + ".db");

        log.info("Flushing to disk: " + table.size() + " keys to " + path.getFileName());

        SstWriter.write(path, table);
        SstFile newSst = SstFile.open(path);

        sstFiles.updateAndGet(current -> {
            List<SstFile> updated = new ArrayList<>(current);
            updated.add(0, newSst);
            return updated;
        });

        walWriter.truncate();

        if (!isClosed.get()) {
            scheduleCompaction();
        }
    }

    private void flushSync(ConcurrentSkipListMap<String, String> table) throws IOException {
        if (table == null || table.isEmpty()) return;

        long timestamp = System.currentTimeMillis();
        long cnt = fileCounter.incrementAndGet();
        Path path = storageDir.resolve("sstable_" + timestamp + "_" + cnt + ".db");

        log.info("Synchronous flush to disk: " + table.size() + " keys to " + path.getFileName());

        SstWriter.write(path, table);
        SstFile newSst = SstFile.open(path);

        sstFiles.updateAndGet(current -> {
            List<SstFile> updated = new ArrayList<>(current);
            updated.add(0, newSst);
            return updated;
        });
    }

    private void scheduleCompaction() {
        List<SstFile> current = sstFiles.get();
        if (current.size() < COMPACTION_THRESHOLD) return;

        int total = current.size();
        int from = Math.max(0, total - COMPACTION_BATCH_SIZE);
        List<SstFile> toCompact = new ArrayList<>(current.subList(from, total));
        Collections.reverse(toCompact); // Сортировка от старых к новым

        List<Path> paths = new ArrayList<>();
        for (SstFile sst : toCompact) {
            paths.add(sst.getPath());
        }

        compactExecutor.submit(() -> {
            if (isClosed.get()) return;

            try {
                log.info("Starting background compaction for " + paths.size() + " files...");
                List<SstFile> filesToCompact = new ArrayList<>();
                for (Path p : paths) {
                    if (Files.exists(p)) {
                        filesToCompact.add(SstFile.open(p));
                    } else {
                        log.warning("File not found, skipping: " + p.getFileName());
                    }
                }

                if (filesToCompact.size() < 2) {
                    log.fine("Not enough files for compaction, skipping.");
                    return;
                }

                Compactor compactor = new Compactor();
                SstFile merged = compactor.submit(filesToCompact, storageDir);
                if (merged == null) {
                    log.warning("Compaction result is empty, skipping.");
                    return;
                }

                applyCompaction(filesToCompact, merged);
                log.info("Compaction completed. New file created: " + merged.getPath().getFileName());

            } catch (Exception e) {
                log.log(Level.SEVERE, "Compaction failure", e);
            }
        });
    }

    private void applyCompaction(List<SstFile> oldFiles, SstFile newFile) {
        while (true) {
            List<SstFile> current = sstFiles.get();
            List<SstFile> updated = new ArrayList<>(current);

            updated.removeIf(sst -> {
                for (SstFile old : oldFiles) {
                    if (sst.getPath().equals(old.getPath())) return true;
                }
                return false;
            });
            updated.add(newFile);

            if (sstFiles.compareAndSet(current, updated)) {
                for (SstFile old : oldFiles) {
                    try {
                        old.close();
                        Files.deleteIfExists(old.getPath());
                        log.fine("Deleted old file: " + old.getPath().getFileName());
                    } catch (Exception e) {
                        log.log(Level.WARNING, "Failed to delete file " + old.getPath().getFileName(), e);
                    }
                }
                break;
            }
        }
    }
}