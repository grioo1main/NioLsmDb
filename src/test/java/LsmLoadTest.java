import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import luminar.ru.lsm.LsmStorage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Комплексные нагрузочные тесты (Benchmarking) для проверки пропускной способности (Throughput)
 * и задержек (Latency) хранилища LSM-Tree. Генерирует отчеты в формате CSV.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class LsmLoadTest {

    @TempDir
    Path tempDir;

    private LsmStorage storage;
    private static PerfLogger perfLogger;

    @BeforeAll
    static void setUpLogger() throws IOException {
        perfLogger = new PerfLogger(Path.of("perf_logs"), "lsm_bench2.csv");
    }

    private void createStorage(int threshold) {
        storage = new LsmStorage(tempDir, threshold);
    }

    private void closeStorage() {
        if (storage != null) {
            storage.close();
        }
    }

    private void cleanDirectory() throws IOException {
        try (var stream = Files.walk(tempDir)) {
            stream.sorted(Comparator.reverseOrder())
                  .forEach(p -> {
                      try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                  });
        }
    }

    private long countSstFiles() throws IOException {
        try (var stream = Files.list(tempDir)) {
            return stream.filter(p -> p.toString().endsWith(".db")).count();
        }
    }

    /**
     * Потокобезопасный сборщик задержек (наносекунд) для вычисления перцентилей (P50, P95, P99).
     */
    static class LatencyCollector {
        private final List<Long> latencies = new ArrayList<>();

        synchronized void add(long nanos) {
            latencies.add(nanos);
        }

        List<Long> getSorted() {
            synchronized (this) {
                ArrayList<Long> copy = new ArrayList<>(latencies);
                Collections.sort(copy);
                return copy;
            }
        }
    }

    static double percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0;
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, index)) / 1_000_000.0;
    }

    private void printAndLog(String title, long ops, long durationMs, LatencyCollector latCollector) throws IOException {
        double throughput = ops / (durationMs / 1000.0);
        List<Long> sorted = latCollector.getSorted();
        
        System.out.printf("%s:%n", title);
        System.out.printf("  Operations: %d | Duration: %d ms | Throughput: %.0f ops/s%n", ops, durationMs, throughput);
        
        if (!sorted.isEmpty()) {
            System.out.printf("  Latency P50: %.3f ms | P95: %.3f ms | P99: %.3f ms%n",
                    percentile(sorted, 50), percentile(sorted, 95), percentile(sorted, 99));
        }
        perfLogger.logStats(title, ops, durationMs, latCollector);
    }

    @Test
    @Order(1)
    void writeOnlyTest() throws Exception {
        int[] thresholds = {10000, 50000, 100000};
        int totalOps = 1_000_000;

        for (int threshold : thresholds) {
            cleanDirectory();
            createStorage(threshold);

            LatencyCollector latCollector = new LatencyCollector();
            AtomicLong opsCount = new AtomicLong();
            long start = System.nanoTime();

            Thread writer = new Thread(() -> {
                for (int i = 0; i < totalOps; i++) {
                    long t0 = System.nanoTime();
                    storage.put("key_" + i, "value_" + i);
                    long t1 = System.nanoTime();
                    latCollector.add(t1 - t0);
                    opsCount.incrementAndGet();
                }
            });
            
            writer.start();
            writer.join();

            long durationMs = (System.nanoTime() - start) / 1_000_000;
            printAndLog("WriteOnly (threshold=" + threshold + ")", opsCount.get(), durationMs, latCollector);
            closeStorage();
        }
    }

    @Test
    @Order(2)
    void readTest() throws Exception {
        int totalKeys = 100_000;
        createStorage(50000);

        for (int i = 0; i < totalKeys; i++) {
            storage.put("key_" + i, "value_" + i);
        }
        Thread.sleep(2000); // Ожидание flush

        LatencyCollector latCollector = new LatencyCollector();
        Random rand = new Random(42);
        int readCount = 50_000;
        
        long start = System.nanoTime();
        for (int i = 0; i < readCount; i++) {
            String key = "key_" + rand.nextInt(totalKeys);
            long t0 = System.nanoTime();
            storage.get(key);
            long t1 = System.nanoTime();
            latCollector.add(t1 - t0);
        }
        long durationMs = (System.nanoTime() - start) / 1_000_000;
        printAndLog("ReadOnly (existing keys)", readCount, durationMs, latCollector);

        latCollector = new LatencyCollector();
        start = System.nanoTime();
        for (int i = 0; i < readCount; i++) {
            String key = "missing_" + rand.nextInt(totalKeys);
            long t0 = System.nanoTime();
            storage.get(key); // Должно отсекаться Bloom Filter'ом
            long t1 = System.nanoTime();
            latCollector.add(t1 - t0);
        }
        durationMs = (System.nanoTime() - start) / 1_000_000;
        printAndLog("ReadOnly (nonexistent keys)", readCount, durationMs, latCollector);

        closeStorage();
    }

    @Test
    @Order(3)
    void mixedReadWriteTest() throws Exception {
        int preload = 100_000;
        createStorage(50000);
        for (int i = 0; i < preload; i++) storage.put("key_" + i, "value_" + i);
        Thread.sleep(2000);

        int testDurationSec = 30;
        ExecutorService executor = Executors.newFixedThreadPool(4); // 2 readers, 2 writers
        AtomicLong writeOps = new AtomicLong();
        AtomicLong readOps = new AtomicLong();
        LatencyCollector writeLat = new LatencyCollector();
        LatencyCollector readLat = new LatencyCollector();
        Random rand = new Random();

        CountDownLatch startLatch = new CountDownLatch(1);

        for (int w = 0; w < 2; w++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    int base = preload + Thread.currentThread().hashCode();
                    while (!Thread.currentThread().isInterrupted()) {
                        long t0 = System.nanoTime();
                        storage.put("mix_key_" + (base++), "value");
                        writeLat.add(System.nanoTime() - t0);
                        writeOps.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {}
            });
        }

        for (int r = 0; r < 2; r++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (!Thread.currentThread().isInterrupted()) {
                        String key = "key_" + rand.nextInt(preload);
                        long t0 = System.nanoTime();
                        storage.get(key);
                        readLat.add(System.nanoTime() - t0);
                        readOps.incrementAndGet();
                    }
                } catch (InterruptedException ignored) {}
            });
        }

        startLatch.countDown();
        Thread.sleep(testDurationSec * 1000L);
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        printAndLog("Mixed Write", writeOps.get(), testDurationSec * 1000L, writeLat);
        printAndLog("Mixed Read", readOps.get(), testDurationSec * 1000L, readLat);
        closeStorage();
    }

    @Test
    @Order(4)
    void enduranceTest() throws Exception {
        int threshold = 50000;
        int totalKeys = 1_000_000;
        createStorage(threshold);

        int checkpointInterval = 50_000;
        long lastTime = System.nanoTime();
        AtomicLong opsSinceCheckpoint = new AtomicLong();

        for (int i = 0; i < totalKeys; i++) {
            storage.put("endurance_key_" + i, "value_" + i);
            opsSinceCheckpoint.incrementAndGet();

            if ((i + 1) % checkpointInterval == 0) {
                long now = System.nanoTime();
                long durationMs = (now - lastTime) / 1_000_000;
                double throughput = opsSinceCheckpoint.get() / (durationMs / 1000.0);
                
                perfLogger.log("endurance_checkpoint_" + ((i+1)/checkpointInterval),
                        opsSinceCheckpoint.get(), durationMs, throughput, 0, 0, 0);

                lastTime = now;
                opsSinceCheckpoint.set(0);
            }
        }
        closeStorage();
    }

    /**
     * Утилита для логирования метрик в CSV файл.
     */
    public static class PerfLogger {
        private final Path csvFile;

        public PerfLogger(Path dir, String fileName) throws IOException {
            Files.createDirectories(dir);
            this.csvFile = dir.resolve(fileName);
            if (!Files.exists(csvFile) || Files.size(csvFile) == 0) {
                String header = "timestamp;test;operations;duration_ms;throughput_ops_s;p50_ms;p95_ms;p99_ms\n";
                Files.writeString(csvFile, header, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }

        public void log(String testName, long ops, long durationMs, double throughput,
                        double p50, double p95, double p99) throws IOException {
            String line = String.format(Locale.US, "%s;%s;%d;%d;%.0f;%.3f;%.3f;%.3f\n",
                    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_TIME),
                    testName, ops, durationMs, throughput, p50, p95, p99);
            Files.writeString(csvFile, line, StandardOpenOption.APPEND);
        }

        public void logStats(String testName, long ops, long durationMs, LatencyCollector collector) throws IOException {
            double throughput = ops / (durationMs / 1000.0);
            List<Long> sorted = collector.getSorted();
            log(testName, ops, durationMs, throughput,
                    LsmLoadTest.percentile(sorted, 50),
                    LsmLoadTest.percentile(sorted, 95),
                    LsmLoadTest.percentile(sorted, 99));
        }
    }
}