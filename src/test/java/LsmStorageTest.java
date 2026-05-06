import luminar.ru.lsm.LsmStorage;
import luminar.ru.lsm.SstFile;
import luminar.ru.lsm.SstWriter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class LsmStorageTest {

    @TempDir
    Path tempDir;

    private LsmStorage storage;
    private static final int SMALL_THRESHOLD = 5;

    @BeforeEach
    void setUp() {
        storage = new LsmStorage(tempDir, SMALL_THRESHOLD);
    }

    @AfterEach
    void tearDown() {
        if (storage != null) {
            storage.close();
        }
    }

    // ====================== ТЕСТЫ ======================

    @Test
    void testBasicPutAndGet() {
        storage.put("key1", "value1");
        storage.put("key2", "value2");

        assertEquals("value1", storage.get("key1"));
        assertEquals("value2", storage.get("key2"));
        assertNull(storage.get("nonexistent"));
    }

    @Test
    void testKeyOverwrite() {
        storage.put("user:1", "Alice");
        assertEquals("Alice", storage.get("user:1"));

        storage.put("user:1", "Bob");           // перезапись
        assertEquals("Bob", storage.get("user:1"));
    }

    @Test
    void testFlushToDisk() throws InterruptedException, IOException {
        // Записываем больше порога, чтобы сработал flush
        for (int i = 0; i < SMALL_THRESHOLD + 3; i++) {
            storage.put("key" + i, "value" + i);
        }

        Thread.sleep(400); // даём время фоновому flushExecutor

        // Проверяем, что данные читаются после flush
        for (int i = 0; i < SMALL_THRESHOLD + 3; i++) {
            assertEquals("value" + i, storage.get("key" + i));
        }

        // Проверяем, что SSTable файл действительно появился
        long dbFileCount = Files.list(tempDir)
                .filter(path -> path.toString().endsWith(".db"))
                .count();

        assertTrue(dbFileCount >= 1, "После flush должен появиться хотя бы один .db файл");
    }

    @Test
    void testWalRecovery() {
        // Записываем данные меньше threshold, чтобы flush не сработал
        storage.put("wal1", "hello");
        storage.put("wal2", "world");
        storage.put("wal1", "hello-updated");   // перезапись

        // Симулируем крах — закрываем хранилище
        storage.close();

        // Создаём новое хранилище на той же папке
        storage = new LsmStorage(tempDir, SMALL_THRESHOLD);

        // Данные должны восстановиться из WAL
        assertEquals("hello-updated", storage.get("wal1"));
        assertEquals("world", storage.get("wal2"));
    }

    // ================== ТЕСТ НА КОМПАКЦИЮ ==================
    @Test
    void testCompactionReducesFileCount() throws InterruptedException, IOException {
        // Записываем достаточно данных, чтобы сработало несколько flush и компакция
        for (int batch = 0; batch < 6; batch++) {           // 6 * 5 = 30 записей
            for (int i = 0; i < SMALL_THRESHOLD; i++) {
                storage.put("batch" + batch + "_key" + i, "value");
            }
            Thread.sleep(100); // даём время flush
        }

        Thread.sleep(800); // даём время на компакцию

        long fileCount = Files.list(tempDir)
                .filter(p -> p.toString().endsWith(".db"))
                .count();

        System.out.println("После компакции файлов: " + fileCount);

        // Без компакции было бы ~6 файлов. С компакцией должно быть меньше.
        assertTrue(fileCount <= 4, "Компакция должна уменьшать количество файлов");
    }
    @Test
    void testEmptyKey() {
        storage.put("", "valueForEmptyKey");
        assertEquals("valueForEmptyKey", storage.get(""));
    }

    @Test
    void testNullKeyShouldThrowException() {
        assertThrows(NullPointerException.class, () -> {
            storage.put(null, "someValue");
        });
    }

    @Test
    void testNullValueShouldThrowException() {
        assertThrows(NullPointerException.class, () -> storage.put("key", null));
    }

    @Test
    void testMassiveKeyOverwrites() {
        String key = "hot_key";

        for (int i = 0; i < 500; i++) {
            storage.put(key, "version_" + i);
        }

        assertEquals("version_499", storage.get(key));
    }
    @Test
    void testLargeValue() throws InterruptedException {
        String bigValue = "A".repeat(2000000);
        storage.put("key" , bigValue);
        Thread.sleep(1000);
        assertEquals(bigValue , storage.get("key"));
    }
    @Test
    void testReadDuringFlush() throws InterruptedException {
        for (int i = 0; i < 500; i++) {
            storage.put("key_" + i, "value_" + i);
        }


        for (int i = 0; i < 500; i++) {
            assertEquals(storage.get("key_" + i) , "value_" +i);
        }
    }
    @Test
    void testSstWriteReadConsistency() throws IOException {
        Map<String, String> data = new TreeMap<>();
        for (int i = 0; i < 100; i++) {
            data.put("key_" + i, "value_" + i);
        }

        Path path = tempDir.resolve("test_sst.db");
        SstWriter.write(path, data);

        SstFile sst = SstFile.open(path);
        for (int i = 0; i < 100; i++) {
            String key = "key_" + i;
            String expected = "value_" + i;
            String actual = sst.get(key);
            assertEquals(expected, actual, "Mismatch for " + key);
        }
        sst.close();
    }
}