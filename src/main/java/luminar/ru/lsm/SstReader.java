package luminar.ru.lsm;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/**
 * Утилита для последовательного чтения всего SSTable файла (используется при Compaction).
 */
public class SstReader {

    /**
     * Загружает все данные из SSTable в память.
     * Используется TreeMap, чтобы при слиянии нескольких файлов ключи оставались отсортированными.
     */
    public static Map<String, String> read(Path path) throws IOException {
        Map<String, String> data = new TreeMap<>();

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            
            int totalRecords = in.readInt();

            for (int i = 0; i < totalRecords; i++) {
                int keyLen = in.readInt();
                int valLen = in.readInt();

                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);

                byte[] valBytes = new byte[valLen];
                in.readFully(valBytes);

                String key = new String(keyBytes, StandardCharsets.UTF_8);
                String value = new String(valBytes, StandardCharsets.UTF_8);

                data.put(key, value);
            }
        }
        return data;
    }
}