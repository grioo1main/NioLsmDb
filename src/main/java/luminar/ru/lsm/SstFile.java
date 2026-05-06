package luminar.ru.lsm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Представляет открытый файл SSTable для точечного чтения (Random Access Read).
 * Метаданные (Индекс и Bloom Filter) загружаются в память при открытии файла, 
 * а сами значения читаются с диска по смещению (Offset) только при запросе.
 */
public class SstFile implements Closeable {

    private static final Logger log = Logger.getLogger(SstFile.class.getName());
    
    private static final int MAGIC_NUMBER = 0x4C534D31; // "LSM1"
    private static final int FOOTER_SIZE = 32;

    private final Path path;
    private final FileChannel channel;
    private final Map<String, Long> index;
    private final BloomFilter bloom;

    private SstFile(Path path, FileChannel channel, Map<String, Long> index, BloomFilter bloom) {
        this.path = path;
        this.channel = channel;
        this.index = index;
        this.bloom = bloom;
    }

    /**
     * Открывает SSTable, проверяет магическое число и загружает индексы в RAM.
     */
    public static SstFile open(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);

        try {
            long fileSize = channel.size();
            if (fileSize < FOOTER_SIZE) {
                throw new IOException("Файл слишком мал для SSTable: " + path);
            }

            ByteBuffer footerBuf = ByteBuffer.allocate(FOOTER_SIZE);
            channel.read(footerBuf, fileSize - FOOTER_SIZE);
            footerBuf.flip();

            int magic = footerBuf.getInt();
            if (magic != MAGIC_NUMBER) {
                throw new IOException("Неверное магическое число файла. Ожидалось LSM1");
            }

            long indexOffset = footerBuf.getLong();
            long bloomOffset = footerBuf.getLong();
            int indexCount = footerBuf.getInt();
            int bloomSize = footerBuf.getInt();

            
            ByteBuffer bloomBuf = ByteBuffer.allocate(4 + bloomSize);
            channel.read(bloomBuf, bloomOffset);
            bloomBuf.flip();
            bloomBuf.getInt(); // Пропускаем размер массива
            byte[] bloomBytes = new byte[bloomSize];
            bloomBuf.get(bloomBytes);
            BloomFilter bloom = BloomFilter.fromBytes(bloomBytes);

            
            long indexLen = bloomOffset - indexOffset;
            ByteBuffer indexBuf = ByteBuffer.allocate((int) indexLen);
            channel.read(indexBuf, indexOffset);
            indexBuf.flip();

            Map<String, Long> index = new HashMap<>(indexCount);
            int count = indexBuf.getInt();
            for (int i = 0; i < count; i++) {
                int keyLen = indexBuf.getInt();
                byte[] keyBytes = new byte[keyLen];
                indexBuf.get(keyBytes);
                long offset = indexBuf.getLong();
                index.put(new String(keyBytes, StandardCharsets.UTF_8), offset);
            }

            return new SstFile(path, channel, index, bloom);

        } catch (Exception e) {
            channel.close();
            throw e;
        }
    }

    /**
     * Точечный поиск значения по ключу.
     * @return Значение, если ключ найден, иначе null.
     */
    public String get(String key) throws IOException {
        // Оптимизация 1: Быстрый отказ, если Bloom Filter говорит "Точно нет"
        if (!bloom.mightContain(key)) return null;

        // Оптимизация 2: Ищем точное смещение в памяти (вместо бинарного поиска по диску)
        Long offset = index.get(key);
        if (offset == null) return null;

        // Читаем заголовок записи (размеры) по смещению
        ByteBuffer headerBuf = ByteBuffer.allocate(8);
        channel.read(headerBuf, offset);
        headerBuf.flip();

        int keyLen = headerBuf.getInt();
        int valLen = headerBuf.getInt();

        
        byte[] valBytes = new byte[valLen];
        channel.read(ByteBuffer.wrap(valBytes), offset + 8 + keyLen);
        
        return new String(valBytes, StandardCharsets.UTF_8);
    }

    public Path getPath() {
        return path;
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }
}