package luminar.ru.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;

/**
 * Утилита для записи SSTable на диск.
 * Формат файла:
 * [Количество записей (4)] [Данные (РазмерКлюча, РазмерЗначения, Ключ, Значение)...]
 * [Индекс блока] [Фильтр Блума] [Footer (32 байта)]
 */
public class SstWriter {

    private static final int MAGIC_NUMBER = 0x4C534D31; // "LSM1"
    private static final int FOOTER_SIZE = 32;

    public static void write(Path path, Map<String, String> data) throws IOException {
        Map<String, String> sortedData = data instanceof TreeMap ? data : new TreeMap<>(data);

        BloomFilter bloom = new BloomFilter(Math.max(sortedData.size(), 1), 0.01);
        Map<String, Long> index = new TreeMap<>();

        try (FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer buffer = ByteBuffer.allocateDirect(8192);

            buffer.putInt(sortedData.size());
            flushBuffer(buffer, channel);

            long currentOffset = 4; 

            for (Map.Entry<String, String> entry : sortedData.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);

                int recordSize = 8 + keyBytes.length + valBytes.length; 
                index.put(entry.getKey(), currentOffset);
                bloom.add(entry.getKey());

                if (buffer.remaining() < recordSize) {
                    flushBuffer(buffer, channel);
                    if (buffer.capacity() < recordSize) {
                        buffer = ByteBuffer.allocateDirect(recordSize);
                    }
                }

                buffer.putInt(keyBytes.length);
                buffer.putInt(valBytes.length);
                buffer.put(keyBytes);
                buffer.put(valBytes);

                currentOffset += recordSize;
            }
            flushBuffer(buffer, channel);

            
            long indexOffset = channel.position();
            writeIndexBlock(channel, index);

            long bloomOffset = channel.position();
            writeBloomBlock(channel, bloom);

            
            writeFooter(channel, indexOffset, bloomOffset, index.size(), bloom.toBytes().length);
            channel.force(true); // fsync: гарантируем сброс на жесткий диск
        }
    }

    private static void flushBuffer(ByteBuffer buffer, FileChannel channel) throws IOException {
        buffer.flip();
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
        buffer.clear();
    }

    private static void writeIndexBlock(FileChannel channel, Map<String, Long> index) throws IOException {
        int size = 4;
        for (Map.Entry<String, Long> e : index.entrySet()) {
            size += 4 + e.getKey().getBytes(StandardCharsets.UTF_8).length + 8;
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(index.size());
        for (Map.Entry<String, Long> e : index.entrySet()) {
            byte[] kb = e.getKey().getBytes(StandardCharsets.UTF_8);
            buf.putInt(kb.length);
            buf.put(kb);
            buf.putLong(e.getValue());
        }

        buf.flip();
        while (buf.hasRemaining()) channel.write(buf);
    }

    private static void writeBloomBlock(FileChannel channel, BloomFilter bloom) throws IOException {
        byte[] bloomBytes = bloom.toBytes();
        ByteBuffer buf = ByteBuffer.allocate(4 + bloomBytes.length);
        buf.putInt(bloomBytes.length);
        buf.put(bloomBytes);
        buf.flip();
        while (buf.hasRemaining()) channel.write(buf);
    }

    private static void writeFooter(FileChannel channel,
                                    long indexOffset, long bloomOffset,
                                    int indexSize, int bloomSize) throws IOException {
        ByteBuffer footer = ByteBuffer.allocate(FOOTER_SIZE);
        footer.putInt(MAGIC_NUMBER);
        footer.putLong(indexOffset);
        footer.putLong(bloomOffset);
        footer.putInt(indexSize);
        footer.putInt(bloomSize);
        footer.putInt(0); // Зарезервировано для будущих расширений
        footer.flip();
        while (footer.hasRemaining()) channel.write(footer);
    }
}