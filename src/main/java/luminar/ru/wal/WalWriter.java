package luminar.ru.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import static java.nio.file.StandardOpenOption.*;

/**
 * Утилита для последовательной записи операций в Write-Ahead Log (WAL).
 * Обеспечивает отказоустойчивость (Durability) базы данных, гарантируя, 
 * что данные будут сохранены на диске до их фактического переноса в SSTable.
 */
public class WalWriter implements Closeable {

    private final FileChannel channel;

    public WalWriter(Path path) throws IOException {
        this.channel = FileChannel.open(path, CREATE, WRITE, APPEND);
    }

    /**
     * Добавляет новую пару ключ-значение в конец файла лога.
     * Каждая запись защищена контрольной суммой CRC32C для защиты от повреждения данных.
     * Метод вызывает принудительный сброс буферов ОС на физический диск (fsync).
     */
    public void append(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        int lenKey = keyBytes.length;
        int lenVal = valueBytes.length;

        CRC32C crc = new CRC32C();

        ByteBuffer intBuf = ByteBuffer.allocate(8);
        intBuf.putInt(lenKey);
        intBuf.putInt(lenVal);
        intBuf.flip();

        crc.update(intBuf);
        crc.update(keyBytes);
        crc.update(valueBytes);

        int checksum = (int) crc.getValue();

        ByteBuffer buffer = ByteBuffer.allocateDirect(12 + lenKey + lenVal);
        buffer.putInt(checksum);
        buffer.putInt(lenKey);
        buffer.putInt(lenVal);
        buffer.put(keyBytes);
        buffer.put(valueBytes);
        buffer.flip();

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    /**
     * Очищает содержимое лог-файла.
     * Вызывается после успешного сброса (Flush) данных из MemTable в SSTable.
     */
    public void truncate() throws IOException {
        channel.truncate(0);
        channel.position(0);
    }
}