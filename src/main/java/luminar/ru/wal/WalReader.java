package luminar.ru.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.CRC32C;

import static java.nio.file.StandardOpenOption.READ;

/**
 * Утилита для чтения и восстановления данных из Write-Ahead Log (WAL).
 * Используется при инициализации хранилища для загрузки данных, 
 * которые были в MemTable, но не успели записаться в SSTable из-за сбоя системы.
 */
public class WalReader implements Closeable {

    private final FileChannel channel;

    public WalReader(Path path) throws IOException {
        this.channel = FileChannel.open(path, READ);
    }

    /**
     * Выполняет парсинг файла WAL и восстанавливает консистентное состояние MemTable.
     * Проверяет целостность каждой записи с помощью контрольной суммы CRC32C.
     * При обнаружении поврежденной записи (например, обрыв питания при записи) процесс чтения безопасно прерывается.
     *
     * @return Восстановленная таблица в формате ConcurrentSkipListMap.
     */
    public ConcurrentSkipListMap<String, String> recover() throws IOException {
        ConcurrentSkipListMap<String, String> map = new ConcurrentSkipListMap<>();
        ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
        ByteBuffer crcHelper = ByteBuffer.allocate(8);

        while (channel.read(buffer) != -1 || buffer.position() > 0) {
            buffer.flip();

            while (true) {
                if (buffer.remaining() < 12) {
                    break;
                }

                buffer.mark();

                int storedCrc = buffer.getInt();
                int keyLen = buffer.getInt();
                int valLen = buffer.getInt();

                if (buffer.remaining() < keyLen + valLen) {
                    buffer.reset();
                    break;
                }

                byte[] keyBytes = new byte[keyLen];
                byte[] valBytes = new byte[valLen];
                buffer.get(keyBytes);
                buffer.get(valBytes);

                CRC32C crc = new CRC32C();
                crcHelper.clear();
                crcHelper.putInt(keyLen);
                crcHelper.putInt(valLen);
                crcHelper.flip();

                crc.update(crcHelper);
                crc.update(keyBytes);
                crc.update(valBytes);

                if ((int) crc.getValue() != storedCrc) {
                    return map;
                }

                map.put(new String(keyBytes, StandardCharsets.UTF_8),
                        new String(valBytes, StandardCharsets.UTF_8));
            }
            buffer.compact();
        }
        return map;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}