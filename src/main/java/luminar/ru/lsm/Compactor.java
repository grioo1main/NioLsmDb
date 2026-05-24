package luminar.ru.lsm;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Отвечает за процесс слияния (Compaction) нескольких файлов SSTable в один.
 * Устраняет дубликаты, устаревшие значения и маркеры удаления (tombstones).
 *
 * Tombstone-записи вычищаются при слиянии самых старых файлов,
 * т.к. на этом уровне нет более старых данных, которые они могли бы скрывать.
 */
public class Compactor {

    /**
     * Сливает список старых SSTable в один новый файл.
     *
     * @param list       Список файлов (ОБЯЗАТЕЛЬНО отсортированный от старого к новому).
     * @param storageDir Директория для сохранения нового файла.
     * @return Открытый объект нового сжатого SstFile, или null если после очистки данных не осталось.
     */
    public SstFile submit(List<SstFile> list, Path storageDir) throws IOException {
        // Слияние: более новые файлы перезаписывают более старые (putAll в порядке от старых к новым)
        Map<String, String> merged = new TreeMap<>();

        for (SstFile file : list) {
            merged.putAll(SstReader.read(file.getPath()));
        }

        // Вычищаем tombstone-записи: поскольку мы сжимаем самые старые файлы,
        // маркеры удаления больше не нужны — нижележащих данных, которые они
        // скрывают, уже не существует.
        merged.entrySet().removeIf(entry -> LsmStorage.isTombstone(entry.getValue()));

        if (merged.isEmpty()) {
            return null;
        }

        String fileName = String.format("sstable_compact_%d.db", System.currentTimeMillis());
        Path path = storageDir.resolve(fileName);

        SstWriter.write(path, merged);
        return SstFile.open(path);
    }
}