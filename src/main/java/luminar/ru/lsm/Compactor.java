package luminar.ru.lsm;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Отвечает за процесс слияния (Compaction) нескольких файлов SSTable в один.
 * Устраняет дубликаты и устаревшие значения.
 */
public class Compactor {

    /**
     * Сливает список старых SSTable в один новый файл.
     * 
     * @param list Список файлов (ОБЯЗАТЕЛЬНО отсортированный от старого к новому).
     * @param storageDir Директория для сохранения нового файла.
     * @return Открытый объект нового сжатого SstFile.
     */
    public SstFile submit(List<SstFile> list, Path storageDir) throws IOException {
        
        Map<String, String> map = new TreeMap<>();

        for (SstFile file : list) {
            map.putAll(SstReader.read(file.getPath()));
        }

        String fileName = String.format("sstable_compact_%d.db", System.currentTimeMillis());
        Path path = storageDir.resolve(fileName); 

        SstWriter.write(path, map);
        return SstFile.open(path);
    }
}