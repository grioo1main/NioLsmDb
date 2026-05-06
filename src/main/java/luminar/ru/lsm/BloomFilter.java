package luminar.ru.lsm;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Objects;

/**
 * Вероятностная структура данных Bloom Filter.
 * Используется для быстрой проверки ОТСУТСТВИЯ ключа в SSTable (избежание лишних чтений с диска).
 * Реализован с использованием механизма Double-Hashing.
 */
public class BloomFilter {

    private final BitSet bits;
    private final int numHashes;
    private final int bitSize;

    /**
     * @param expectedInsertions Ожидаемое количество уникальных ключей.
     * @param fpp Допустимая вероятность ложного срабатывания (False Positive Probability, например 0.01 = 1%).
     */
    public BloomFilter(int expectedInsertions, double fpp) {
        if (expectedInsertions <= 0) throw new IllegalArgumentException("Expected insertions must be > 0");
        if (fpp <= 0 || fpp >= 1) throw new IllegalArgumentException("FPP must be in (0, 1)");

        // Вычисление оптимального размера битового массива: m = -n * ln(p) / (ln(2)^2)
        this.bitSize = optimalNumOfBits(expectedInsertions, fpp);

        // Вычисление оптимального количества хеш-функций: k = (m/n) * ln(2)
        this.numHashes = optimalNumOfHashes(expectedInsertions, bitSize);

        this.bits = new BitSet(bitSize);
    }

    private BloomFilter(BitSet bits, int numHashes, int bitSize) {
        this.bits = bits;
        this.numHashes = numHashes;
        this.bitSize = bitSize;
    }

    private static int optimalNumOfBits(long n, double p) {
        return (int) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalNumOfHashes(long n, long m) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }

    /**
     * Добавляет ключ в фильтр.
     */
    public void add(String key) {
        int hash1 = hash1(key);
        int hash2 = hash2(key);

        for (int i = 0; i < numHashes; i++) {
            // Метод Double Hashing: g(i) = h1 + i * h2
            int combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) combinedHash = ~combinedHash;

            int index = combinedHash % bitSize;
            bits.set(index);
        }
    }

    /**
     * Проверяет возможное наличие ключа.
     * @return false — ключа ТОЧНО нет. true — ключ ВОЗМОЖНО есть.
     */
    public boolean mightContain(String key) {
        int hash1 = hash1(key);
        int hash2 = hash2(key);

        for (int i = 0; i < numHashes; i++) {
            int combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) combinedHash = ~combinedHash;

            int index = combinedHash % bitSize;

            if (!bits.get(index)) {
                return false;
            }
        }
        return true;
    }

    private int hash1(String key) {
        return Objects.hash(key);
    }
    
    private int hash2(String key) {
        // Простой сдвиг для получения второго хеша. В реальной реализации можно использовать более сложный алгоритм, например MurmurHash3.
        return Objects.hash(key) >>> 16; 
    }



    public byte[] toBytes() {
        byte[] bitBytes = bits.toByteArray();
        ByteBuffer buf = ByteBuffer.allocate(8 + bitBytes.length);
        buf.putInt(numHashes);
        buf.putInt(bitSize);
        buf.put(bitBytes);
        return buf.array();
    }

    public static BloomFilter fromBytes(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int numHashes = buf.getInt();
        int bitSize = buf.getInt();
        byte[] bitBytes = new byte[buf.remaining()];
        buf.get(bitBytes);
        return new BloomFilter(BitSet.valueOf(bitBytes), numHashes, bitSize);
    }
}