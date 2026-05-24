# NioLsmDb

Встраиваемая key-value база данных на основе **LSM-дерева** (Log-Structured Merge-Tree) с неблокирующим HTTP-интерфейсом на Java NIO.

---

## Архитектура

```
HTTP-запрос
     │
     ▼
NioHttpServer (Java NIO, Event Loop)
     │
     ▼
LsmStorage
  ├── MemTable (ConcurrentSkipListMap) ← горячие данные
  ├── Immutable MemTable              ← сбрасывается асинхронно
  ├── WAL (Write-Ahead Log, CRC32C)  ← гарантия durability
  └── SSTable файлы (.db)
        ├── BloomFilter (per-file)   ← быстрый отказ по чтению
        ├── In-memory Index          ← точечный доступ без поиска
        └── Compactor                ← фоновое слияние файлов
```

**Путь записи:** WAL → MemTable → (при достижении порога) async flush → SSTable

**Путь чтения:** MemTable → Immutable MemTable → SSTables (новые к старым, через Bloom Filter + Index)

**Восстановление после сбоя:** при запуске читается WAL и восстанавливается MemTable.

---

## Формат SSTable-файла

```
[Count: 4 байта]
[Record: keyLen(4) + valLen(4) + key(N) + value(M)] × Count
[Index Block: count(4) + {keyLen(4) + key(N) + offset(8)} × count]
[Bloom Block: size(4) + bloomBytes(N)]
[Footer: magic(4) + indexOffset(8) + bloomOffset(8) + indexSize(4) + bloomSize(4) + reserved(4) = 32 байта]
```

Магическое число: `0x4C534D31` ("LSM1")

---

## Запуск

**Требования:** Java 17+, Gradle 8+

```bash
./gradlew run
```

Сервер стартует на `http://localhost:8080`. Данные хранятся в директории `data/`.

---

## HTTP API

| Метод  | Эндпоинт               | Описание              |
|--------|------------------------|-----------------------|
| GET    | `/get?key={key}`       | Получить значение     |
| POST   | `/set`                 | Записать значение     |
| DELETE | `/delete?key={key}`    | Удалить ключ          |

**POST /set** — тело запроса: `key=mykey&value=myvalue` (application/x-www-form-urlencoded)

### Примеры

```bash
# Запись
curl -X POST http://localhost:8080/set \
  -d "key=user:1&value=Alice"

# Чтение
curl "http://localhost:8080/get?key=user:1"

# Удаление
curl -X DELETE "http://localhost:8080/delete?key=user:1"

# Ключ после удаления
curl "http://localhost:8080/get?key=user:1"
# → 404 Key not found
```

---

## Ключевые параметры

| Константа              | Значение | Где               | Смысл                                 |
|------------------------|----------|-------------------|---------------------------------------|
| `FLUSH_THRESHOLD`      | 1000     | NioHttpServer     | Кол-во записей до async flush         |
| `COMPACTION_THRESHOLD` | 4        | LsmStorage        | Кол-во SSTable до запуска compaction  |
| `COMPACTION_BATCH_SIZE`| 3        | LsmStorage        | Кол-во старых файлов за один раз      |
| `CONNECTION_TIMEOUT_MS`| 30 000   | NioHttpServer     | Таймаут idle-соединения               |
| Bloom FPP              | 1%       | SstWriter         | Вероятность ложного срабатывания      |

---

## Тесты и бенчмарки

```bash
# Запустить unit-тесты
./gradlew test

# Нагрузочный тест (см. LsmLoadTest.java)
./gradlew test --tests LsmLoadTest
```

Результаты бенчмарка (threshold=1000, 50 000 операций):

| Тест                    | Throughput     | p50     | p99     |
|-------------------------|----------------|---------|---------|
| WriteOnly               | ~2 489 ops/s   | 0.37 ms | 0.71 ms |
| ReadOnly (existing)     | ~82 508 ops/s  | 0.01 ms | 0.03 ms |
| ReadOnly (nonexistent)  | ~231 481 ops/s | 0.004 ms| 0.009 ms|
| Mixed (write+read)      | ~115k reads/s  | 0.014 ms| 0.038 ms|

---

## Удаление и томбстоуны

Удаление в LSM реализовано через **маркер-удаления (tombstone)**. Метод `delete(key)` записывает специальное значение `\u0000TOMBSTONE\u0000` в WAL и MemTable. При чтении наличие tombstone означает «ключа нет». При compaction tombstone-записи вычищаются из merged-файлов.

---

## Структура проекта

```
src/
├── main/java/luminar/ru/
│   ├── http/
│   │   └── NioHttpServer.java      # Event loop, HTTP-парсинг, маршрутизация
│   ├── lsm/
│   │   ├── LsmStorage.java         # Основной движок: put/get/delete, flush, compaction
│   │   ├── BloomFilter.java        # Вероятностный фильтр (double-hashing)
│   │   ├── Compactor.java          # Слияние старых SSTable
│   │   ├── SstFile.java            # Открытый SSTable (случайное чтение)
│   │   ├── SstReader.java          # Полное последовательное чтение (для compaction)
│   │   └── SstWriter.java          # Запись SSTable на диск
│   └── wal/
│       ├── WalWriter.java          # Запись в WAL с CRC32C и fsync
│       └── WalReader.java          # Восстановление из WAL
└── test/java/
    ├── LsmStorageTest.java         # Unit-тесты движка
    ├── LsmLoadTest.java            # Нагрузочное тестирование
    └── BlockingHttp.java           # HTTP-клиент для тестов
```

---

## Известные ограничения

- **Нет range-запросов** — только точечный поиск по ключу
- **Однопоточный event loop** — один медленный клиент не заблокирует остальных, но CPU используется слабо
- **Весь index каждого SSTable в RAM** — при большом числе ключей потребление памяти растёт линейно
- **Двойное хеширование в BloomFilter** — `hash2` является сдвигом `hash1`, а не независимой хеш-функцией; при коллизиях фильтр деградирует

---

## Планы / TODO

- [ ] Спарс-индекс (sparse index) вместо full index в памяти
- [ ] MurmurHash3 для BloomFilter
- [ ] Многоуровневый compaction (Leveled Compaction)
- [ ] Бинарный протокол (вместо HTTP)
- [ ] TTL для записей
