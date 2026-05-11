# Архитектура FuseDB

Техническая документация внутреннего устройства FuseDB для контрибутеров и исследователей.

## Содержание

- [Обзор системы](#обзор-системы)
- [Формат сегмента](#формат-сегмента)
- [Система компрессии](#система-компрессии)
- [Реализация skiplist](#реализация-skiplist)
- [Write-Ahead Log](#write-ahead-log)
- [Процесс merge](#процесс-merge)
- [Слой кэша](#слой-кэша)
- [Кодирование значений](#кодирование-значений)
- [Поток данных](#поток-данных)

---

## Обзор системы

FuseDB организует данные в листья (leaves). Каждый лист — независимая единица, содержащая in-memory буфер мутаций и иммутабельный сегмент на диске. Текущая реализация (`internal/oneleafdb`) работает как база данных с одним листом.

### Основные компоненты

**Skiplist буфер** (`internal/skiplist`)  
Lock-free упорядоченный индекс, хранящий последние мутации. Поддерживает операции Put, Delete и Inc с in-place слиянием для счётчиков.

**Сегмент** (`internal/segment`)  
Иммутабельная структура на диске, содержащая сжатые блоки данных, bloom filter, индекс и метаданные. Записывается один раз при merge, никогда не модифицируется.

**Write-Ahead Log** (`internal/leaf/wal.go`)  
Слой долговечности с двойной буферизацией и асинхронным групповым коммитом. Сбрасывает батчи каждые 200µs.

**Компрессия** (`internal/compression`)  
LZ4 словарная компрессия с персистентным реестром. Словари обучаются оффлайн на репрезентативных сэмплах, хранятся как `.zdict` файлы.

**Кэш** (`internal/oneleafdb/cache.go`)  
Простой map-based кэш значений с инвалидацией по эпохам. Вытесняет случайно при превышении лимита.

### Принципы дизайна

- Иммутабельность: сегменты никогда не модифицируются после создания
- Lock-free чтение: skiplist позволяет конкурентный доступ без блокировок
- Локальный merge: каждый лист мерджится независимо
- Zero-copy: пулы буферов минимизируют аллокации
- Словарная компрессия: предобученные LZ4 словари для маленьких блоков

---

## Формат сегмента

Файл: `internal/segment/segment_file.go`, `header.go`, `footer.go`, `index.go`

### Структура

```
[Header][Data Blocks][Bloom Filter][Index][Footer]
```

**Header** (фиксированный размер)  
Версия, тип компрессии, ID словаря.

**Data Blocks** (переменная длина)  
Последовательность сжатых блоков. Каждый блок: `[4-байта размер][LZ4 payload]`. Поле размера хранит оригинальную несжатую длину.

**Bloom Filter** (фиксированный размер)  
Вероятностный фильтр, рассчитанный по количеству ключей. Использует `github.com/bits-and-blooms/bloom`.

**Index** (переменная длина)  
Массив записей, отображающих диапазоны ключей на смещения блоков:
```go
type IndexEntry struct {
    FirstKey []byte
    LastKey  []byte
    Offset   uint64
    Length   uint32
}
```

**Footer** (фиксированный размер)  
Количество блоков и указатели на секции (смещение, длина) для Data, Bloom и Index.

### Структура блока

Каждый блок содержит несколько пар ключ-значение:
```
[keyLen:varint][key][valueLen:varint][value]...
```

Целевой размер блока: 4KB. Блоки сжимаются независимо с LZ4 словарём.

### Алгоритм чтения

1. Прочитать footer для определения секций
2. Проверить bloom filter на наличие ключа
3. Бинарный поиск в индексе для блока, содержащего ключ
4. Прочитать и декомпрессировать блок
5. Линейный поиск в блоке для точного совпадения ключа

Расположение: `segment_file.go:228-258`, `reader.go:80-150`

---

## Система компрессии

Файл: `internal/compression/dict.go`, `dict_file.go`

### Обучение словаря

Словари обучаются оффлайн с использованием `github.com/klauspost/compress/zstd/dict.BuildRawDict`:

```go
samples := [][]byte{...}  // Репрезентативные блоки
dict, err := BuildRawDict(samples, 4096)  // 4KB словарь
```

Обучение извлекает общие паттерны из сэмпл-данных. Типичная степень сжатия: экономия 62.6% места.

Расположение: `dict.go:150-186`

### Формат персистентности

Словари хранятся как `.zdict` файлы:
```
[Magic:4][Version:4][ID:4][Level:4][RawLen:4][CRC32:4][RawBytes]
```

Magic: "ZSTD"  
Version: 1  
ID: уникальный идентификатор  
Level: уровень компрессии  
RawLen: размер словаря в байтах  
CRC32: контрольная сумма  
RawBytes: данные словаря

Расположение: `dict_file.go:52-67`

### Пайплайн компрессии

**Сжатие:**
1. `LZ4_createStream()` - инициализация потока
2. `LZ4_loadDict(dict)` - загрузка словаря
3. `LZ4_compress_fast_continue(src, dst, level)` - сжатие блока
4. Добавление 4-байтового заголовка размера

**Декомпрессия:**
1. Чтение 4-байтового заголовка размера
2. Выделение буфера из пула (макс 5KB)
3. `LZ4_decompress_safe_usingDict(src, dst, dict)` - декомпрессия
4. Возврат пулового буфера (вызывающий должен вызвать `Release()`)

Расположение: `dict.go:228-305`

### Реестр

LRU кэш загруженных словарей:

```go
type Registry struct {
    cache    map[uint32]*Dictionary
    lru      []uint32
    maxCount int
    diskDir  string
}
```

При запросе словаря:
1. Проверка in-memory кэша
2. Если не найдено и `diskDir` установлен, загрузка с диска
3. Вытеснение LRU записи при заполнении кэша

Расположение: `dict.go:379-414`

---

## Реализация skiplist

Файл: `internal/skiplist/skiplist.go`

### Структура

```go
type node struct {
    key  []byte
    op   atomic.Pointer[ops.Op]
    next []atomic.Pointer[node]
}
```

Lock-free дизайн с использованием атомарных указателей. Максимальная высота: 20 уровней.

### Выбор высоты

Детерминированный на основе хэша:

```go
func randomHeight(key []byte) int32 {
    h := int32(1)
    hash := xxhash.Sum64(key) ^ seed
    
    for h < maxHeight && (hash&3) == 0 {
        h++
        hash >>= 2
    }
    return h
}
```

Вероятность: 25% шанс каждого дополнительного уровня. Один и тот же ключ всегда получает одну и ту же высоту.

Расположение: `skiplist.go:175-185`

### Алгоритм вставки

1. Найти splice: определить узлы-предшественники на всех уровнях
2. Создать узел с определённой высотой
3. Опубликовать уровень 0 с CAS
4. Опубликовать верхние уровни с повтором при конфликте
5. Атомарно обновить высоту skiplist при необходимости

```go
func (s *SkipList) Apply(key []byte, op ops.Op) {
    for {
        s.findSplice(key, &prevList, &nextList)
        
        if next := nextList[0]; next != nil && bytes.Equal(next.key, key) {
            s.updateNode(next, op)
            return
        }
        
        newNode := newNode(key, op, nodeHeight)
        if !s.publishBaseLevel(newNode, &prevList, &nextList) {
            continue
        }
        
        s.publishUpperLevels(key, newNode, nodeHeight, &prevList, &nextList)
        return
    }
}
```

Расположение: `skiplist.go:77-102`

### Алгоритм обновления

Когда ключ существует, обновить указатель Op in-place без переподключения:

```go
func (s *SkipList) updateNode(n *node, op ops.Op) {
    for {
        oldPtr := n.op.Load()
        merged := coalesceToNew(*oldPtr, op)
        
        if n.op.CompareAndSwap(oldPtr, &merged) {
            s.dataBytes.Add(int64(len(merged.Data) - len(oldPtr.Data)))
            return
        }
    }
}
```

Расположение: `skiplist.go:222-232`

### Слияние операций

Inc операции сливаются для уменьшения памяти:

```go
func coalesceToNew(old, next ops.Op) ops.Op {
    switch next.Kind {
    case ops.OpPut, ops.OpDelete:
        return next
    case ops.OpInc:
        if old.Kind == ops.OpInc {
            sum := ops.DecodeInc(old) + ops.DecodeInc(next)
            return ops.NewInc(sum)
        }
        return next
    }
}
```

Расположение: `skiplist.go:295-308`

---

## Write-Ahead Log

Файл: `internal/leaf/wal.go`

### Двойная буферизация

Два буфера обеспечивают конкурентные записи и сбросы:

```go
type WAL struct {
    active   *bytes.Buffer
    flushing *bytes.Buffer
    mu       sync.Mutex
    file     *os.File
}
```

Активный буфер накапливает записи под мьютексом. Фоновая горутина меняет буферы местами и сбрасывает на диск.

### Формат записи

```
[kind:1][keyLen:8][payloadLen:8][key][payload]
```

Kind: OpPut/OpDelete/OpInc  
KeyLen: varint  
PayloadLen: varint  
Key: сырые байты  
Payload: данные операции

Расположение: `wal.go:117-123`

### Групповой коммит

Фоновая горутина сбрасывает каждые 200µs:

```go
func (w *WAL) flushLoop() {
    ticker := time.NewTicker(200 * time.Microsecond)
    for {
        select {
        case <-ticker.C:
            w.swapAndFlush()
        case <-w.stopCh:
            w.finalFlush()
            return
        }
    }
}
```

Расположение: `wal.go:136-152`

### Режимы синхронизации

**Async (по умолчанию):** Возвращается сразу после добавления в активный буфер.  
**Sync:** Блокируется до завершения сброса.

Каждый сброс выполняет `Write()` с последующим `Sync()` для долговечности.

Расположение: `wal.go:170-174`

---

## Процесс merge

Файл: `internal/oneleafdb/db.go`, `internal/leaf/leaf.go`

### Триггер

Фоновый воркер проверяет `BufferedBytes >= ThresholdBytes` (по умолчанию 5MB). Просыпается по каналу уведомлений.

Расположение: `db.go:277-292`

### Алгоритм

1. Заморозить буфер: остановить новые записи, создать иммутабельный снимок
2. Создать итератор: слить замороженный skiplist с текущим сегментом
3. Построить новый сегмент: записать слитые данные в новый файл
4. Атомарная замена: заменить старый segment reader на новый
5. Вывести из эксплуатации старый сегмент: добавить в очередь TTL (2s grace period)

```go
func (db *DB) runMergeWorker() {
    for range db.notifyMergeWorker {
        if db.leaf.BufferedBytes() < db.thresholdBytes {
            continue
        }
        
        snapshot := db.leaf.Freeze()
        newSegment, err := db.merger.merge(snapshot, db.leaf.CurrentSegment())
        db.leaf.ReplaceSegment(newSegment)
        db.retireSegment(oldSegment, 2*time.Second)
    }
}
```

Расположение: `db.go:186-211`

### Обработка буфера

Замороженный буфер остаётся читаемым во время merge. После завершения merge замороженный буфер удаляется и создаётся свежий активный буфер.

```go
type Leaf struct {
    active  *Buffer
    frozen  *Buffer
    segment *Reader
}
```

Расположение: `leaf.go:183-200`

---

## Слой кэша

Файл: `internal/oneleafdb/cache.go`

### Структура

```go
type Cache struct {
    entries  map[string]cacheEntry
    maxBytes int64
    curBytes int64
    epoch    atomic.Uint64
}

type cacheEntry struct {
    value []byte
    epoch uint64
}
```

### Формат ключа

Zero-copy поиск с использованием `unsafe.String`:

```go
keyStr := unsafe.String(unsafe.SliceData(key), len(key))
entry, ok := c.entries[keyStr]
```

Расположение: `cache.go:37`

### Вытеснение

Случайная итерация по map при превышении `maxBytes`:

```go
func (c *Cache) evictOne() {
    for k := range c.entries {
        delete(c.entries, k)
        return
    }
}
```

Расположение: `cache.go:63-69`

### Инвалидация по эпохам

Каждая запись инкрементирует глобальную эпоху. Записи кэша хранят эпоху создания. При чтении несовпадение эпохи вызывает cache miss.

```go
func (c *Cache) Set(key, value []byte) {
    currentEpoch := c.epoch.Load()
    c.entries[string(key)] = cacheEntry{
        value: value,
        epoch: currentEpoch,
    }
}

func (c *Cache) Get(key []byte) ([]byte, bool) {
    entry, ok := c.entries[keyStr]
    if !ok || entry.epoch != c.epoch.Load() {
        return nil, false
    }
    return entry.value, true
}
```

Расположение: `cache.go:31-40, 124-127, 146-150`

---

## Кодирование значений

Файл: `internal/value/value.go`

### Размещение тега типа

Тег типа хранится в конце закодированных данных для возможности prefix scanning без декодирования значений.

```go
type Kind byte

const (
    KindBytes Kind = 1
    KindInt64 Kind = 2
)
```

### Форматы

**Bytes:**
```
[сырые байты][0x01]
```

**Int64:**
```
[varint][0x02]
```

### Кодирование

```go
func EncodeBytes(data []byte) []byte {
    data = append(data, byte(KindBytes))
    return data
}

func EncodeInt64(v int64) []byte {
    var buf [binary.MaxVarintLen64 + 1]byte
    n := binary.PutVarint(buf[:], v)
    buf[n] = byte(KindInt64)
    return buf[:n+1]
}
```

Расположение: `value.go:40-64`

### Декодирование

Сначала читается последний байт для определения типа, затем извлекается payload:

```go
func KindOf(data []byte) (Kind, error) {
    if len(data) == 0 {
        return 0, ErrEmptyValue
    }
    return Kind(data[len(data)-1]), nil
}

func DecodeBytes(data []byte) ([]byte, error) {
    kind, err := KindOf(data)
    if err != nil || kind != KindBytes {
        return nil, err
    }
    return data[:len(data)-1], nil
}
```

Расположение: `value.go:25-56`

### Обоснование

Дизайн с тегом в конце позволяет итерации по префиксам ключей пропускать парсинг значений до необходимости, снижая CPU в range scans.

---

## Поток данных

### Путь чтения

```
1. Проверить skiplist буфер
   - Если найдено: вернуть немедленно
   - Если Delete tombstone: вернуть not found
   - Если Inc: разрешить против базового значения из сегмента

2. Проверить кэш
   - Если найдено и эпоха совпадает: вернуть кэшированное значение

3. Прочитать из сегмента
   a. Bloom filter: вероятно содержит ключ?
   b. Index: бинарный поиск блока
   c. Прочитать блок с пуловым буфером
   d. Декомпрессировать с LZ4 словарём
   e. Сканировать блок для точного ключа
   f. Кэшировать результат
```

Код:

```go
func (db *DB) Get(key []byte) ([]byte, bool, error) {
    if op, ok := db.leaf.buffer.Read(key); ok {
        if op.Kind == ops.OpDelete {
            return nil, false, nil
        }
        if op.Kind == ops.OpPut {
            return op.Data, true, nil
        }
    }
    
    if val, ok := db.cache.Get(key); ok {
        return val, true, nil
    }
    
    val, ok, err := db.leaf.segment.Get(key)
    if ok {
        db.cache.Set(key, val)
    }
    return val, ok, err
}
```

Расположение: `db.go:120-150`

### Путь записи

```
1. Добавить в WAL
   - Сериализовать операцию
   - Добавить в активный буфер
   - Если sync режим: ждать сброса

2. Применить к skiplist
   - Если ключ существует: слить операции
   - Если новый ключ: вставить новый узел
   - Обновить счётчик DataBytes

3. Инвалидировать кэш
   - Инкрементировать глобальную эпоху
   - Все кэшированные записи теперь устаревшие

4. Проверить порог merge
   - Если BufferedBytes >= ThresholdBytes: уведомить merge worker
```

Код:

```go
func (db *DB) Put(key, value []byte) error {
    if err := db.wal.Append(ops.OpPut, key, value); err != nil {
        return err
    }
    
    db.leaf.buffer.Apply(key, ops.Op{
        Kind: ops.OpPut,
        Data: value,
    })
    
    db.cache.Invalidate()
    
    if db.leaf.BufferedBytes() >= db.thresholdBytes {
        select {
        case db.notifyMergeWorker <- struct{}{}:
        default:
        }
    }
    
    return nil
}
```

Расположение: `db.go:80-110`

---

## Оптимизации производительности

### Zero-Copy управление буферами

Пуловые буферы с явными вызовами `Release()`:

```go
type PooledBuffer struct {
    Data   []byte
    bufPtr *[]byte
}

func (pb *PooledBuffer) Release() {
    if pb != nil && pb.bufPtr != nil {
        blockBufPool.Put(pb.bufPtr)
        pb.bufPtr = nil
    }
}
```

Паттерн вызывающего:
```go
pb, err := readFullAt(offset, length)
if err != nil {
    return err
}
defer pb.Release()
```

Расположение: `segment_file.go:50-80`

### Lock-Free декомпрессия

Использует `atomic.Bool` вместо мьютекса для флага closed:

```go
type Dictionary struct {
    raw    []byte
    closed atomic.Bool
}

func (d *Dictionary) Decompress(src []byte) (*PooledDecompressBuffer, error) {
    if d.closed.Load() {
        return nil, ErrDictionaryClosed
    }
    // Декомпрессия без удержания блокировки
}
```

Расположение: `dict.go:51-71, 262-305`

### Иммутабельные ключи

Skiplist хранит ключи напрямую без клонирования:

```go
func newNode(key []byte, op ops.Op, height int32) *node {
    return &node{
        key:  key,  // Без bytes.Clone - вызывающий гарантирует иммутабельность
        next: make([]atomic.Pointer[node], height),
    }
}
```

Расположение: `skiplist.go:165-172`

---

## Ссылки

Pugh, William (1990). "Skip lists: a probabilistic alternative to balanced trees"  
LZ4 compression: https://github.com/lz4/lz4  
Bloom filters: https://github.com/bits-and-blooms/bloom  
Pebble: https://github.com/cockroachdb/pebble

---

Последнее обновление: 2026-05-11
