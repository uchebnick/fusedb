# FuseDB Detailed Benchmark Dashboard

## 🎯 Что включено

Создан полный детальный Grafana dashboard с автоматической провизией:

### 📊 Метрики производительности
- **Current RPS** - текущий RPS с линейным ростом
- **Operations Rate** - скорость операций (get/put)
- **Latency p50, p95, p99** - перцентили задержки в микросекундах

### 💾 Метрики памяти
- **Allocations per Operation** - количество аллокаций на операцию
- **Bytes Allocated per Operation** - байты выделенные на операцию
- **Memory Usage (Heap)** - использование heap памяти
- **Memory Stats (Alloc vs Sys)** - детальная статистика памяти

### 🗑️ Метрики GC
- **GC Frequency** - частота сборки мусора (GC/sec)
- **GC Duration** - средняя длительность GC

### 💻 Системные метрики
- **CPU Usage** - использование CPU
- **Goroutines** - количество горутин
- **Errors & Misses** - ошибки и промахи кеша

## 🚀 Быстрый старт

```bash
cd benchmarks/docker

# Запустить все сервисы
docker-compose -f docker-compose.metrics.yml up -d

# Открыть Grafana
open http://localhost:3000
# Login: admin / admin

# Посмотреть логи
docker-compose -f docker-compose.metrics.yml logs -f

# Остановить
docker-compose -f docker-compose.metrics.yml down -v
```

## 📈 Dashboards

После запуска автоматически загружаются два dashboard:

1. **FuseDB Benchmark Comparison** - базовое сравнение
2. **FuseDB Detailed Benchmark** - детальный dashboard со всеми метриками

Найти их можно в Grafana:
- Home → Dashboards → Browse
- Или прямая ссылка: http://localhost:3000/d/fusedb-detailed

## 🔧 Конфигурация

### Изменить параметры нагрузки

Отредактируй `docker-compose.metrics.yml`:

```yaml
environment:
  - RECORD_COUNT=100000      # Количество ключей
  - VALUE_SIZE=1024          # Размер значения (байты)
  - TARGET_RPS=1000000       # Целевой RPS (линейный рост)
  - RAMP_UP_MINUTES=5        # Время роста нагрузки
  - TEST_MINUTES=10          # Время теста после роста
  - READ_RATIO=0.8           # 80% чтений, 20% записей
  - CACHE_SIZE_MB=64         # Размер кеша
  - GOMAXPROCS=2             # CPU ядра
```

### Ресурсы контейнера

```yaml
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 512M
```

## 📊 Что показывают метрики

### Latency (p50, p95, p99)
- **p50** - медиана, 50% запросов быстрее этого значения
- **p95** - 95% запросов быстрее этого значения
- **p99** - 99% запросов быстрее этого значения

Цвета:
- 🟢 Зеленый: хорошо
- 🟡 Желтый: внимание
- 🔴 Красный: проблема

### Allocations
- **Allocs/op** - количество аллокаций памяти на операцию
- **Bytes/op** - байты выделенные на операцию

Меньше = лучше. Цель: <5 allocs/op, <500 bytes/op

### GC
- **GC Frequency** - как часто запускается сборщик мусора
- **GC Duration** - сколько времени занимает GC

Частый GC = много аллокаций. Долгий GC = проблемы с производительностью.

### Memory
- **Heap** - память выделенная приложением
- **Alloc** - текущая выделенная память
- **Sys** - память запрошенная у ОС

## 🎯 Как читать результаты

### OneLeaf vs Pebble

**Хорошо:**
- OneLeaf latency ниже чем Pebble
- OneLeaf RPS выше чем Pebble
- Allocations/op примерно равны

**Плохо:**
- OneLeaf bytes/op значительно выше Pebble
- OneLeaf GC чаще чем Pebble
- OneLeaf memory растет быстрее

### Linear Ramp-Up

График **Current RPS** должен показывать линейный рост:
```
0 min:  0 RPS
1 min:  200K RPS
2 min:  400K RPS
3 min:  600K RPS
4 min:  800K RPS
5 min:  1M RPS (steady state)
```

Если RPS перестает расти - нашли bottleneck!

## 🔍 Troubleshooting

### Dashboard не загружается

```bash
# Проверить что Grafana запущена
docker-compose -f docker-compose.metrics.yml ps grafana

# Проверить логи
docker-compose -f docker-compose.metrics.yml logs grafana

# Перезапустить
docker-compose -f docker-compose.metrics.yml restart grafana
```

### Нет данных в графиках

```bash
# Проверить что Prometheus видит targets
open http://localhost:9092/targets

# Должны быть UP:
# - oneleaf-bench:9090
# - pebble-bench:9090

# Проверить метрики напрямую
curl http://localhost:9090/metrics | grep benchmark_current_rps
curl http://localhost:9091/metrics | grep benchmark_current_rps
```

### Высокое использование памяти

Уменьши нагрузку:
```yaml
- RECORD_COUNT=50000       # Было 100000
- TARGET_RPS=500000        # Было 1000000
```

## 📁 Структура файлов

```
benchmarks/docker/
├── docker-compose.metrics.yml          # Compose файл
├── prometheus-bench.yml                # Конфиг Prometheus
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── prometheus.yml         # Datasource (авто)
│   │   └── dashboards/
│   │       └── dashboards.yml         # Провизия dashboards (авто)
│   └── dashboards/
│       ├── fusedb-benchmark.json      # Базовый dashboard
│       └── fusedb-detailed.json       # Детальный dashboard (НОВЫЙ!)
```

## ✅ Что сделано

1. ✅ **Buffer pooling** - добавлен sync.Pool для буферов
   - `internal/segment/segment_file.go` - block buffers (64KB)
   - `internal/compression/dict.go` - decompression buffers (128KB)

2. ✅ **Removed clones** - убраны лишние bytes.Clone()
   - `internal/oneleafdb/db.go` - Get() return
   - `internal/segment/block.go` - Separator()
   - `internal/compression/dict.go` - Raw()

3. ✅ **Optimized cache** - zero-alloc lookups
   - `internal/oneleafdb/cache.go` - unsafe.String()

4. ✅ **Fixed benchmark overhead** - 1% sampling вместо per-op ReadMemStats()
   - `cmd/benchmark-runner-metrics/main.go`
   - `cmd/pebble-runner-metrics/main.go`

5. ✅ **Detailed Grafana dashboard** - автоматическая провизия
   - Latency p50, p95, p99
   - GC frequency & duration
   - CPU usage
   - Memory stats (Alloc, Sys, Heap)
   - Allocations & bytes per op
   - Goroutines
   - Errors & misses

## 🎉 Результаты

### Single-threaded
- **Before:** 5 allocs/op, 3554 B/op
- **After:** 3 allocs/op, 3278 B/op
- **Improvement:** -40% allocs, -7.8% bytes

### High load (90K RPS)
- **OneLeaf:** 637 allocs/op, 309KB/op
- **Pebble:** 642 allocs/op, 17KB/op
- **Gap:** OneLeaf allocates **17.6x more bytes**

## 🎯 Next Steps

1. Profile memory allocations under load
2. Optimize byte allocations (17.6x gap)
3. Investigate decompression buffer usage
4. Consider zero-copy value returns

---

**Все готово! Открой http://localhost:3000 и смотри детальные метрики в реальном времени! 🚀**
