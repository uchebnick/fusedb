# Пилот: лайки и счётчик купленных билетов

Этот профиль позволяет подключить FuseDB как локальную перестраиваемую
проекцию. Платёж, заказ и доступный остаток билетов должны оставаться в
транзакционной основной БД. События в FuseDB доставляются из transactional
outbox с постоянным `eventID`.

## Гарантия записи

`DB.Apply`, `DB.ApplyOnce` и `DB.ApplyOnceIf` записывают все мутации одним
checksummed WAL-record. Публичные point-операции не видят частично применённый
набор.

- `Apply` атомарно проверяет `KeyAbsent`/`KeyPresent` и применяет мутации;
- `ApplyOnce` сохраняет digest набора под выделенным idempotency key;
- `ApplyOnceIf` запоминает и первое отрицательное решение условий, поэтому
  запоздалый retry не применяется к более новому состоянию;
- повтор с тем же ключом и набором возвращает `applied=false`;
- повтор с другим набором возвращает `fusedb.ErrIdempotencyConflict`.

Idempotency key является обычным ключом FuseDB. Приложение не должно изменять
его через `Put` или `Delete`. Храните его в отдельном namespace `event/...`.

Готовая реализация ключей и операций находится в
[`examples/likes-tickets`](../examples/likes-tickets/store.go).

## Открытие базы

```go
options := fusedb.DurablePilotOptions("/var/lib/service/fusedb")
options.Scheduler.CPUCores = 4
options.Scheduler.MemoryBytes = 4 << 30
options.Scheduler.DiskBytesPerSecond = 250 << 20

db, err := fusedb.Open(options)
if err != nil {
    return err
}
```

Профиль включает synchronous WAL, оставляет 30% CPU/disk/RAM как foreground
headroom, включает latency sampling и временно выключает adaptive dictionary
training. После soak-теста обучение можно включить отдельно.

## Лайк

```go
likeKey := []byte("like/post-42/user-7")
applied, err := db.ApplyOnceIf(
    []byte("event/evt-01932"),
    []fusedb.Condition{fusedb.KeyAbsent(likeKey)},
    []fusedb.Mutation{
        fusedb.PutMutation(likeKey, []byte("2026-08-19T18:00:00Z")),
        fusedb.IncMutation([]byte("like-count/post-42"), 1),
    },
)
```

Для unlike используйте новый `eventID`, `KeyPresent`, `DeleteMutation` и
`IncMutation(..., -1)`.

## Покупка

```go
applied, err := db.ApplyOnce(
    []byte("event/purchase-paid/order-991"),
    []fusedb.Mutation{
        fusedb.PutMutation([]byte("purchase/order-991"), []byte("paid")),
        fusedb.IncMutation([]byte("tickets/event-55/sold"), 3),
    },
)
```

Этот счётчик является проекцией оплаченных заказов. Он не заменяет проверку
остатка и списание inventory в основной SQL-транзакции.

Один synchronous-WAL counter key последовательно упорядочивает все изменения
этого ключа. Для горячих объектов пример распределяет likes/tickets по 64
детерминированным counter shards и суммирует их при чтении. Количество shards
является частью key schema: выберите его до запуска и не меняйте без rebuild.

## Обязательный запуск

1. Создать отдельный локальный SSD volume; NFS и общий каталог не использовать.
2. Зафиксировать commit, Go toolchain и версию native LZ4.
3. Запустить qualification на том же volume с реальными размерами ключей,
   значений, steady/spike QPS и latency SLO.
4. Подключить `/readyz`, Prometheus collector и алерты на terminal errors,
   background failures, p99 и maintenance debt.
5. Проверить kill/reopen и восстановление backup в отдельный каталог.
6. При `ErrWALPersistence` или `ErrCommitUncertain` снять instance с readiness,
   закрыть handle и reopen; retry без того же idempotency key запрещён.
7. Сохранять authoritative outbox достаточно долго, чтобы полностью перестроить
   проекцию в новый каталог.

Без основной БД/outbox этот профиль остаётся no-go для денежных данных.
