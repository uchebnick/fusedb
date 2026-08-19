# FuseDB comparative benchmark

> Generated from the versioned JSON report. Lower latency is better; higher throughput is better.

Run: `2026-08-19T20:46:49Z` · commit `024b1e50b716`

Command:

```bash
go run -tags rocksdb ./cmd/kvbench -engines all -workloads read-random,read-heavy,balanced,overwrite -durability both -workers 1,8 -keys 20000 -value-bytes 256 -cache-bytes 67108864 -memtable-bytes 16777216 -warmup 300ms -duration 2s -repetitions 3 -seed 1 -json results/apple-m4-2026-08-19.json -markdown RESULTS.md
```

Environment: `darwin/arm64`, `go1.25.13`, `Apple M4`, 10 CPUs, GOMAXPROCS=10.

Dataset: 20000 keys × 256 B values; cache 64.0 MiB; memtable/merge target 16.0 MiB; warmup 300ms; measured 2s; repetitions 3.

## Median summary

| Engine | Durability | Workload | Workers | Median ops/s | Median p99 | Runs |
|---|---|---|---:|---:|---:|---:|
| badger | async | balanced | 1 | 117754 | 25.167µs | 3 |
| fusedb | async | balanced | 1 | 601537 | 4.795µs | 3 |
| pebble | async | balanced | 1 | 444551 | 8.711µs | 3 |
| rocksdb | async | balanced | 1 | 237544 | 10.671µs | 3 |
| badger | async | balanced | 8 | 191774 | 120.895µs | 3 |
| fusedb | async | balanced | 8 | 674176 | 167.551µs | 3 |
| pebble | async | balanced | 8 | 889014 | 73.343µs | 3 |
| rocksdb | async | balanced | 8 | 236685 | 122.559µs | 3 |
| badger | async | overwrite | 1 | 110520 | 24.847µs | 3 |
| fusedb | async | overwrite | 1 | 763706 | 3.417µs | 3 |
| pebble | async | overwrite | 1 | 604324 | 3.251µs | 3 |
| rocksdb | async | overwrite | 1 | 198792 | 10.711µs | 3 |
| badger | async | overwrite | 8 | 167231 | 119.743µs | 3 |
| fusedb | async | overwrite | 8 | 1078182 | 66.943µs | 3 |
| pebble | async | overwrite | 8 | 577023 | 71.039µs | 3 |
| rocksdb | async | overwrite | 8 | 125684 | 136.959µs | 3 |
| badger | async | read-heavy | 1 | 241393 | 13.711µs | 3 |
| fusedb | async | read-heavy | 1 | 440454 | 6.711µs | 3 |
| pebble | async | read-heavy | 1 | 281558 | 10.167µs | 3 |
| rocksdb | async | read-heavy | 1 | 359862 | 7.335µs | 3 |
| badger | async | read-heavy | 8 | 383439 | 158.207µs | 3 |
| fusedb | async | read-heavy | 8 | 549419 | 179.199µs | 3 |
| pebble | async | read-heavy | 8 | 1901056 | 16.927µs | 3 |
| rocksdb | async | read-heavy | 8 | 1816176 | 42.175µs | 3 |
| badger | async | read-random | 1 | 329166 | 13.631µs | 3 |
| fusedb | async | read-random | 1 | 2462905 | 958ns | 3 |
| pebble | async | read-random | 1 | 428371 | 5.667µs | 3 |
| rocksdb | async | read-random | 1 | 522409 | 3.917µs | 3 |
| badger | async | read-random | 8 | 519084 | 214.015µs | 3 |
| fusedb | async | read-random | 8 | 4794668 | 2.751µs | 3 |
| pebble | async | read-random | 8 | 1515893 | 11.799µs | 3 |
| rocksdb | async | read-random | 8 | 2158282 | 11.167µs | 3 |
| badger | sync | balanced | 1 | 34096 | 87.167µs | 3 |
| fusedb | sync | balanced | 1 | 523 | 4.784127ms | 3 |
| pebble | sync | balanced | 1 | 544 | 4.179967ms | 3 |
| rocksdb | sync | balanced | 1 | 531 | 4.116479ms | 3 |
| badger | sync | balanced | 8 | 41219 | 558.591µs | 3 |
| fusedb | sync | balanced | 8 | 4060 | 5.083135ms | 3 |
| pebble | sync | balanced | 8 | 2088 | 8.921087ms | 3 |
| rocksdb | sync | balanced | 8 | 2117 | 8.953855ms | 3 |
| badger | sync | overwrite | 1 | 18785 | 99.327µs | 3 |
| fusedb | sync | overwrite | 1 | 258 | 5.009407ms | 3 |
| pebble | sync | overwrite | 1 | 263 | 4.644863ms | 3 |
| rocksdb | sync | overwrite | 1 | 255 | 4.976639ms | 3 |
| badger | sync | overwrite | 8 | 20683 | 838.655µs | 3 |
| fusedb | sync | overwrite | 8 | 2033 | 5.189631ms | 3 |
| pebble | sync | overwrite | 8 | 1056 | 9.756671ms | 3 |
| rocksdb | sync | overwrite | 8 | 1137 | 9.650175ms | 3 |
| badger | sync | read-heavy | 1 | 169273 | 45.567µs | 3 |
| fusedb | sync | read-heavy | 1 | 5199 | 4.005887ms | 3 |
| pebble | sync | read-heavy | 1 | 5043 | 3.971071ms | 3 |
| rocksdb | sync | read-heavy | 1 | 5251 | 3.977215ms | 3 |
| badger | sync | read-heavy | 8 | 262134 | 182.015µs | 3 |
| fusedb | sync | read-heavy | 8 | 23478 | 8.241151ms | 3 |
| pebble | sync | read-heavy | 8 | 21153 | 7.938047ms | 3 |
| rocksdb | sync | read-heavy | 8 | 21104 | 7.979007ms | 3 |
| badger | sync | read-random | 1 | 360803 | 11.591µs | 3 |
| fusedb | sync | read-random | 1 | 2933887 | 833ns | 3 |
| pebble | sync | read-random | 1 | 468938 | 4.875µs | 3 |
| rocksdb | sync | read-random | 1 | 533266 | 3.501µs | 3 |
| badger | sync | read-random | 8 | 529895 | 217.343µs | 3 |
| fusedb | sync | read-random | 8 | 4772245 | 2.751µs | 3 |
| pebble | sync | read-random | 8 | 1517628 | 12.799µs | 3 |
| rocksdb | sync | read-random | 8 | 2376920 | 10.583µs | 3 |

<details>
<summary>Raw repetitions</summary>

| Engine | Durability | Workload | Workers | Repeat | ops/s | p50 | p95 | p99 | p99.9 | Size | Errors |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| badger | async | balanced | 1 | 1 | 117754 | 7.291µs | 15.463µs | 25.167µs | 47.839µs | 39.6 MiB | 0 |
| badger | async | balanced | 1 | 2 | 114412 | 7.295µs | 16.047µs | 29.087µs | 53.631µs | 38.7 MiB | 0 |
| badger | async | balanced | 1 | 3 | 119135 | 7.127µs | 15.255µs | 23.375µs | 45.183µs | 40.0 MiB | 0 |
| fusedb | async | balanced | 1 | 1 | 577363 | 1.333µs | 2.959µs | 5.127µs | 18.383µs | 5.7 MiB | 0 |
| fusedb | async | balanced | 1 | 2 | 617978 | 1.209µs | 2.667µs | 4.795µs | 16.167µs | 5.7 MiB | 0 |
| fusedb | async | balanced | 1 | 3 | 601537 | 1.291µs | 2.751µs | 4.667µs | 17.007µs | 5.7 MiB | 0 |
| pebble | async | balanced | 1 | 1 | 425290 | 1.458µs | 4.543µs | 8.711µs | 29.167µs | 100.6 MiB | 0 |
| pebble | async | balanced | 1 | 2 | 444551 | 1.5µs | 4.667µs | 8.919µs | 26.591µs | 101.1 MiB | 0 |
| pebble | async | balanced | 1 | 3 | 458262 | 1.417µs | 4.419µs | 8.255µs | 21.087µs | 101.6 MiB | 0 |
| rocksdb | async | balanced | 1 | 1 | 240354 | 3.959µs | 7.251µs | 10.631µs | 35.807µs | 16.6 MiB | 0 |
| rocksdb | async | balanced | 1 | 2 | 233950 | 4.001µs | 7.375µs | 10.959µs | 31.967µs | 15.6 MiB | 0 |
| rocksdb | async | balanced | 1 | 3 | 237544 | 3.959µs | 7.291µs | 10.671µs | 36.351µs | 16.5 MiB | 0 |
| badger | async | balanced | 8 | 1 | 191774 | 37.023µs | 88.895µs | 120.895µs | 184.063µs | 10.8 MiB | 0 |
| badger | async | balanced | 8 | 2 | 184351 | 38.431µs | 91.519µs | 124.223µs | 218.879µs | 10.0 MiB | 0 |
| badger | async | balanced | 8 | 3 | 203605 | 34.527µs | 83.135µs | 115.263µs | 208.383µs | 16.5 MiB | 0 |
| fusedb | async | balanced | 8 | 1 | 599230 | 1.958µs | 83.711µs | 173.823µs | 356.351µs | 5.7 MiB | 0 |
| fusedb | async | balanced | 8 | 2 | 683507 | 1.791µs | 75.071µs | 153.983µs | 302.847µs | 5.7 MiB | 0 |
| fusedb | async | balanced | 8 | 3 | 674176 | 1.792µs | 77.311µs | 167.551µs | 346.367µs | 5.7 MiB | 0 |
| pebble | async | balanced | 8 | 1 | 878829 | 2.583µs | 38.111µs | 74.367µs | 148.095µs | 99.6 MiB | 0 |
| pebble | async | balanced | 8 | 2 | 902904 | 2.501µs | 37.823µs | 72.511µs | 138.239µs | 103.0 MiB | 0 |
| pebble | async | balanced | 8 | 3 | 889014 | 2.501µs | 37.887µs | 73.343µs | 145.151µs | 101.6 MiB | 0 |
| rocksdb | async | balanced | 8 | 1 | 228890 | 18.015µs | 94.591µs | 127.295µs | 220.671µs | 15.1 MiB | 0 |
| rocksdb | async | balanced | 8 | 2 | 245611 | 17.471µs | 86.015µs | 118.335µs | 190.975µs | 18.6 MiB | 0 |
| rocksdb | async | balanced | 8 | 3 | 236685 | 17.631µs | 89.983µs | 122.559µs | 282.111µs | 16.0 MiB | 0 |
| badger | async | overwrite | 1 | 1 | 108783 | 7.919µs | 16.543µs | 26.719µs | 51.071µs | 19.6 MiB | 0 |
| badger | async | overwrite | 1 | 2 | 110520 | 7.751µs | 16.295µs | 24.847µs | 45.311µs | 20.3 MiB | 0 |
| badger | async | overwrite | 1 | 3 | 112829 | 7.711µs | 16.007µs | 22.799µs | 45.727µs | 21.7 MiB | 0 |
| fusedb | async | overwrite | 1 | 1 | 751425 | 958ns | 1.875µs | 3.751µs | 13.799µs | 5.7 MiB | 0 |
| fusedb | async | overwrite | 1 | 2 | 763706 | 958ns | 1.709µs | 3.417µs | 12.375µs | 5.7 MiB | 0 |
| fusedb | async | overwrite | 1 | 3 | 783458 | 917ns | 1.667µs | 3.375µs | 12.127µs | 5.7 MiB | 0 |
| pebble | async | overwrite | 1 | 1 | 600328 | 1.25µs | 2.209µs | 3.293µs | 13.423µs | 102.1 MiB | 0 |
| pebble | async | overwrite | 1 | 2 | 604324 | 1.209µs | 2.209µs | 3.251µs | 13.215µs | 97.4 MiB | 0 |
| pebble | async | overwrite | 1 | 3 | 610521 | 1.208µs | 2.125µs | 3.125µs | 13.127µs | 97.4 MiB | 0 |
| rocksdb | async | overwrite | 1 | 1 | 189480 | 4.251µs | 7.835µs | 11.879µs | 48.799µs | 11.5 MiB | 0 |
| rocksdb | async | overwrite | 1 | 2 | 198792 | 4.211µs | 7.419µs | 10.711µs | 35.007µs | 13.9 MiB | 0 |
| rocksdb | async | overwrite | 1 | 3 | 202542 | 4.127µs | 7.375µs | 10.631µs | 32.095µs | 14.5 MiB | 0 |
| badger | async | overwrite | 8 | 1 | 167231 | 44.191µs | 86.847µs | 119.743µs | 178.559µs | 55.9 MiB | 0 |
| badger | async | overwrite | 8 | 2 | 182085 | 41.215µs | 75.839µs | 102.847µs | 154.879µs | 64.5 MiB | 0 |
| badger | async | overwrite | 8 | 3 | 151979 | 43.295µs | 87.999µs | 138.367µs | 1.679359ms | 47.8 MiB | 0 |
| fusedb | async | overwrite | 8 | 1 | 1078182 | 2.791µs | 25.167µs | 66.943µs | 161.023µs | 5.7 MiB | 0 |
| fusedb | async | overwrite | 8 | 2 | 1115970 | 2.875µs | 21.423µs | 56.095µs | 148.991µs | 5.7 MiB | 0 |
| fusedb | async | overwrite | 8 | 3 | 911987 | 2.751µs | 21.343µs | 68.479µs | 288.255µs | 5.7 MiB | 0 |
| pebble | async | overwrite | 8 | 1 | 564088 | 3.501µs | 45.471µs | 72.639µs | 123.647µs | 99.3 MiB | 0 |
| pebble | async | overwrite | 8 | 2 | 577023 | 3.417µs | 44.511µs | 71.039µs | 118.271µs | 99.6 MiB | 0 |
| pebble | async | overwrite | 8 | 3 | 588767 | 3.251µs | 44.319µs | 70.911µs | 120.767µs | 100.1 MiB | 0 |
| rocksdb | async | overwrite | 8 | 1 | 131679 | 56.799µs | 98.879µs | 130.687µs | 300.031µs | 19.5 MiB | 0 |
| rocksdb | async | overwrite | 8 | 2 | 116306 | 57.503µs | 107.839µs | 167.167µs | 2.840575ms | 17.2 MiB | 0 |
| rocksdb | async | overwrite | 8 | 3 | 125684 | 58.815µs | 104.831µs | 136.959µs | 532.991µs | 18.9 MiB | 0 |
| badger | async | read-heavy | 1 | 1 | 241393 | 3.417µs | 7.251µs | 14.711µs | 33.439µs | 13.3 MiB | 0 |
| badger | async | read-heavy | 1 | 2 | 241314 | 3.417µs | 8.127µs | 13.711µs | 31.087µs | 13.2 MiB | 0 |
| badger | async | read-heavy | 1 | 3 | 247497 | 3.375µs | 7.295µs | 13.335µs | 30.719µs | 13.4 MiB | 0 |
| fusedb | async | read-heavy | 1 | 1 | 445261 | 1.5µs | 4.335µs | 6.627µs | 22.383µs | 5.7 MiB | 0 |
| fusedb | async | read-heavy | 1 | 2 | 440454 | 1.542µs | 4.375µs | 6.711µs | 26.591µs | 5.7 MiB | 0 |
| fusedb | async | read-heavy | 1 | 3 | 325305 | 1.958µs | 5.127µs | 9.839µs | 57.087µs | 5.7 MiB | 0 |
| pebble | async | read-heavy | 1 | 1 | 278252 | 3.125µs | 7.251µs | 10.799µs | 34.815µs | 12.6 MiB | 0 |
| pebble | async | read-heavy | 1 | 2 | 281558 | 3.125µs | 7.083µs | 10.167µs | 28.463µs | 12.6 MiB | 0 |
| pebble | async | read-heavy | 1 | 3 | 288083 | 3.043µs | 7.003µs | 10.007µs | 29.967µs | 12.6 MiB | 0 |
| rocksdb | async | read-heavy | 1 | 1 | 362054 | 2.375µs | 4.835µs | 7.335µs | 26.095µs | 9.6 MiB | 0 |
| rocksdb | async | read-heavy | 1 | 2 | 359862 | 2.375µs | 4.875µs | 7.543µs | 24.927µs | 9.6 MiB | 0 |
| rocksdb | async | read-heavy | 1 | 3 | 358255 | 2.417µs | 4.835µs | 7.211µs | 25.919µs | 9.6 MiB | 0 |
| badger | async | read-heavy | 8 | 1 | 339777 | 6.919µs | 99.263µs | 175.999µs | 304.383µs | 15.9 MiB | 0 |
| badger | async | read-heavy | 8 | 2 | 383439 | 5.795µs | 86.591µs | 157.823µs | 294.143µs | 17.2 MiB | 0 |
| badger | async | read-heavy | 8 | 3 | 389270 | 5.127µs | 88.127µs | 158.207µs | 273.151µs | 17.5 MiB | 0 |
| fusedb | async | read-heavy | 8 | 1 | 474106 | 2.417µs | 102.911µs | 209.151µs | 441.343µs | 5.7 MiB | 0 |
| fusedb | async | read-heavy | 8 | 2 | 549419 | 1.917µs | 91.007µs | 179.199µs | 372.223µs | 5.7 MiB | 0 |
| fusedb | async | read-heavy | 8 | 3 | 618691 | 1.667µs | 84.095µs | 158.847µs | 325.887µs | 5.7 MiB | 0 |
| pebble | async | read-heavy | 8 | 1 | 1475474 | 2.709µs | 10.799µs | 22.127µs | 235.007µs | 47.6 MiB | 0 |
| pebble | async | read-heavy | 8 | 2 | 1995690 | 2.167µs | 8.043µs | 15.671µs | 151.295µs | 61.7 MiB | 0 |
| pebble | async | read-heavy | 8 | 3 | 1901056 | 2.293µs | 8.295µs | 16.927µs | 160.767µs | 58.0 MiB | 0 |
| rocksdb | async | read-heavy | 8 | 1 | 1166228 | 3.043µs | 14.711µs | 95.551µs | 245.887µs | 18.4 MiB | 0 |
| rocksdb | async | read-heavy | 8 | 2 | 1816176 | 2.459µs | 10.543µs | 42.175µs | 126.015µs | 10.5 MiB | 0 |
| rocksdb | async | read-heavy | 8 | 3 | 2003309 | 2.459µs | 9.127µs | 25.087µs | 113.471µs | 13.2 MiB | 0 |
| badger | async | read-random | 1 | 1 | 297756 | 2.501µs | 5.919µs | 18.335µs | 41.727µs | 6.4 MiB | 0 |
| badger | async | read-random | 1 | 2 | 329166 | 2.375µs | 5.291µs | 13.631µs | 31.295µs | 6.4 MiB | 0 |
| badger | async | read-random | 1 | 3 | 362649 | 2.291µs | 3.959µs | 11.751µs | 25.087µs | 6.4 MiB | 0 |
| fusedb | async | read-random | 1 | 1 | 2462905 | 292ns | 625ns | 958ns | 4.959µs | 5.7 MiB | 0 |
| fusedb | async | read-random | 1 | 2 | 2067015 | 333ns | 750ns | 1.084µs | 8.423µs | 5.7 MiB | 0 |
| fusedb | async | read-random | 1 | 3 | 2988934 | 250ns | 500ns | 750ns | 3.085µs | 5.7 MiB | 0 |
| pebble | async | read-random | 1 | 1 | 404367 | 2.125µs | 3.417µs | 6.335µs | 29.583µs | 5.2 MiB | 0 |
| pebble | async | read-random | 1 | 2 | 463517 | 1.875µs | 2.917µs | 5.043µs | 18.543µs | 5.2 MiB | 0 |
| pebble | async | read-random | 1 | 3 | 428371 | 1.958µs | 3.167µs | 5.667µs | 28.591µs | 5.2 MiB | 0 |
| rocksdb | async | read-random | 1 | 1 | 527517 | 1.667µs | 2.501µs | 3.917µs | 19.375µs | 5.3 MiB | 0 |
| rocksdb | async | read-random | 1 | 2 | 454701 | 1.875µs | 3.041µs | 5.043µs | 24.959µs | 5.3 MiB | 0 |
| rocksdb | async | read-random | 1 | 3 | 522409 | 1.667µs | 2.459µs | 3.793µs | 17.503µs | 5.3 MiB | 0 |
| badger | async | read-random | 8 | 1 | 519084 | 3.417µs | 74.303µs | 226.559µs | 468.479µs | 6.4 MiB | 0 |
| badger | async | read-random | 8 | 2 | 492189 | 4.459µs | 76.927µs | 214.015µs | 453.375µs | 6.4 MiB | 0 |
| badger | async | read-random | 8 | 3 | 541169 | 3.625µs | 70.335µs | 206.591µs | 431.359µs | 6.4 MiB | 0 |
| fusedb | async | read-random | 8 | 1 | 4794668 | 1.375µs | 2.083µs | 2.625µs | 36.863µs | 5.7 MiB | 0 |
| fusedb | async | read-random | 8 | 2 | 4796406 | 1.334µs | 2.125µs | 2.751µs | 39.519µs | 5.7 MiB | 0 |
| fusedb | async | read-random | 8 | 3 | 4741539 | 1.416µs | 2.209µs | 2.791µs | 32.047µs | 5.7 MiB | 0 |
| pebble | async | read-random | 8 | 1 | 1515893 | 4.419µs | 6.919µs | 11.799µs | 167.807µs | 5.2 MiB | 0 |
| pebble | async | read-random | 8 | 2 | 1471621 | 4.459µs | 7.419µs | 12.423µs | 176.383µs | 5.2 MiB | 0 |
| pebble | async | read-random | 8 | 3 | 1609308 | 4.211µs | 6.291µs | 10.295µs | 156.799µs | 5.2 MiB | 0 |
| rocksdb | async | read-random | 8 | 1 | 2059775 | 2.875µs | 4.211µs | 12.335µs | 170.239µs | 5.3 MiB | 0 |
| rocksdb | async | read-random | 8 | 2 | 2158282 | 2.959µs | 4.251µs | 11.167µs | 144.511µs | 5.3 MiB | 0 |
| rocksdb | async | read-random | 8 | 3 | 2373179 | 2.625µs | 4.127µs | 10.671µs | 130.751µs | 5.3 MiB | 0 |
| badger | sync | balanced | 1 | 1 | 34935 | 33.855µs | 61.471µs | 87.167µs | 168.447µs | 16.4 MiB | 0 |
| badger | sync | balanced | 1 | 2 | 33130 | 33.887µs | 65.919µs | 95.167µs | 238.335µs | 15.8 MiB | 0 |
| badger | sync | balanced | 1 | 3 | 34096 | 31.759µs | 63.167µs | 86.271µs | 179.071µs | 16.1 MiB | 0 |
| fusedb | sync | balanced | 1 | 1 | 553 | 33.311µs | 4.108287ms | 4.972543ms | 6.250495ms | 5.7 MiB | 0 |
| fusedb | sync | balanced | 1 | 2 | 523 | 45.983µs | 4.063231ms | 4.784127ms | 6.615039ms | 5.7 MiB | 0 |
| fusedb | sync | balanced | 1 | 3 | 519 | 57.279µs | 4.061183ms | 4.739071ms | 6.078463ms | 5.7 MiB | 0 |
| pebble | sync | balanced | 1 | 1 | 544 | 65.503µs | 4.052991ms | 4.179967ms | 5.623807ms | 5.4 MiB | 0 |
| pebble | sync | balanced | 1 | 2 | 528 | 97.791µs | 4.036607ms | 4.104191ms | 4.968447ms | 5.4 MiB | 0 |
| pebble | sync | balanced | 1 | 3 | 546 | 106.879µs | 4.032511ms | 4.571135ms | 6.217727ms | 5.4 MiB | 0 |
| rocksdb | sync | balanced | 1 | 1 | 543 | 73.919µs | 4.044799ms | 4.116479ms | 5.054463ms | 5.4 MiB | 0 |
| rocksdb | sync | balanced | 1 | 2 | 528 | 100.543µs | 4.048895ms | 4.120575ms | 6.012927ms | 5.4 MiB | 0 |
| rocksdb | sync | balanced | 1 | 3 | 531 | 109.887µs | 4.032511ms | 4.102143ms | 5.005311ms | 5.4 MiB | 0 |
| badger | sync | balanced | 8 | 1 | 41219 | 183.039µs | 424.447µs | 558.591µs | 777.727µs | 18.2 MiB | 0 |
| badger | sync | balanced | 8 | 2 | 41016 | 183.807µs | 428.543µs | 559.615µs | 763.391µs | 18.2 MiB | 0 |
| badger | sync | balanced | 8 | 3 | 41692 | 182.527µs | 419.327µs | 540.671µs | 688.639µs | 18.4 MiB | 0 |
| fusedb | sync | balanced | 8 | 1 | 4060 | 71.743µs | 4.112383ms | 5.050367ms | 10.526719ms | 5.7 MiB | 0 |
| fusedb | sync | balanced | 8 | 2 | 4077 | 80.511µs | 4.159487ms | 5.984255ms | 8.314879ms | 5.7 MiB | 0 |
| fusedb | sync | balanced | 8 | 3 | 4030 | 99.071µs | 4.513791ms | 5.083135ms | 10.698751ms | 5.7 MiB | 0 |
| pebble | sync | balanced | 8 | 1 | 2062 | 5.865471ms | 8.065023ms | 9.027583ms | 14.868479ms | 5.9 MiB | 0 |
| pebble | sync | balanced | 8 | 2 | 2093 | 5.902335ms | 8.032255ms | 8.142847ms | 12.001279ms | 5.9 MiB | 0 |
| pebble | sync | balanced | 8 | 3 | 2088 | 195.583µs | 8.060927ms | 8.921087ms | 15.007743ms | 5.9 MiB | 0 |
| rocksdb | sync | balanced | 8 | 1 | 2055 | 3.899391ms | 8.069119ms | 8.953855ms | 12.943359ms | 5.8 MiB | 0 |
| rocksdb | sync | balanced | 8 | 2 | 2182 | 2.875391ms | 8.118271ms | 8.970239ms | 10.305535ms | 5.8 MiB | 0 |
| rocksdb | sync | balanced | 8 | 3 | 2117 | 93.311µs | 8.085503ms | 8.552447ms | 14.573567ms | 5.8 MiB | 0 |
| badger | sync | overwrite | 1 | 1 | 18785 | 51.231µs | 74.431µs | 99.327µs | 181.631µs | 17.1 MiB | 0 |
| badger | sync | overwrite | 1 | 2 | 18229 | 50.687µs | 81.599µs | 137.471µs | 253.567µs | 16.8 MiB | 0 |
| badger | sync | overwrite | 1 | 3 | 18971 | 50.239µs | 73.023µs | 93.695µs | 195.839µs | 17.2 MiB | 0 |
| fusedb | sync | overwrite | 1 | 1 | 272 | 3.946495ms | 4.143103ms | 4.870143ms | 6.397951ms | 5.7 MiB | 0 |
| fusedb | sync | overwrite | 1 | 2 | 252 | 3.997695ms | 4.167679ms | 5.009407ms | 5.832703ms | 5.7 MiB | 0 |
| fusedb | sync | overwrite | 1 | 3 | 258 | 3.983359ms | 4.128767ms | 5.017599ms | 6.074367ms | 5.7 MiB | 0 |
| pebble | sync | overwrite | 1 | 1 | 263 | 3.975167ms | 4.120575ms | 4.644863ms | 5.914623ms | 5.4 MiB | 0 |
| pebble | sync | overwrite | 1 | 2 | 252 | 3.997695ms | 4.939775ms | 5.513215ms | 8.044543ms | 5.4 MiB | 0 |
| pebble | sync | overwrite | 1 | 3 | 266 | 3.966975ms | 4.118527ms | 4.526079ms | 5.042175ms | 5.4 MiB | 0 |
| rocksdb | sync | overwrite | 1 | 1 | 253 | 3.989503ms | 4.550655ms | 5.283839ms | 9.994239ms | 5.4 MiB | 0 |
| rocksdb | sync | overwrite | 1 | 2 | 255 | 3.991551ms | 4.083711ms | 4.192255ms | 5.083135ms | 5.4 MiB | 0 |
| rocksdb | sync | overwrite | 1 | 3 | 298 | 3.053567ms | 4.034559ms | 4.976639ms | 5.918719ms | 5.4 MiB | 0 |
| badger | sync | overwrite | 8 | 1 | 20753 | 371.967µs | 564.735µs | 838.655µs | 1.284095ms | 18.3 MiB | 0 |
| badger | sync | overwrite | 8 | 2 | 20683 | 377.343µs | 538.623µs | 740.863µs | 1.302527ms | 18.3 MiB | 0 |
| badger | sync | overwrite | 8 | 3 | 19397 | 380.415µs | 676.351µs | 1.123327ms | 3.870719ms | 17.6 MiB | 0 |
| fusedb | sync | overwrite | 8 | 1 | 1982 | 3.999743ms | 4.943871ms | 5.189631ms | 10.625023ms | 5.7 MiB | 0 |
| fusedb | sync | overwrite | 8 | 2 | 2033 | 3.985407ms | 4.665343ms | 6.090751ms | 8.015871ms | 5.7 MiB | 0 |
| fusedb | sync | overwrite | 8 | 3 | 2246 | 3.147775ms | 4.943871ms | 5.119999ms | 7.946239ms | 5.7 MiB | 0 |
| pebble | sync | overwrite | 8 | 1 | 1056 | 7.909375ms | 8.134655ms | 10.043391ms | 14.974975ms | 5.9 MiB | 0 |
| pebble | sync | overwrite | 8 | 2 | 1041 | 7.962623ms | 8.101887ms | 9.019391ms | 15.024127ms | 5.9 MiB | 0 |
| pebble | sync | overwrite | 8 | 3 | 1065 | 7.827455ms | 8.626175ms | 9.756671ms | 14.950399ms | 5.9 MiB | 0 |
| rocksdb | sync | overwrite | 8 | 1 | 1141 | 7.118847ms | 8.187903ms | 9.961471ms | 14.680063ms | 5.9 MiB | 0 |
| rocksdb | sync | overwrite | 8 | 2 | 1076 | 7.970815ms | 8.101887ms | 8.228863ms | 13.156351ms | 5.8 MiB | 0 |
| rocksdb | sync | overwrite | 8 | 3 | 1137 | 7.061503ms | 8.146943ms | 9.650175ms | 11.501567ms | 5.8 MiB | 0 |
| badger | sync | read-heavy | 1 | 1 | 143080 | 3.625µs | 34.879µs | 49.567µs | 146.559µs | 10.5 MiB | 0 |
| badger | sync | read-heavy | 1 | 2 | 169273 | 3.333µs | 32.047µs | 45.439µs | 73.471µs | 11.2 MiB | 0 |
| badger | sync | read-heavy | 1 | 3 | 170738 | 3.293µs | 31.887µs | 45.567µs | 77.055µs | 11.2 MiB | 0 |
| fusedb | sync | read-heavy | 1 | 1 | 5199 | 3.917µs | 94.143µs | 3.997695ms | 4.698111ms | 5.7 MiB | 0 |
| fusedb | sync | read-heavy | 1 | 2 | 4866 | 4.919µs | 2.889727ms | 4.030463ms | 4.915199ms | 5.7 MiB | 0 |
| fusedb | sync | read-heavy | 1 | 3 | 5444 | 5.167µs | 55.007µs | 4.005887ms | 4.841471ms | 5.7 MiB | 0 |
| pebble | sync | read-heavy | 1 | 1 | 5043 | 4.459µs | 76.095µs | 4.435967ms | 5.013503ms | 5.4 MiB | 0 |
| pebble | sync | read-heavy | 1 | 2 | 4986 | 5.251µs | 2.770943ms | 3.971071ms | 4.415487ms | 5.4 MiB | 0 |
| pebble | sync | read-heavy | 1 | 3 | 5420 | 4.751µs | 50.015µs | 3.971071ms | 4.214783ms | 5.4 MiB | 0 |
| rocksdb | sync | read-heavy | 1 | 1 | 5251 | 4.251µs | 72.319µs | 3.979263ms | 4.235263ms | 5.4 MiB | 0 |
| rocksdb | sync | read-heavy | 1 | 2 | 5007 | 4.587µs | 2.807807ms | 3.977215ms | 4.333567ms | 5.4 MiB | 0 |
| rocksdb | sync | read-heavy | 1 | 3 | 5353 | 4.959µs | 70.271µs | 3.973119ms | 4.509695ms | 5.4 MiB | 0 |
| badger | sync | read-heavy | 8 | 1 | 258415 | 5.167µs | 115.135µs | 186.495µs | 308.223µs | 13.7 MiB | 0 |
| badger | sync | read-heavy | 8 | 2 | 264364 | 5.127µs | 110.143µs | 178.431µs | 286.207µs | 13.9 MiB | 0 |
| badger | sync | read-heavy | 8 | 3 | 262134 | 5.083µs | 112.063µs | 182.015µs | 293.887µs | 13.9 MiB | 0 |
| fusedb | sync | read-heavy | 8 | 1 | 22822 | 6.251µs | 355.071µs | 8.282111ms | 12.386303ms | 5.7 MiB | 0 |
| fusedb | sync | read-heavy | 8 | 2 | 23478 | 6.167µs | 281.343µs | 8.187903ms | 12.017663ms | 5.7 MiB | 0 |
| fusedb | sync | read-heavy | 8 | 3 | 24963 | 6.711µs | 1.115135ms | 8.241151ms | 11.804671ms | 5.7 MiB | 0 |
| pebble | sync | read-heavy | 8 | 1 | 21153 | 5.127µs | 86.271µs | 7.954431ms | 8.970239ms | 5.9 MiB | 0 |
| pebble | sync | read-heavy | 8 | 2 | 20988 | 5.751µs | 3.870719ms | 7.938047ms | 9.527295ms | 5.9 MiB | 0 |
| pebble | sync | read-heavy | 8 | 3 | 21352 | 5.835µs | 3.616767ms | 7.905279ms | 8.863743ms | 5.9 MiB | 0 |
| rocksdb | sync | read-heavy | 8 | 1 | 21104 | 4.211µs | 68.351µs | 7.979007ms | 8.118271ms | 5.8 MiB | 0 |
| rocksdb | sync | read-heavy | 8 | 2 | 20411 | 4.167µs | 98.879µs | 8.011775ms | 9.945087ms | 5.8 MiB | 0 |
| rocksdb | sync | read-heavy | 8 | 3 | 21764 | 4.375µs | 141.823µs | 7.909375ms | 8.863743ms | 5.8 MiB | 0 |
| badger | sync | read-random | 1 | 1 | 358780 | 2.293µs | 4.043µs | 11.591µs | 24.511µs | 6.4 MiB | 0 |
| badger | sync | read-random | 1 | 2 | 360803 | 2.293µs | 3.917µs | 11.711µs | 23.839µs | 6.4 MiB | 0 |
| badger | sync | read-random | 1 | 3 | 374609 | 2.251µs | 3.583µs | 11.215µs | 23.087µs | 6.4 MiB | 0 |
| fusedb | sync | read-random | 1 | 1 | 2685635 | 250ns | 584ns | 1µs | 5.167µs | 5.7 MiB | 0 |
| fusedb | sync | read-random | 1 | 2 | 2970758 | 250ns | 541ns | 792ns | 3.501µs | 5.7 MiB | 0 |
| fusedb | sync | read-random | 1 | 3 | 2933887 | 250ns | 541ns | 833ns | 3.875µs | 5.7 MiB | 0 |
| pebble | sync | read-random | 1 | 1 | 461562 | 1.875µs | 2.917µs | 4.875µs | 19.263µs | 5.2 MiB | 0 |
| pebble | sync | read-random | 1 | 2 | 468938 | 1.834µs | 2.833µs | 4.835µs | 17.967µs | 5.2 MiB | 0 |
| pebble | sync | read-random | 1 | 3 | 469801 | 1.834µs | 2.793µs | 5.003µs | 18.095µs | 5.2 MiB | 0 |
| rocksdb | sync | read-random | 1 | 1 | 491925 | 1.75µs | 2.667µs | 3.959µs | 25.919µs | 5.3 MiB | 0 |
| rocksdb | sync | read-random | 1 | 2 | 534500 | 1.666µs | 2.375µs | 3.501µs | 15.631µs | 5.3 MiB | 0 |
| rocksdb | sync | read-random | 1 | 3 | 533266 | 1.667µs | 2.375µs | 3.459µs | 14.839µs | 5.3 MiB | 0 |
| badger | sync | read-random | 8 | 1 | 520767 | 3.793µs | 72.575µs | 217.727µs | 462.335µs | 6.4 MiB | 0 |
| badger | sync | read-random | 8 | 2 | 531289 | 3.541µs | 72.127µs | 216.703µs | 439.039µs | 6.4 MiB | 0 |
| badger | sync | read-random | 8 | 3 | 529895 | 3.501µs | 72.255µs | 217.343µs | 451.583µs | 6.4 MiB | 0 |
| fusedb | sync | read-random | 8 | 1 | 4600975 | 1.375µs | 2.209µs | 2.917µs | 47.423µs | 5.7 MiB | 0 |
| fusedb | sync | read-random | 8 | 2 | 4806435 | 1.375µs | 2.209µs | 2.751µs | 30.799µs | 5.7 MiB | 0 |
| fusedb | sync | read-random | 8 | 3 | 4772245 | 1.375µs | 2.209µs | 2.751µs | 33.343µs | 5.7 MiB | 0 |
| pebble | sync | read-random | 8 | 1 | 1464791 | 4.419µs | 7.627µs | 14.255µs | 183.167µs | 5.2 MiB | 0 |
| pebble | sync | read-random | 8 | 2 | 1517628 | 4.459µs | 6.835µs | 11.879µs | 166.911µs | 5.2 MiB | 0 |
| pebble | sync | read-random | 8 | 3 | 1532789 | 4.251µs | 6.959µs | 12.799µs | 176.383µs | 5.2 MiB | 0 |
| rocksdb | sync | read-random | 8 | 1 | 2184685 | 2.917µs | 4.295µs | 11.463µs | 141.183µs | 5.3 MiB | 0 |
| rocksdb | sync | read-random | 8 | 2 | 2417153 | 2.583µs | 4.085µs | 10.583µs | 127.551µs | 5.3 MiB | 0 |
| rocksdb | sync | read-random | 8 | 3 | 2376920 | 2.667µs | 4.167µs | 10.375µs | 129.023µs | 5.3 MiB | 0 |

</details>

## Interpretation rules

- `async` means acknowledged writes may still be outside stable storage; `sync` requests stable WAL durability for every write.
- A row compares point-operation behavior only. FuseDB does not currently expose scans or general transactions, so those workloads are intentionally absent.
- Directory size is measured after foreground work is drained and the database is closed. It is not a write-amplification measurement.
- Do not compare these numbers with results from another machine, filesystem, build, dataset, or durability profile.
