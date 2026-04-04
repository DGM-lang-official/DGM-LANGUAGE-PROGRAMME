# DGM API Server

Một backend mẫu chạy hoàn toàn bằng `DGM`, dùng:

- `http.new()` cho routing
- middleware viết bằng DGM
- bearer token auth
- JSON file store cho `users`
- bundle/source đều chạy được

## Run

```bash
cd examples/api-server
AUTH_TOKEN=dev-token PORT=3000 dgm run
```

## Endpoints

- `GET /health`
- `GET /metrics`
- `GET /users`
- `POST /users`
- `GET /users/:id`

## Auth

Tất cả route `/users*` yêu cầu header:

```text
Authorization: Bearer <AUTH_TOKEN>
```

`/metrics` cũng dùng bearer token mặc định. Có thể tắt bằng `METRICS_AUTH=fals`.

## Create User

```bash
curl -X POST http://127.0.0.1:3000/users \
  -H 'authorization: Bearer dev-token' \
  -H 'content-type: application/json' \
  -d '{"name":"Minh","email":"minh@example.com"}'
```

## Bundle

```bash
dgm build --output dist/api-server.dgm
dgm run dist/api-server.dgm
```

## Environment

- `PORT`
- `AUTH_TOKEN`
- `DATA_DIR`
- `MAX_THREADS`
- `MAX_CONNECTIONS`
- `TIMEOUT_MS`
- `READ_TIMEOUT_MS`
- `WRITE_TIMEOUT_MS`
- `MAX_BODY_SIZE`
- `MAX_REQUESTS`
- `LOG_REQUESTS`
- `METRICS_AUTH`
- `COLLECT_METRICS`

## Soak & Benchmark

```bash
python3 tools/soak_api_server.py --mode stationary --duration 86400 --concurrency 64 --rps 200 --sample-interval 10 --max-threads 16 --output results/soak-stationary-24h.json
python3 tools/soak_api_server.py --mode mixed --duration 86400 --concurrency 64 --rps 200 --sample-interval 10 --max-threads 16 --output results/soak-24h.json
python3 tools/benchmark_api_server.py --max-threads 16 --output results/benchmark-lock-state.json --summary-output results/final-benchmark.json
```

Các lệnh này ghi kết quả vào `results/`.

## Current Validation Snapshot

Máy đo:

- Fedora 43
- Intel i7-6500U
- `target/debug/dgm`
- ngày chạy: `2026-04-03`

Benchmark baseline hiện tại:

| Runtime | Scenario | req/sec | p95 | p99 | peak RSS |
|---------|----------|--------:|----:|----:|---------:|
| DGM | `GET /users` | 433.47 | 106.89 ms | 128.82 ms | 720.43 MB |
| DGM | `GET /users/:id` | 373.60 | 122.83 ms | 140.41 ms | 720.43 MB |
| DGM | `POST /users` | 138.34 | 154.81 ms | 174.87 ms | 720.43 MB |
| Node | `GET /users` | 1443.40 | 36.41 ms | 43.47 ms | 67.92 MB |
| Node | `GET /users/:id` | 1547.27 | 34.92 ms | 41.80 ms | 67.92 MB |
| Node | `POST /users` | 938.65 | 16.27 ms | 16.79 ms | 67.92 MB |
| Python | `GET /users` | 767.76 | 18.89 ms | 1054.47 ms | 23.46 MB |
| Python | `GET /users/:id` | 567.85 | 10.28 ms | 1070.37 ms | 23.46 MB |
| Python | `POST /users` | 27.20 | 88.27 ms | 10011.85 ms | 23.46 MB |

Mixed soak snapshot sau response-path fix:

- command:
  `python3 tools/soak_api_server.py --mode mixed --duration 120 --concurrency 64 --rps 200 --sample-interval 10 --max-threads 16 --output results/soak-lock-state-after-response-fix.json`
- workload: `70% GET /users`, `20% GET /users/:id`, `10% POST /users`
- requests: `16449`
- errors: `0`
- avg req/sec: `135.94`
- avg latency: `324.52 ms`
- first sampled RSS at `10s`: `531.99 MB`
- peak RSS: `3627.85 MB`
- final RSS: `3360.78 MB`
- RSS drift: `+531.74%`
- `retained_tasks = 0`
- `open_sockets = 0`
- route counts cuối run:
  `users.index = 11515`, `users.show = 3290`, `users.create = 1649`

Stationary soak snapshot:

- command:
  `python3 tools/soak_api_server.py --mode stationary --duration 120 --concurrency 64 --rps 200 --sample-interval 10 --max-threads 16 --output results/soak-stationary-120s.json`
- workload: `70% GET /users`, `30% GET /users/:id`, không có `POST` ngoài 5 request seed ban đầu
- requests: `24000`
- errors: `0`
- avg req/sec: `199.97`
- avg latency: `67.04 ms`
- first sampled RSS at `10s`: `533.10 MB`
- peak RSS: `4019.16 MB`
- final RSS: `3432.27 MB`
- RSS drift: `+543.83%`
- `retained_tasks = 0`
- `open_sockets = 0`
- route counts cuối run:
  `users.index = 16800`, `users.show = 7200`, `users.create = 5`

Kết luận thẳng:

- `DGM` đã đủ để chạy backend thật và benchmark đối chiếu với Node/Python.
- throughput của `DGM` đã vào mức usable, nhưng vẫn kém Node rõ rệt.
- response path cho `GET /users` đã bỏ re-parse `json.raw(...)`; alloc hot path cũ không còn nằm ở `json.raw`.
- mixed soak `120s` vẫn hữu ích để đo growth/scalability, nhưng không phải leak test vì `10% POST /users` làm `users.json` lớn dần trong suốt run.
- stationary soak `120s` giữ dataset gần như cố định mà RSS vẫn tăng từ `533 MB` lên `4.0 GB` rồi chỉ hạ về `3.4 GB`; điều này xác nhận còn tồn tại memory/retention instability thật ở runtime/request path, không còn bị nhiễu bởi data growth.
