#!/usr/bin/env python3

import argparse
import concurrent.futures
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path


TOOLS_DIR = Path(__file__).resolve().parent
EXAMPLE_DIR = TOOLS_DIR.parent
REPO_DIR = EXAMPLE_DIR.parent.parent
DGM_BIN = REPO_DIR / "target" / "debug" / "dgm"
RESULTS_DIR = EXAMPLE_DIR / "results"


def percentile(values, ratio):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int((len(ordered) - 1) * ratio)
    return ordered[index]


def read_rss_mb(pid):
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return 0.0
    for line in status_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            parts = line.split()
            return round(int(parts[1]) / 1024.0, 2)
    return 0.0


def read_cpu_ticks(pid):
    stat_path = Path(f"/proc/{pid}/stat")
    if not stat_path.exists():
        return None
    text = stat_path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    rparen = text.rfind(")")
    if rparen < 0:
        return None
    fields = text[rparen + 2 :].split()
    if len(fields) < 13:
        return None
    utime = int(fields[11])
    stime = int(fields[12])
    return utime + stime


class ProcessSampler:
    def __init__(self, pid):
        self.pid = pid
        self.peak_rss_mb = 0.0
        self.peak_cpu_pct = 0.0
        self.avg_cpu_pct = 0.0
        self._cpu_samples = 0
        self.samples = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=2)

    def _run(self):
        ticks_per_second = os.sysconf("SC_CLK_TCK")
        last_ticks = read_cpu_ticks(self.pid)
        last_time = time.perf_counter()
        while not self._stop.is_set():
            now = time.perf_counter()
            rss = read_rss_mb(self.pid)
            cpu_pct = 0.0
            current_ticks = read_cpu_ticks(self.pid)
            if last_ticks is not None and current_ticks is not None:
                elapsed = max(now - last_time, 0.001)
                cpu_pct = max((current_ticks - last_ticks) / ticks_per_second / elapsed * 100.0, 0.0)
                self.peak_cpu_pct = max(self.peak_cpu_pct, cpu_pct)
                self._cpu_samples += 1
                self.avg_cpu_pct += (cpu_pct - self.avg_cpu_pct) / self._cpu_samples
            last_ticks = current_ticks
            last_time = now
            self.peak_rss_mb = max(self.peak_rss_mb, rss)
            self.samples.append({"time": time.time(), "rss_mb": rss, "cpu_pct": round(cpu_pct, 2)})
            time.sleep(0.1)


def free_port():
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request_json(method, url, token=None, body=None, timeout=10.0):
    headers = {}
    if token:
        headers["authorization"] = f"Bearer {token}"
    data = None
    if body is not None:
        headers["content-type"] = "application/json"
        data = json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8")
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            return response.status, elapsed_ms, json.loads(text)
    except urllib.error.HTTPError as error:
        text = error.read().decode("utf-8")
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return error.code, elapsed_ms, json.loads(text)


def wait_ready(base_url, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = request_json("GET", f"{base_url}/health", timeout=1.0)
            if status == 200:
                return
        except Exception:
            pass
        time.sleep(0.05)
    raise RuntimeError("server did not become ready")


def seed_users(base_url, token):
    request_json("POST", f"{base_url}/users", token=token, body={"name": "Minh", "email": "minh@example.com"})
    request_json("POST", f"{base_url}/users", token=token, body={"name": "Lan", "email": "lan@example.com"})


def run_scenario(base_url, token, name, method, path_factory, total_requests, concurrency, body_factory=None):
    statuses = []
    latencies = []
    started = time.perf_counter()

    def one(index):
        body = body_factory(index) if body_factory else None
        started = time.perf_counter()
        try:
            status, elapsed_ms, _ = request_json(method, f"{base_url}{path_factory(index)}", token=token, body=body)
            return status, elapsed_ms
        except Exception:
            return 599, (time.perf_counter() - started) * 1000.0

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(one, index) for index in range(total_requests)]
        for future in concurrent.futures.as_completed(futures):
            status, elapsed_ms = future.result()
            statuses.append(status)
            latencies.append(elapsed_ms)

    elapsed = max(time.perf_counter() - started, 0.001)
    successes = sum(1 for status in statuses if 200 <= status < 400)
    return {
        "name": name,
        "requests": total_requests,
        "successes": successes,
        "errors": total_requests - successes,
        "requests_per_sec": round(total_requests / elapsed, 2),
        "latency_ms": {
            "avg": round(sum(latencies) / len(latencies), 2),
            "p50": round(percentile(latencies, 0.50), 2),
            "p95": round(percentile(latencies, 0.95), 2),
            "p99": round(percentile(latencies, 0.99), 2),
            "max": round(max(latencies), 2),
        },
    }


def launch_server(runtime_name, port, token, data_dir, max_threads=64, max_connections=2048):
    env = os.environ.copy()
    env.update({
        "PORT": str(port),
        "AUTH_TOKEN": token,
        "DATA_DIR": str(data_dir),
        "MAX_REQUESTS": "0",
        "LOG_REQUESTS": "fals",
        "METRICS_AUTH": "fals",
        "COLLECT_METRICS": "fals",
        "TIMEOUT_MS": "20000",
        "READ_TIMEOUT_MS": "20000",
        "WRITE_TIMEOUT_MS": "20000",
        "MAX_CONNECTIONS": str(max_connections),
        "MAX_THREADS": str(max_threads),
    })
    if runtime_name == "dgm":
        command = [str(DGM_BIN), "run"]
        cwd = EXAMPLE_DIR
    elif runtime_name == "node":
        command = ["node", str(TOOLS_DIR / "bench_node_server.js")]
        cwd = EXAMPLE_DIR
    elif runtime_name == "python":
        command = ["python3", str(TOOLS_DIR / "bench_python_server.py")]
        cwd = EXAMPLE_DIR
    else:
        raise ValueError(runtime_name)
    return subprocess.Popen(command, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def stop_server(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
    stderr = process.stderr.read() if process.stderr else ""
    stdout = process.stdout.read() if process.stdout else ""
    return stdout, stderr


def benchmark_runtime(runtime_name, quick, max_threads=64, max_connections=2048):
    data_dir = Path(tempfile.mkdtemp(prefix=f"dgm-bench-{runtime_name}-"))
    port = free_port()
    token = "bench-token"
    process = launch_server(runtime_name, port, token, data_dir, max_threads=max_threads, max_connections=max_connections)
    sampler = ProcessSampler(process.pid)
    try:
        sampler.start()
        wait_ready(f"http://127.0.0.1:{port}")
        seed_users(f"http://127.0.0.1:{port}", token)
        if quick:
            scenarios = [
                ("GET /users", "GET", lambda _: "/users", 240, 24, None),
                ("GET /users/:id", "GET", lambda _: "/users/1", 240, 24, None),
                ("POST /users", "POST", lambda _: "/users", 80, 8, lambda i: {"name": f"user-{i}", "email": f"user-{i}@example.com"}),
            ]
        else:
            scenarios = [
                ("GET /users", "GET", lambda _: "/users", 1200, 32, None),
                ("GET /users/:id", "GET", lambda _: "/users/1", 1200, 32, None),
                ("POST /users", "POST", lambda _: "/users", 300, 12, lambda i: {"name": f"user-{i}", "email": f"user-{i}@example.com"}),
            ]
        results = [run_scenario(f"http://127.0.0.1:{port}", token, *scenario) for scenario in scenarios]
        metrics = None
        if runtime_name == "dgm":
            try:
                _, _, metrics = request_json("GET", f"http://127.0.0.1:{port}/metrics", token=token)
            except Exception:
                metrics = None
        return {
            "runtime": runtime_name,
            "port": port,
            "peak_rss_mb": sampler.peak_rss_mb,
            "final_rss_mb": read_rss_mb(process.pid),
            "avg_cpu_pct": round(sampler.avg_cpu_pct, 2),
            "peak_cpu_pct": round(sampler.peak_cpu_pct, 2),
            "scenarios": results,
            "metrics": metrics,
        }
    finally:
        sampler.stop()
        stdout, stderr = stop_server(process)
        shutil.rmtree(data_dir, ignore_errors=True)
        if process.returncode not in (0, -15, None):
            raise RuntimeError(f"{runtime_name} server exited with {process.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}")


def print_table(results):
    headers = ["Runtime", "Scenario", "req/sec", "p95 ms", "p99 ms", "peak RSS MB", "avg CPU %", "peak CPU %"]
    rows = []
    for result in results:
        for scenario in result["scenarios"]:
            rows.append([
                result["runtime"],
                scenario["name"],
                f'{scenario["requests_per_sec"]:.2f}',
                f'{scenario["latency_ms"]["p95"]:.2f}',
                f'{scenario["latency_ms"]["p99"]:.2f}',
                f'{result["peak_rss_mb"]:.2f}',
                f'{result["avg_cpu_pct"]:.2f}',
                f'{result["peak_cpu_pct"]:.2f}',
            ])
    widths = [max(len(headers[index]), max(len(row[index]) for row in rows)) for index in range(len(headers))]
    print(" | ".join(headers[index].ljust(widths[index]) for index in range(len(headers))))
    print("-+-".join("-" * widths[index] for index in range(len(headers))))
    for row in rows:
        print(" | ".join(row[index].ljust(widths[index]) for index in range(len(headers))))


def summarize_results(results):
    summary = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
    for result in results:
        scenarios = {}
        for scenario in result["scenarios"]:
            scenarios[scenario["name"]] = {
                "req_per_sec": scenario["requests_per_sec"],
                "p50_ms": scenario["latency_ms"]["p50"],
                "p95_ms": scenario["latency_ms"]["p95"],
                "p99_ms": scenario["latency_ms"]["p99"],
                "errors": scenario["errors"],
            }
        summary[result["runtime"]] = {
            "peak_rss_mb": result["peak_rss_mb"],
            "final_rss_mb": result["final_rss_mb"],
            "avg_cpu_pct": result["avg_cpu_pct"],
            "peak_cpu_pct": result["peak_cpu_pct"],
            "scenarios": scenarios,
        }
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output", default=str(RESULTS_DIR / "benchmark-smoke.json"))
    parser.add_argument("--summary-output")
    parser.add_argument("--max-threads", type=int, default=64)
    parser.add_argument("--max-connections", type=int, default=2048)
    args = parser.parse_args()

    if not DGM_BIN.exists():
        raise SystemExit(f"missing DGM binary at {DGM_BIN}; run cargo build first")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results = [
        benchmark_runtime(runtime, args.quick, max_threads=args.max_threads, max_connections=args.max_connections)
        for runtime in ("dgm", "node", "python")
    ]
    payload = {
        "mode": "quick" if args.quick else "full",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "results": results,
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    summary_output = Path(args.summary_output) if args.summary_output else (RESULTS_DIR / "final-benchmark.json" if not args.quick else None)
    if summary_output is not None:
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        summary_output.write_text(json.dumps(summarize_results(results), indent=2), encoding="utf-8")
    print_table(results)
    print(f"\nwritten: {output_path}")
    if summary_output is not None:
        print(f"summary: {summary_output}")


if __name__ == "__main__":
    main()
