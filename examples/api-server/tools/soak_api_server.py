#!/usr/bin/env python3

import argparse
import concurrent.futures
import json
import os
import queue
import random
import shutil
import socket
import subprocess
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
            return round(int(line.split()[1]) / 1024.0, 2)
    return 0.0


def safe_get(mapping, *path, default=None):
    value = mapping
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def free_port():
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
            return response.status, (time.perf_counter() - started) * 1000.0, json.loads(text)
    except urllib.error.HTTPError as error:
        text = error.read().decode("utf-8")
        return error.code, (time.perf_counter() - started) * 1000.0, json.loads(text)


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
    for index in range(1, 6):
        request_json("POST", f"{base_url}/users", token=token, body={"name": f"user-{index}", "email": f"user-{index}@example.com"})


def launch_server(port, token, data_dir, max_threads=16, max_connections=4096):
    env = os.environ.copy()
    env.update({
        "PORT": str(port),
        "AUTH_TOKEN": token,
        "DATA_DIR": str(data_dir),
        "MAX_REQUESTS": "0",
        "LOG_REQUESTS": "fals",
        "METRICS_AUTH": "fals",
        "COLLECT_METRICS": "tru",
        "TIMEOUT_MS": "20000",
        "READ_TIMEOUT_MS": "20000",
        "WRITE_TIMEOUT_MS": "20000",
        "MAX_CONNECTIONS": str(max_connections),
        "MAX_THREADS": str(max_threads),
    })
    return subprocess.Popen([str(DGM_BIN), "run"], cwd=EXAMPLE_DIR, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def stop_server(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
    stdout = process.stdout.read() if process.stdout else ""
    stderr = process.stderr.read() if process.stderr else ""
    return stdout, stderr


def phase_request(base_url, token, phase_name, counter):
    if phase_name == "baseline":
        return request_json("GET", f"{base_url}/users", token=token)
    if phase_name == "stress":
        if counter % 5 == 0:
            return request_json("POST", f"{base_url}/users", token=token, body={"name": f"stress-{counter}", "email": f"stress-{counter}@example.com"})
        if counter % 2 == 0:
            return request_json("GET", f"{base_url}/users/1", token=token)
        return request_json("GET", f"{base_url}/users", token=token)
    if counter % 7 == 0:
        return request_json("GET", f"{base_url}/users")
    if counter % 11 == 0:
        sock = socket.create_connection(("127.0.0.1", int(base_url.rsplit(":", 1)[1])), timeout=1.0)
        sock.sendall(b"GET /users HTTP/1.1\r\nHost: 127.0.0.1\r\n")
        sock.close()
        return 0, 0.0, {"ok": False, "transport": "partial-close"}
    if counter % 3 == 0:
        time.sleep(random.uniform(0.0, 0.02))
        return request_json("GET", f"{base_url}/users/1", token=token)
    return request_json("GET", f"{base_url}/users", token=token)


def mixed_request(base_url, token, counter):
    bucket = counter % 10
    if bucket < 7:
        return request_json("GET", f"{base_url}/users", token=token)
    if bucket < 9:
        return request_json("GET", f"{base_url}/users/1", token=token)
    return request_json("POST", f"{base_url}/users", token=token, body={"name": f"soak-{counter}", "email": f"soak-{counter}@example.com"})


def stationary_request(base_url, token, counter):
    if counter % 10 < 7:
        return request_json("GET", f"{base_url}/users", token=token)
    return request_json("GET", f"{base_url}/users/1", token=token)


def collect_metrics(base_url):
    try:
        status, _, body = request_json("GET", f"{base_url}/metrics")
        if status == 200:
            return body
    except Exception:
        pass
    return None


def trigger_trim(base_url):
    try:
        status, _, body = request_json("POST", f"{base_url}/metrics/trim")
        if status == 200:
            return body
    except Exception:
        pass
    return None


class RequestAccumulator:
    def __init__(self):
        self._lock = threading.Lock()
        self._total_requests = 0
        self._total_successes = 0
        self._total_errors = 0
        self._total_latency_ms = 0.0
        self._interval_latencies = []
        self._interval_requests = 0
        self._interval_successes = 0
        self._interval_errors = 0

    def record(self, status, latency_ms):
        success = 200 <= status < 400
        with self._lock:
            self._total_requests += 1
            self._total_successes += 1 if success else 0
            self._total_errors += 0 if success else 1
            self._total_latency_ms += latency_ms
            self._interval_requests += 1
            self._interval_successes += 1 if success else 0
            self._interval_errors += 0 if success else 1
            self._interval_latencies.append(latency_ms)

    def snapshot_interval(self, elapsed_s):
        with self._lock:
            requests = self._interval_requests
            successes = self._interval_successes
            errors = self._interval_errors
            latencies = self._interval_latencies
            self._interval_requests = 0
            self._interval_successes = 0
            self._interval_errors = 0
            self._interval_latencies = []
        return {
            "requests": requests,
            "successes": successes,
            "errors": errors,
            "error_rate": round(errors / requests, 6) if requests else 0.0,
            "req_per_sec": round(requests / max(elapsed_s, 0.001), 2),
            "latency_ms": {
                "avg": round(sum(latencies) / len(latencies), 2) if latencies else 0.0,
                "p50": round(percentile(latencies, 0.50), 2),
                "p95": round(percentile(latencies, 0.95), 2),
                "p99": round(percentile(latencies, 0.99), 2),
                "max": round(max(latencies), 2) if latencies else 0.0,
            },
        }

    def totals(self, elapsed_s):
        with self._lock:
            requests = self._total_requests
            successes = self._total_successes
            errors = self._total_errors
            total_latency_ms = self._total_latency_ms
        return {
            "requests": requests,
            "successes": successes,
            "errors": errors,
            "error_rate": round(errors / requests, 6) if requests else 0.0,
            "req_per_sec": round(requests / max(elapsed_s, 0.001), 2),
            "latency_ms": {
                "avg": round(total_latency_ms / requests, 2) if requests else 0.0,
            },
        }


def run_phase(base_url, token, phase_name, duration_s, rps, concurrency):
    statuses = []
    latencies = []
    started = time.perf_counter()
    submitted = 0
    next_tick = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        while time.perf_counter() - started < duration_s:
            now = time.perf_counter()
            if now < next_tick:
                time.sleep(min(next_tick - now, 0.01))
                continue
            futures.append(executor.submit(phase_request, base_url, token, phase_name, submitted))
            submitted += 1
            next_tick += 1.0 / max(rps, 1)
        for future in concurrent.futures.as_completed(futures):
            status, latency_ms, _ = future.result()
            if status != 0:
                statuses.append(status)
                latencies.append(latency_ms)
    successes = sum(1 for status in statuses if 200 <= status < 400)
    return {
        "name": phase_name,
        "requests": len(statuses),
        "successes": successes,
        "errors": len(statuses) - successes,
        "latency_ms": {
            "avg": round(sum(latencies) / len(latencies), 2) if latencies else 0.0,
            "p50": round(percentile(latencies, 0.50), 2),
            "p95": round(percentile(latencies, 0.95), 2),
            "p99": round(percentile(latencies, 0.99), 2),
            "max": round(max(latencies), 2) if latencies else 0.0,
        },
    }


def run_soak(
    base_url,
    token,
    process,
    duration_s,
    rps,
    concurrency,
    sample_interval_s,
    mode_name,
    workload,
    request_fn,
    idle_after_s=0.0,
    trim_after_idle=False,
    trim_settle_s=2.0,
):
    accumulator = RequestAccumulator()
    completed = queue.Queue()
    started_at = time.perf_counter()
    last_sample = started_at
    next_submit = started_at
    next_sample = started_at + sample_interval_s
    submitted = 0
    inflight = 0
    peak_inflight = 0
    samples = []

    def submit_request(executor, counter):
        future = executor.submit(request_fn, base_url, token, counter)

        def done(task):
            try:
                completed.put(task.result())
            except Exception:
                completed.put((599, 0.0, {"ok": False, "transport": "exception"}))

        future.add_done_callback(done)

    def drain(block=False, timeout=0.05):
        nonlocal inflight
        drained = 0
        while True:
            try:
                if block and drained == 0:
                    status, latency_ms, _ = completed.get(timeout=timeout)
                else:
                    status, latency_ms, _ = completed.get_nowait()
            except queue.Empty:
                break
            drained += 1
            inflight = max(inflight - 1, 0)
            if status != 0:
                accumulator.record(status, latency_ms)
        return drained

    def sample(now):
        nonlocal last_sample
        metrics = collect_metrics(base_url) or {}
        interval = accumulator.snapshot_interval(now - last_sample)
        last_sample = now
        samples.append({
            "time": time.time(),
            "elapsed_s": round(now - started_at, 2),
            "rss_mb": read_rss_mb(process.pid),
            "inflight": inflight,
            "interval": interval,
            "open_sockets": safe_get(metrics, "runtime", "open_sockets", default=0),
            "retained_tasks": safe_get(metrics, "async", "retained_tasks", default=0),
            "heap_alloc_bytes": safe_get(metrics, "profile", "alloc_bytes", default=0),
            "heap_peak_bytes": safe_get(metrics, "profile", "peak_heap_bytes", default=0),
            "metrics": metrics,
        })

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        while True:
            now = time.perf_counter()
            if now - started_at >= duration_s:
                break
            drain()
            while now >= next_submit and inflight < concurrency:
                submit_request(executor, submitted)
                submitted += 1
                inflight += 1
                peak_inflight = max(peak_inflight, inflight)
                next_submit += 1.0 / max(rps, 1)
                now = time.perf_counter()
                drain()
            if now >= next_sample:
                sample(now)
                next_sample += sample_interval_s
                continue
            if inflight >= concurrency and completed.empty():
                drain(block=True, timeout=0.05)
            else:
                time.sleep(0.002)
        while inflight > 0:
            drain(block=True, timeout=0.1)

    finished_at = time.perf_counter()
    if finished_at - last_sample > 0.0:
        sample(finished_at)
    final_metrics = collect_metrics(base_url) or (samples[-1]["metrics"] if samples else {})
    summary = accumulator.totals(finished_at - started_at)
    rss_values = [entry["rss_mb"] for entry in samples]
    first_rss = rss_values[0] if rss_values else read_rss_mb(process.pid)
    final_rss = read_rss_mb(process.pid)
    idle_probe = None
    trim_probe = None
    if idle_after_s > 0:
        before_idle_rss = final_rss
        before_idle_metrics = final_metrics
        time.sleep(idle_after_s)
        after_idle_metrics = collect_metrics(base_url) or before_idle_metrics
        after_idle_rss = read_rss_mb(process.pid)
        idle_probe = {
            "duration_s": idle_after_s,
            "before_rss_mb": before_idle_rss,
            "after_rss_mb": after_idle_rss,
            "delta_mb": round(after_idle_rss - before_idle_rss, 2),
            "delta_pct": round(((after_idle_rss - before_idle_rss) / before_idle_rss) * 100.0, 2) if before_idle_rss else 0.0,
            "before_metrics": before_idle_metrics,
            "after_metrics": after_idle_metrics,
        }
        final_metrics = after_idle_metrics
        final_rss = after_idle_rss
    if trim_after_idle:
        before_trim_rss = final_rss
        before_trim_metrics = final_metrics
        trim_result = trigger_trim(base_url) or {}
        if trim_settle_s > 0:
            time.sleep(trim_settle_s)
        after_trim_metrics = collect_metrics(base_url) or trim_result or before_trim_metrics
        after_trim_rss = read_rss_mb(process.pid)
        trim_probe = {
            "requested": True,
            "trimmed": bool(safe_get(trim_result, "trimmed", default=False)),
            "settle_s": trim_settle_s,
            "before_rss_mb": before_trim_rss,
            "after_rss_mb": after_trim_rss,
            "delta_mb": round(after_trim_rss - before_trim_rss, 2),
            "delta_pct": round(((after_trim_rss - before_trim_rss) / before_trim_rss) * 100.0, 2) if before_trim_rss else 0.0,
            "before_metrics": before_trim_metrics,
            "after_metrics": after_trim_metrics,
        }
        final_metrics = after_trim_metrics
        final_rss = after_trim_rss
    return {
        "mode": mode_name,
        "workload": workload,
        "duration_s": duration_s,
        "concurrency": concurrency,
        "rps_target": rps,
        "sample_interval_s": sample_interval_s,
        "submitted_requests": submitted,
        "peak_inflight": peak_inflight,
        "peak_rss_mb": max(rss_values, default=final_rss),
        "final_rss_mb": final_rss,
        "rss_drift_pct": round(((final_rss - first_rss) / first_rss) * 100.0, 2) if first_rss else 0.0,
        "summary": summary,
        "final_metrics": final_metrics,
        "idle_probe": idle_probe,
        "trim_probe": trim_probe,
        "samples": samples,
        "checks": {
            "retained_tasks_zero": safe_get(final_metrics, "async", "retained_tasks", default=0) == 0,
            "open_sockets_zero": safe_get(final_metrics, "runtime", "open_sockets", default=0) == 0,
            "error_free": summary["errors"] == 0,
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--output", default=str(RESULTS_DIR / "soak-smoke.json"))
    parser.add_argument("--mode", choices=("phased", "mixed", "stationary"))
    parser.add_argument("--duration", type=int)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--rps", type=int)
    parser.add_argument("--sample-interval", type=float, default=10.0)
    parser.add_argument("--max-threads", type=int, default=16)
    parser.add_argument("--max-connections", type=int, default=4096)
    parser.add_argument("--idle-after", type=float, default=0.0)
    parser.add_argument("--trim-after", action="store_true")
    parser.add_argument("--trim-settle", type=float, default=2.0)
    args = parser.parse_args()

    if not DGM_BIN.exists():
        raise SystemExit(f"missing DGM binary at {DGM_BIN}; run cargo build first")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    data_dir = Path(tempfile.mkdtemp(prefix="dgm-soak-"))
    port = free_port()
    token = "soak-token"
    base_url = f"http://127.0.0.1:{port}"
    process = launch_server(port, token, data_dir, max_threads=args.max_threads, max_connections=args.max_connections)

    try:
        wait_ready(base_url)
        seed_users(base_url, token)
        mode = args.mode
        if mode is None:
            mode = "mixed" if (args.duration is not None or args.concurrency is not None or args.rps is not None) else "phased"
        if args.quick and mode == "mixed":
            payload = run_soak(
                base_url,
                token,
                process,
                30,
                60,
                24,
                2.0,
                "mixed",
                {
                    "GET /users": 0.7,
                    "GET /users/:id": 0.2,
                    "POST /users": 0.1,
                },
                mixed_request,
                idle_after_s=args.idle_after,
                trim_after_idle=args.trim_after,
                trim_settle_s=args.trim_settle,
            )
        elif args.quick and mode == "stationary":
            payload = run_soak(
                base_url,
                token,
                process,
                30,
                60,
                24,
                2.0,
                "stationary",
                {
                    "GET /users": 0.7,
                    "GET /users/:id": 0.3,
                },
                stationary_request,
                idle_after_s=args.idle_after,
                trim_after_idle=args.trim_after,
                trim_settle_s=args.trim_settle,
            )
        elif mode == "mixed":
            payload = run_soak(
                base_url,
                token,
                process,
                args.duration if args.duration is not None else 3600,
                args.rps if args.rps is not None else 200,
                args.concurrency if args.concurrency is not None else 64,
                args.sample_interval,
                "mixed",
                {
                    "GET /users": 0.7,
                    "GET /users/:id": 0.2,
                    "POST /users": 0.1,
                },
                mixed_request,
                idle_after_s=args.idle_after,
                trim_after_idle=args.trim_after,
                trim_settle_s=args.trim_settle,
            )
        elif mode == "stationary":
            payload = run_soak(
                base_url,
                token,
                process,
                args.duration if args.duration is not None else 3600,
                args.rps if args.rps is not None else 200,
                args.concurrency if args.concurrency is not None else 64,
                args.sample_interval,
                "stationary",
                {
                    "GET /users": 0.7,
                    "GET /users/:id": 0.3,
                },
                stationary_request,
                idle_after_s=args.idle_after,
                trim_after_idle=args.trim_after,
                trim_settle_s=args.trim_settle,
            )
        else:
            if args.quick:
                phases = [
                    ("baseline", 8, 40, 16),
                    ("stress", 8, 120, 32),
                    ("chaos", 8, 80, 24),
                ]
            else:
                phases = [
                    ("baseline", 3600, 80, 24),
                    ("stress", 3600, 300, 48),
                    ("chaos", 3600, 180, 32),
                ]
            phase_results = [run_phase(base_url, token, *phase) for phase in phases]
            payload = {
                "mode": "quick" if args.quick else "full",
                "peak_rss_mb": 0.0,
                "final_rss_mb": read_rss_mb(process.pid),
                "phases": phase_results,
                "final_metrics": collect_metrics(base_url),
                "samples": [],
            }
        payload["generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        payload["pid"] = process.pid
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps({
            "peak_rss_mb": payload["peak_rss_mb"],
            "final_rss_mb": payload["final_rss_mb"],
            "mode": payload["mode"],
            "summary": payload.get("summary"),
            "phases": payload.get("phases"),
            "written": str(output_path),
        }, indent=2))
    finally:
        stdout, stderr = stop_server(process)
        shutil.rmtree(data_dir, ignore_errors=True)
        if process.returncode not in (0, -15, None):
            raise RuntimeError(f"DGM server exited with {process.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}")


if __name__ == "__main__":
    main()
