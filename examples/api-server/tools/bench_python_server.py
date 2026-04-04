#!/usr/bin/env python3

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, unquote

PORT = int(os.environ.get("PORT", "3000"))
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "dev-token")
DATA_DIR = Path(os.environ.get("DATA_DIR", os.path.join(os.getcwd(), "data")))
MAX_REQUESTS = int(os.environ.get("MAX_REQUESTS", "0"))

DATA_DIR.mkdir(parents=True, exist_ok=True)
STORE = DATA_DIR / "users.json"
if not STORE.exists():
    STORE.write_text("[]", encoding="utf-8")

handled_requests = 0


def read_users():
    return json.loads(STORE.read_text(encoding="utf-8"))


def write_users(users):
    STORE.write_text(json.dumps(users, indent=2), encoding="utf-8")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return

    def send_json(self, status, body):
        encoded = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(encoded)))
        self.send_header("connection", "close")
        self.end_headers()
        self.wfile.write(encoded)
        self.wfile.flush()

    def authorized(self):
        return self.headers.get("authorization", "") == f"Bearer {AUTH_TOKEN}"

    def mark_request(self):
        global handled_requests
        handled_requests += 1
        if MAX_REQUESTS > 0 and handled_requests >= MAX_REQUESTS:
            self.server.shutdown()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self.mark_request()
            self.send_json(200, {"ok": True, "service": "python-api", "status": "ready"})
            return
        if parsed.path == "/users":
            self.mark_request()
            if not self.authorized():
                self.send_json(401, {"ok": False, "error": {"code": "unauthorized", "message": "missing or invalid bearer token", "details": None}})
                return
            self.send_json(200, {"ok": True, "users": read_users()})
            return
        if parsed.path.startswith("/users/"):
            self.mark_request()
            if not self.authorized():
                self.send_json(401, {"ok": False, "error": {"code": "unauthorized", "message": "missing or invalid bearer token", "details": None}})
                return
            user_id = unquote(parsed.path[len("/users/"):])
            for user in read_users():
                if str(user.get("id")) == user_id:
                    self.send_json(200, {"ok": True, "user": user})
                    return
            self.send_json(404, {"ok": False, "error": {"code": "user_not_found", "message": "user not found", "details": None}})
            return
        self.send_json(404, {"ok": False, "error": {"code": "not_found", "message": "not found", "details": None}})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/users":
            self.send_json(404, {"ok": False, "error": {"code": "not_found", "message": "not found", "details": None}})
            return
        self.mark_request()
        if not self.authorized():
            self.send_json(401, {"ok": False, "error": {"code": "unauthorized", "message": "missing or invalid bearer token", "details": None}})
            return
        try:
            length = int(self.headers.get("content-length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:
            self.send_json(400, {"ok": False, "error": {"code": "invalid_body", "message": "body must be a JSON object", "details": None}})
            return
        name = str(payload.get("name", "")).strip()
        email = str(payload.get("email", "")).strip().lower()
        if not name:
            self.send_json(400, {"ok": False, "error": {"code": "invalid_name", "message": "name is required", "details": None}})
            return
        if not email:
            self.send_json(400, {"ok": False, "error": {"code": "invalid_email", "message": "email is required", "details": None}})
            return
        users = read_users()
        next_id = str(max([int(user.get("id", 0)) for user in users] + [0]) + 1)
        user = {"id": next_id, "name": name, "email": email}
        users.append(user)
        write_users(users)
        self.send_json(201, {"ok": True, "user": user})


def main():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f'[info] "python-api starting" {{"port": {PORT}}}', file=os.sys.stderr, flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
