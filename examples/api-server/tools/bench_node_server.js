#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const http = require("node:http");
const { URL } = require("node:url");

const port = Number(process.env.PORT || "3000");
const authToken = process.env.AUTH_TOKEN || "dev-token";
const dataDir = process.env.DATA_DIR || path.join(process.cwd(), "data");
const maxRequests = Number(process.env.MAX_REQUESTS || "0");

function ensureStore() {
  fs.mkdirSync(dataDir, { recursive: true });
  const file = path.join(dataDir, "users.json");
  if (!fs.existsSync(file)) {
    fs.writeFileSync(file, "[]");
  }
  return file;
}

const storeFile = ensureStore();
let handledRequests = 0;

function readUsers() {
  return JSON.parse(fs.readFileSync(storeFile, "utf8"));
}

function writeUsers(users) {
  fs.writeFileSync(storeFile, JSON.stringify(users, null, 2));
}

function sendJson(res, status, body) {
  const encoded = JSON.stringify(body);
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(encoded),
    "connection": "close",
  });
  res.end(encoded);
}

function authorized(req) {
  return (req.headers.authorization || "") === `Bearer ${authToken}`;
}

function notFound(res) {
  sendJson(res, 404, { ok: false, error: { code: "not_found", message: "not found", details: null } });
}

function unauthorized(res) {
  sendJson(res, 401, { ok: false, error: { code: "unauthorized", message: "missing or invalid bearer token", details: null } });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.on("data", chunk => {
      body += chunk;
      if (body.length > 128 * 1024) {
        reject(new Error("body too large"));
        req.destroy();
      }
    });
    req.on("end", () => resolve(body));
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "127.0.0.1"}`);
  if (req.method === "GET" && url.pathname === "/health") {
    handledRequests += 1;
    sendJson(res, 200, { ok: true, service: "node-api", status: "ready" });
  } else if (url.pathname === "/users" || url.pathname.startsWith("/users/")) {
    if (!authorized(req)) {
      handledRequests += 1;
      unauthorized(res);
    } else if (req.method === "GET" && url.pathname === "/users") {
      handledRequests += 1;
      sendJson(res, 200, { ok: true, users: readUsers() });
    } else if (req.method === "GET" && url.pathname.startsWith("/users/")) {
      handledRequests += 1;
      const id = decodeURIComponent(url.pathname.slice("/users/".length));
      const user = readUsers().find(item => String(item.id) === id);
      if (!user) {
        sendJson(res, 404, { ok: false, error: { code: "user_not_found", message: "user not found", details: null } });
      } else {
        sendJson(res, 200, { ok: true, user });
      }
    } else if (req.method === "POST" && url.pathname === "/users") {
      handledRequests += 1;
      try {
        const payload = JSON.parse(await readBody(req));
        const users = readUsers();
        const id = String(users.reduce((max, user) => Math.max(max, Number(user.id || 0)), 0) + 1);
        const user = {
          id,
          name: String(payload.name || "").trim(),
          email: String(payload.email || "").trim().toLowerCase(),
        };
        if (!user.name) {
          sendJson(res, 400, { ok: false, error: { code: "invalid_name", message: "name is required", details: null } });
          return;
        }
        if (!user.email) {
          sendJson(res, 400, { ok: false, error: { code: "invalid_email", message: "email is required", details: null } });
          return;
        }
        users.push(user);
        writeUsers(users);
        sendJson(res, 201, { ok: true, user });
      } catch (error) {
        sendJson(res, 400, { ok: false, error: { code: "invalid_body", message: "body must be a JSON object", details: null } });
      }
    } else {
      notFound(res);
    }
  } else {
    notFound(res);
  }

  if (maxRequests > 0 && handledRequests >= maxRequests) {
    setImmediate(() => server.close(() => process.exit(0)));
  }
});

server.listen(port, "127.0.0.1", () => {
  process.stderr.write(`[info] "node-api starting" {"port": ${port}}\n`);
});
