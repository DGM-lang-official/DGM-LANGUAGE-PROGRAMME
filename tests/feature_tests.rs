use dgm::{bundle, run_bundle, run_source, run_source_with_path};
use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::json;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tar::Builder;

fn temp_dir(name: &str) -> PathBuf {
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = std::env::temp_dir().join(format!("dgm-{name}-{stamp}"));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn copy_dir_recursive(src: &PathBuf, dst: &PathBuf) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_recursive(&src_path, &dst_path);
        } else {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::copy(&src_path, &dst_path).unwrap();
        }
    }
}

fn example_project(name: &str) -> PathBuf {
    let project = temp_dir(name);
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/api-server");
    copy_dir_recursive(&source, &project);
    project
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn send_with_retry<F>(mut request: F) -> ureq::Response
where
    F: FnMut() -> Result<ureq::Response, ureq::Error>,
{
    for _ in 0..40 {
        match request() {
            Ok(response) => return response,
            Err(ureq::Error::Status(_, response)) => return response,
            Err(ureq::Error::Transport(_)) => thread::sleep(Duration::from_millis(25)),
        }
    }
    panic!("server did not become ready");
}

fn send_with_retry_timeout<F>(mut request: F, timeout: Duration) -> ureq::Response
where
    F: FnMut() -> Result<ureq::Response, ureq::Error>,
{
    let deadline = Instant::now() + timeout;
    loop {
        match request() {
            Ok(response) => return response,
            Err(ureq::Error::Status(_, response)) => return response,
            Err(ureq::Error::Transport(_)) => {
                if Instant::now() >= deadline {
                    break;
                }
                thread::sleep(Duration::from_millis(25));
            }
        }
    }
    panic!("server did not become ready");
}

struct RegistryServer {
    base_url: String,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for RegistryServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.base_url.trim_start_matches("http://"));
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn start_registry_server<F>(build_routes: F) -> RegistryServer
where
    F: FnOnce(&str) -> Vec<(String, Vec<u8>, &'static str)> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);
    let routes = build_routes(&base_url);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_flag = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        while !stop_flag.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buf = [0u8; 8192];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    if n == 0 {
                        continue;
                    }
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    if let Some((_, body, content_type)) = routes.iter().find(|(route, _, _)| route == path) {
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            content_type,
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.write_all(body);
                    } else {
                        let body = b"not found";
                        let response = format!(
                            "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.write_all(body);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(10)),
                Err(_) => break,
            }
        }
    });
    RegistryServer {
        base_url,
        stop,
        handle: Some(handle),
    }
}

fn package_archive(files: Vec<(&str, Vec<u8>)>) -> Vec<u8> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = Builder::new(encoder);
    for (path, bytes) in files {
        let mut header = tar::Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(bytes.len() as u64);
        header.set_cksum();
        builder.append_data(&mut header, path, bytes.as_slice()).unwrap();
    }
    builder.finish().unwrap();
    builder.into_inner().unwrap().finish().unwrap()
}

fn raw_package_archive(files: Vec<(&str, Vec<u8>)>) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    for (path, bytes) in files {
        let mut header = [0u8; 512];
        write_tar_bytes(&mut header[0..100], path.as_bytes());
        write_tar_octal(&mut header[100..108], 0o644);
        write_tar_octal(&mut header[108..116], 0);
        write_tar_octal(&mut header[116..124], 0);
        write_tar_octal(&mut header[124..136], bytes.len() as u64);
        write_tar_octal(&mut header[136..148], 0);
        header[148..156].fill(b' ');
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum: u32 = header.iter().map(|b| *b as u32).sum();
        write_tar_checksum(&mut header[148..156], checksum);
        tar_bytes.extend_from_slice(&header);
        tar_bytes.extend_from_slice(&bytes);
        let padding = (512 - (bytes.len() % 512)) % 512;
        tar_bytes.extend(std::iter::repeat_n(0u8, padding));
    }
    tar_bytes.extend(std::iter::repeat_n(0u8, 1024));
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&tar_bytes).unwrap();
    encoder.finish().unwrap()
}

fn write_tar_bytes(field: &mut [u8], value: &[u8]) {
    let len = value.len().min(field.len());
    field[..len].copy_from_slice(&value[..len]);
}

fn write_tar_octal(field: &mut [u8], value: u64) {
    let width = field.len().saturating_sub(1);
    let text = format!("{value:o}");
    let start = width.saturating_sub(text.len());
    field.fill(b'0');
    field[start..start + text.len()].copy_from_slice(text.as_bytes());
    field[width] = 0;
}

fn write_tar_checksum(field: &mut [u8], value: u32) {
    let text = format!("{value:06o}\0 ");
    field.copy_from_slice(text.as_bytes());
}

fn package_with_files(name: &str, version: &str, main_source: &str, extra_files: Vec<(&str, Vec<u8>)>) -> Vec<u8> {
    let manifest = format!(
        "[package]\nname = \"{}\"\nversion = \"{}\"\n",
        name, version
    );
    let mut files = vec![
        ("dgm.toml", manifest.into_bytes()),
        ("main.dgm", main_source.as_bytes().to_vec()),
    ];
    files.extend(extra_files);
    package_archive(files)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn write_project_manifest(root: &PathBuf, name: &str, deps: &[(&str, &str)]) {
    let mut text = format!("[package]\nname = \"{}\"\nversion = \"0.1.0\"\n\n[dependencies]\n", name);
    for (dep, version) in deps {
        text.push_str(&format!("{} = \"{}\"\n", dep, version));
    }
    fs::write(root.join("dgm.toml"), text).unwrap();
}

fn run_dgm(dir: &PathBuf, home: &PathBuf, registry: Option<&str>, args: &[&str]) -> Output {
    let bin = env!("CARGO_BIN_EXE_dgm");
    let mut command = Command::new(bin);
    command.current_dir(dir).env("HOME", home);
    if let Some(registry) = registry {
        command.env("DGM_REGISTRY_URL", registry);
    }
    command.args(args).output().unwrap()
}

fn spawn_dgm(
    dir: &PathBuf,
    home: &PathBuf,
    args: &[&str],
    envs: &[(&str, String)],
) -> Child {
    let bin = env!("CARGO_BIN_EXE_dgm");
    let mut command = Command::new(bin);
    command
        .current_dir(dir)
        .env("HOME", home)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    command.spawn().unwrap()
}

fn collect_child_output(child: &mut Child, status: std::process::ExitStatus) -> Output {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_end(&mut stdout).unwrap();
    }
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_end(&mut stderr).unwrap();
    }
    Output { status, stdout, stderr }
}

fn wait_for_child_output(child: &mut Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return collect_child_output(child, status);
        }
        if Instant::now() >= deadline {
            child.kill().ok();
            let status = child.wait().unwrap();
            return collect_child_output(child, status);
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn concurrent_json_gets(url: &str, token: &str, count: usize) -> Vec<(u16, Value)> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..count {
            let client = client.clone();
            let url = url.to_string();
            let token = token.to_string();
            set.spawn(async move {
                match client
                    .get(&url)
                    .header("authorization", format!("Bearer {}", token))
                    .send()
                    .await
                {
                    Ok(response) => {
                        let status = response.status().as_u16();
                        let body = response.text().await.unwrap_or_else(|err| json!({"error": err.to_string()}).to_string());
                        let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
                        (status, parsed)
                    }
                    Err(err) => (599, json!({"error": err.to_string()})),
                }
            });
        }
        let mut out = Vec::with_capacity(count);
        while let Some(result) = set.join_next().await {
            out.push(result.unwrap());
        }
        out
    })
}

fn concurrent_json_posts(url: &str, token: &str, count: usize) -> Vec<(u16, Value)> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        let mut set = tokio::task::JoinSet::new();
        for index in 0..count {
            let client = client.clone();
            let url = url.to_string();
            let token = token.to_string();
            set.spawn(async move {
                let body = json!({
                    "name": format!("user-{}", index),
                    "email": format!("user-{}@example.com", index),
                });
                match client
                    .post(&url)
                    .header("authorization", format!("Bearer {}", token))
                    .header("content-type", "application/json")
                    .body(body.to_string())
                    .send()
                    .await
                {
                    Ok(response) => {
                        let status = response.status().as_u16();
                        let body = response.text().await.unwrap_or_else(|err| json!({"error": err.to_string()}).to_string());
                        let parsed = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!({"raw": body}));
                        (status, parsed)
                    }
                    Err(err) => (599, json!({"error": err.to_string()})),
                }
            });
        }
        let mut out = Vec::with_capacity(count);
        while let Some(result) = set.join_next().await {
            out.push(result.unwrap());
        }
        out
    })
}

fn assert_ok(output: &Output) {
    assert!(
        output.status.success(),
        "status: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

const TEST_BUNDLE_HEADER_SIZE_V1: usize = 60;
const TEST_BUNDLE_HEADER_SIZE_V2: usize = 68;

fn bundle_parts(path: &PathBuf) -> (u32, Vec<u8>, Value, Vec<u8>, Vec<u8>) {
    let bytes = fs::read(path).unwrap();
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let (header_size, manifest_len, index_len, modules_len, checksum_range) = match version {
        1 => (
            TEST_BUNDLE_HEADER_SIZE_V1,
            u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize,
            0usize,
            u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize,
            28..60,
        ),
        2 => (
            TEST_BUNDLE_HEADER_SIZE_V2,
            u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize,
            u64::from_le_bytes(bytes[20..28].try_into().unwrap()) as usize,
            u64::from_le_bytes(bytes[28..36].try_into().unwrap()) as usize,
            36..68,
        ),
        _ => panic!("unsupported test bundle version {}", version),
    };
    let manifest_start = header_size;
    let manifest_end = manifest_start + manifest_len;
    let index_end = manifest_end + index_len;
    let modules_end = index_end + modules_len;
    let mut checksum = vec![0u8; 32];
    checksum.copy_from_slice(&bytes[checksum_range]);
    let manifest: Value = serde_json::from_slice(&bytes[manifest_start..manifest_end]).unwrap();
    let index = bytes[manifest_end..index_end].to_vec();
    let modules = bytes[index_end..modules_end].to_vec();
    (version, checksum, manifest, index, modules)
}

fn write_bundle_parts(path: &PathBuf, version: u32, manifest: &Value, index: &[u8], modules: &[u8]) {
    let manifest_bytes = serde_json::to_vec(manifest).unwrap();
    let (header_size, checksum) = match version {
        1 => {
            let mut hasher = Sha256::new();
            hasher.update(b"DGMBNDL1");
            hasher.update(1u32.to_le_bytes());
            hasher.update((manifest_bytes.len() as u64).to_le_bytes());
            hasher.update((modules.len() as u64).to_le_bytes());
            hasher.update(&manifest_bytes);
            hasher.update(modules);
            (TEST_BUNDLE_HEADER_SIZE_V1, hasher.finalize().to_vec())
        }
        2 => {
            let mut hasher = Sha256::new();
            hasher.update(b"DGMBNDL1");
            hasher.update(2u32.to_le_bytes());
            hasher.update((manifest_bytes.len() as u64).to_le_bytes());
            hasher.update((index.len() as u64).to_le_bytes());
            hasher.update((modules.len() as u64).to_le_bytes());
            hasher.update(&manifest_bytes);
            hasher.update(index);
            hasher.update(modules);
            (TEST_BUNDLE_HEADER_SIZE_V2, hasher.finalize().to_vec())
        }
        _ => panic!("unsupported test bundle version {}", version),
    };
    let mut bytes = Vec::with_capacity(header_size + manifest_bytes.len() + index.len() + modules.len());
    bytes.extend_from_slice(b"DGMBNDL1");
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&(manifest_bytes.len() as u64).to_le_bytes());
    if version == 1 {
        bytes.extend_from_slice(&(modules.len() as u64).to_le_bytes());
    } else {
        bytes.extend_from_slice(&(index.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(modules.len() as u64).to_le_bytes());
    }
    bytes.extend_from_slice(&checksum);
    bytes.extend_from_slice(&manifest_bytes);
    if version != 1 {
        bytes.extend_from_slice(index);
    }
    bytes.extend_from_slice(modules);
    fs::write(path, bytes).unwrap();
}

fn v2_modules_to_v1_blob(index: &[u8], modules: &[u8]) -> Vec<u8> {
    let index_json: Value = serde_json::from_slice(index).unwrap();
    let module_values = index_json["modules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            let offset = entry["offset"].as_u64().unwrap() as usize;
            let len = entry["len"].as_u64().unwrap() as usize;
            serde_json::from_slice::<Value>(&modules[offset..offset + len]).unwrap()
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&json!({ "modules": module_values })).unwrap()
}

#[test]
fn aliases_and_multiline_strings_work() {
    let source = r#"
cls Animal {
    def speak() { retrun "animal" }
}

cls Dog ext Animal {
    def bark() {
        retrun """woof
dog"""
    }
}

let label = "ok"
let picked = 0
mtch 2 {
    2 => picked = 2
    _ => picked = -1
}

try {
    thro "boom"
} ctch(err) {
    assert_eq(err, "boom")
}

let make = lam(x) => f"{label}:{x}"
assert_eq(make(5), "ok:5")
let d = new Dog()
assert_eq(d.speak(), "animal")
assert_eq(d.bark(), "woof\ndog")
assert_eq(picked, 2)
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn relative_imports_cache_and_module_classes_work() {
    let dir = temp_dir("imports");
    let lib = dir.join("lib.dgm");
    let main = dir.join("main.dgm");
    fs::write(&lib, r#"
let value = 41
def add_one(x) { retrun x + 1 }
cls Box {
    def init(v) { ths.v = v }
}
"#).unwrap();
    fs::write(&main, r#"
imprt "./lib.dgm"
imprt "./lib.dgm"
assert_eq(lib.add_one(lib.value), 42)
let box = new lib.Box(7)
assert_eq(box.v, 7)
"#).unwrap();

    let source = fs::read_to_string(&main).unwrap();
    run_source_with_path(&source, Some(main)).unwrap();
}

#[test]
fn cached_stdlib_imports_rebind_in_new_scopes() {
    let source = r#"
def load_math() {
    imprt math
    assert_eq(math.sqrt(9), 3)
}

load_math()
imprt math
assert_eq(math.sqrt(16), 4)
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn hof_constructor_and_class_definition_scope_are_stable() {
    let source = r#"
let outside = "global"

cls Maker {
    def init(v) { ths.v = v }
    def read() { retrun outside }
}

def build() {
    let outside = "local"
    let items = map([1, 2], lam(x) => new Maker(x))
    assert_eq(items[0].read(), "global")
    assert_eq(items[1].v, 2)
}

build()
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn strutils_and_collections_modules_work() {
    let source = r#"
imprt strutils
imprt collections

let pieces = strutils.split("a,b,c", ",")
assert_eq(strutils.join(pieces, "-"), "a-b-c")

let s = collections.new_set()
collections.set_add(s, 1)
collections.set_add(s, 2)
collections.set_add(s, 2)
assert(collections.set_has(s, 1))
assert_eq(len(collections.set_values(s)), 2)

let mapped = collections.map([1, 2, 3], lam(x) => x * 2)
assert_eq(mapped[2], 6)
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn http_headers_json_and_first_class_env_work() {
    let key_name = "DGM_HTTP_TEST_KEY";
    std::env::set_var(key_name, "secret-token");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).unwrap();
        let req = String::from_utf8_lossy(&buf[..n]).to_string();
        assert!(req.contains("GET /api?mode=test HTTP/1.1"));
        assert!(req.to_ascii_lowercase().contains("x-api-key: secret-token"));
        let body = r#"{"auth":true,"mode":"test"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
    });

    let source = format!(r#"
imprt http
assert_eq(env.{key_name}, "secret-token")
assert_eq(args[0], argv[0])
let res = http.get("http://{addr}/api", {{
    "headers": {{"x-api-key": env.{key_name}}},
    "query": {{"mode": "test"}},
    "timeout_ms": 2000
}})
assert_eq(res.status, 200)
assert(res.ok)
assert_eq(res.headers.content_type, "application/json")
assert(res.json.auth)
assert_eq(res.json.mode, "test")
"#);

    run_source_with_path(&source, None).unwrap();
    handle.join().unwrap();
}

#[test]
fn path_and_config_modules_work() {
    let dir = temp_dir("config");
    let env_file = dir.join("app.env");
    let json_file = dir.join("app.json");
    fs::write(&env_file, "API_KEY=abc123\nPORT=8080\n").unwrap();
    fs::write(&json_file, r#"{"name":"dgm","enabled":true}"#).unwrap();

    let source = format!(r#"
imprt path
imprt config

let joined = path.join("{dir}", "nested", "file.txt")
assert(path.is_abs(joined))
assert_eq(path.basename(joined), "file.txt")
assert_eq(path.stem(joined), "file")
assert_eq(path.ext(joined), "txt")
assert_eq(path.dirname(joined), path.join("{dir}", "nested"))

let envcfg = config.load_env("{env_file}")
let jsoncfg = config.load_json("{json_file}")
let merged = config.merge(envcfg, jsoncfg)
assert_eq(config.require(merged, "API_KEY"), "abc123")
assert_eq(config.require(merged, "name"), "dgm")
assert(jsoncfg.enabled)
"#,
        dir = dir.to_string_lossy(),
        env_file = env_file.to_string_lossy(),
        json_file = json_file.to_string_lossy(),
    );

    run_source_with_path(&source, None).unwrap();
}

#[test]
fn http_rejects_header_injection() {
    let source = r#"
imprt http
http.get("http://127.0.0.1:1", {
    "headers": {
        "Authorization": "Bearer ok\nhack"
    }
})
"#;

    let err = run_source(source).err().unwrap();
    assert!(format!("{}", err).contains("invalid header"));
}

#[test]
fn http_limits_response_body_size() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 2048];
        let _ = stream.read(&mut buf).unwrap();
        let body = "x".repeat(128);
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
    });

    let source = format!(r#"
imprt http
http.get("http://{addr}/", {{
    "max_body_size": 32
}})
"#);

    let err = run_source(&source).err().unwrap();
    assert!(format!("{}", err).contains("response exceeds 32 bytes"));
    handle.join().unwrap();
}

#[test]
fn json_parse_rejects_excessive_nesting() {
    let nested = format!("{}0{}", "[".repeat(80), "]".repeat(80));
    let source = format!("imprt json\njson.parse({nested:?})\n");

    let err = run_source(&source).err().unwrap();
    assert!(format!("{}", err).contains("nesting exceeds"));
}

#[test]
fn path_safe_join_blocks_traversal() {
    let dir = temp_dir("safe-join");
    let source = format!(r#"
imprt path
assert(path.within("{dir}", "nested/file.txt"))
path.safe_join("{dir}", "../etc/passwd")
"#, dir = dir.to_string_lossy());

    let err = run_source(&source).err().unwrap();
    assert!(format!("{}", err).contains("path traversal"));
}

#[test]
fn io_read_file_rejects_large_files() {
    let dir = temp_dir("io-limit");
    let file = dir.join("large.txt");
    fs::write(&file, "a".repeat(10 * 1024 * 1024 + 1)).unwrap();

    let source = format!(r#"
imprt io
io.read_file("{file}")
"#, file = file.to_string_lossy());

    let err = run_source(&source).err().unwrap();
    assert!(format!("{}", err).contains("file exceeds"));
}

#[test]
fn runtime_instruction_budget_stops_infinite_loop() {
    let source = r#"
imprt runtime
runtime.set_max_steps(200)
whl tru {
}
"#;

    let err = run_source(source).err().unwrap();
    assert!(format!("{}", err).contains("execution limit exceeded"));
}

#[test]
fn runtime_call_depth_limit_stops_recursion() {
    let source = r#"
imprt runtime
runtime.set_max_steps(100000)
runtime.set_max_call_depth(20)
def boom(n) {
    retrun boom(n + 1)
}
boom(0)
"#;

    let err = run_source(source).err().unwrap();
    assert!(format!("{}", err).contains("max call depth exceeded"));
}

#[test]
fn runtime_memory_limit_stops_heap_growth() {
    let source = r#"
imprt runtime
runtime.set_max_steps(5000)
runtime.set_max_heap_bytes(1024)
let items = []
whl tru {
    push(items, "abcdefghij")
}
"#;

    let err = run_source(source).err().unwrap();
    assert!(format!("{}", err).contains("memory limit exceeded"));
}

#[test]
fn runtime_profile_reports_functions_modules_and_allocations() {
    let source = r#"
imprt runtime
def foo(n) {
    retrun n + 1
}
foo(1)
foo(2)
let items = []
push(items, "abc")
let p = runtime.profile()
assert_eq(p.function_calls["foo"], 2)
assert_eq(type(p.function_time["foo"]), "int")
assert_eq(type(p.module_time["<main>"]), "int")
assert(p.total_time > 0)
assert(p.alloc_count > 0)
assert(p.alloc_bytes > 0)
"#;

    run_source(source).unwrap();
}

#[test]
fn runtime_profile_reports_allocation_hotspots_by_label() {
    let source = r#"
imprt json
imprt runtime
let value = json.parse("{\"users\":[{\"name\":\"minh\",\"email\":\"minh@example.com\"}]}")
let pretty = json.pretty(value)
assert(len(pretty) > 0)
let p = runtime.profile()
assert(p.alloc_by_label["json.parse"].count > 0)
assert(p.alloc_by_label["json.parse"].bytes > 0)
assert(p.alloc_by_label["json.pretty"].count > 0)
assert(p.alloc_by_label["json.pretty"].bytes > 0)
"#;

    run_source(source).unwrap();
}

#[test]
fn json_read_file_cached_behaves_like_lazy_list_and_map() {
    let dir = temp_dir("json-read-file-cached");
    let file = dir.join("users.json");
    fs::write(&file, r#"[{"id":"1","name":"Minh"},{"id":"2","name":"Lan"}]"#).unwrap();
    let path = file.to_string_lossy().replace('\\', "\\\\");
    let source = format!(
        r#"
imprt json
let users = json.read_file_cached("{path}")
assert_eq(type(users), "list")
assert_eq(len(users), 2)
assert_eq(type(users[0]), "map")
assert_eq(users[0].id, "1")
assert_eq(users[1]["name"], "Lan")
assert_eq(has_key(users[0], "name"), tru)
assert_eq("id" in users[0], tru)
let names = []
fr user in users {{
    push(names, user.name)
}}
assert_eq(join(names, ","), "Minh,Lan")
let copy = json.materialize(users)
push(copy, {{"id": "3", "name": "Bao"}})
assert_eq(len(copy), 3)
assert_eq(len(users), 2)
"#
    );

    run_source(&source).unwrap();
}

#[test]
fn json_invalidate_file_cache_refreshes_cached_file_content() {
    let dir = temp_dir("json-invalidate-file-cache");
    let file = dir.join("users.json");
    fs::write(&file, r#"[{"id":"1"}]"#).unwrap();
    let path = file.to_string_lossy().replace('\\', "\\\\");
    let source = format!(
        r#"
imprt io
imprt json
let first = json.read_file_cached("{path}")
assert_eq(first[0].id, "1")
io.write_file("{path}", "[{{\"id\":\"2\"}}]")
json.invalidate_file_cache("{path}")
let second = json.read_file_cached("{path}")
assert_eq(second[0].id, "2")
"#
    );

    run_source(&source).unwrap();
}

#[test]
fn json_read_file_text_cached_returns_raw_json_text() {
    let dir = temp_dir("json-read-file-text-cached");
    let file = dir.join("users.json");
    fs::write(&file, r#"[{"id":"1","name":"Minh"}]"#).unwrap();
    let path = file.to_string_lossy().replace('\\', "\\\\");
    let source = format!(
        r#"
imprt json
let text = json.read_file_text_cached("{path}")
assert(contains(text, "\"id\":\"1\""))
assert(contains(text, "\"name\":\"Minh\""))
"#
    );

    run_source(&source).unwrap();
}

#[test]
fn json_read_file_raw_cached_returns_raw_json_without_copying_to_str() {
    let dir = temp_dir("json-read-file-raw-cached");
    let file = dir.join("users.json");
    fs::write(&file, r#"[{"id":"1","name":"Minh"}]"#).unwrap();
    let path = file.to_string_lossy().replace('\\', "\\\\");
    let source = format!(
        r#"
imprt json
let raw = json.read_file_raw_cached("{path}")
assert_eq(type(raw), "raw_json")
assert(starts_with(raw, "["))
assert(contains(raw, "\"name\":\"Minh\""))
assert_eq(json.stringify(raw), "[{{\"id\":\"1\",\"name\":\"Minh\"}}]")
"#
    );

    run_source(&source).unwrap();
}

#[test]
fn json_raw_wraps_valid_json_without_restringify() {
    let source = r#"
imprt json
let raw = json.raw("{\"ok\":true,\"items\":[1,2]}")
assert_eq(type(raw), "raw_json")
assert(contains(raw, "\"items\""))
assert_eq(json.stringify(raw), "{\"ok\":true,\"items\":[1,2]}")
let pretty = json.pretty(raw)
assert(contains(pretty, "\n"))
"#;

    run_source(source).unwrap();
}

#[test]
fn json_raw_trusted_and_parts_build_raw_json_without_parse_roundtrip() {
    let source = r#"
imprt json
let inner = json.raw_trusted("[1,2,3]")
let outer = json.raw_parts("{\"ok\":true,\"items\":", inner, "}")
assert_eq(type(inner), "raw_json")
assert_eq(type(outer), "raw_json")
assert(starts_with(outer, "{\"ok\":true"))
assert_eq(json.stringify(outer), "{\"ok\":true,\"items\":[1,2,3]}")
"#;

    run_source(source).unwrap();
}

#[test]
fn runtime_profile_reports_vm_metrics() {
    let source = r#"
imprt runtime
def add(a, b) {
    let total = a + b
    retrun total
}
assert_eq(add(2, 3), 5)
let p = runtime.profile()
assert(p.vm_instruction_count > 0)
assert(p.vm_frame_count > 0)
assert(p.vm_time >= 0)
assert(p.vm_alloc_bytes >= 0)
"#;

    run_source(source).unwrap();
}

#[test]
fn runtime_profile_reports_io_metrics() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 2048];
        let _ = stream.read(&mut buf).unwrap();
        let body = "ok";
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
    });

    let source = format!(
        r#"
imprt http
imprt runtime
let res = http.get("http://{addr}/")
assert_eq(res.status, 200)
let p = runtime.profile()
assert(p.io_call_count > 0)
assert(p.io_time >= 0)
"#
    );

    run_source(&source).unwrap();
    handle.join().unwrap();
}

#[test]
fn config_validate_coerces_defaults_and_optionals() {
    let source = r#"
imprt config
let raw = {
    "PORT": "8080",
    "API_KEY": "abc123",
    "DEBUG": "tru"
}
let cfg = config.validate(raw, {
    "PORT": config.default(config.int, 3000),
    "API_KEY": config.string,
    "DEBUG": config.optional(config.bool),
    "HOST": config.default(config.string, "127.0.0.1")
})
assert_eq(cfg.PORT, 8080)
assert_eq(cfg.API_KEY, "abc123")
assert(cfg.DEBUG)
assert_eq(cfg.HOST, "127.0.0.1")
"#;

    run_source(source).unwrap();
}

#[test]
fn config_validate_rejects_wrong_types() {
    let source = r#"
imprt config
config.validate({
    "PORT": "abc"
}, {
    "PORT": "int"
})
"#;

    let err = run_source(source).err().unwrap();
    assert!(format!("{}", err).contains("must be int"));
}

#[test]
fn http_server_callback_supports_fn_alias_and_request_parsing() {
    let port = free_port();
    let source = format!(r#"
imprt http
http.serve({port}, fn(req) {{
    assert_eq(type(req), "map")
    assert_eq(type(req.headers), "map")
    assert_eq(has_key(req, "json"), tru)
    assert_eq(has_key(req.headers, "content_type"), tru)
    assert_eq(contains(keys(req), "headers"), tru)
    assert_eq(req.method, "POST")
    assert_eq(req.path, "/api")
    assert_eq(req.query.mode, "test")
    assert_eq(req.headers.content_type, "application/json")
    assert_eq(req.headers["content_type"], "application/json")
    assert_eq(req.headers.x_api_key, "secret")
    let data = req.json()
    assert_eq(data.name, "DGM")
    retrun {{
        "status": 201,
        "headers": {{
            "x-server": "dgm"
        }},
        "body": "ok"
    }}
}}, {{
    "max_requests": 1,
    "timeout_ms": 100
}})
"#);

    let handle = thread::spawn(move || {
        run_source(&source).unwrap();
    });
    let response = send_with_retry(|| {
        ureq::post(&format!("http://127.0.0.1:{port}/api?mode=test"))
            .set("content-type", "application/json")
            .set("x-api-key", "secret")
            .send_string(r#"{"name":"DGM"}"#)
    });

    assert_eq!(response.status(), 201);
    assert_eq!(response.header("x-server"), Some("dgm"));
    assert_eq!(response.into_string().unwrap(), "ok");
    handle.join().unwrap();
}

#[test]
fn http_router_matches_exact_param_and_wildcard_routes() {
    let port = free_port();
    let source = format!(r#"
imprt http
let app = http.new()

app.get("/users", fn(req) {{
    retrun {{
        "status": 200,
        "body": "list"
    }}
}})

app.get("/user/:id", fn(req) {{
    retrun {{
        "status": 200,
        "body": req.params.id
    }}
}})

app.get("/static/*", fn(req) {{
    retrun {{
        "status": 200,
        "body": req.params.wildcard
    }}
}})

app.post("/login", fn(req) {{
    let data = req.json()
    retrun {{
        "status": 200,
        "body": {{
            "user": data.user,
            "ok": tru
        }}
    }}
}})

app.listen({port}, {{
    "max_requests": 4,
    "timeout_ms": 100
}})
"#);

    let handle = thread::spawn(move || {
        run_source(&source).unwrap();
    });

    let users = send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{port}/users")).call());
    assert_eq!(users.status(), 200);
    assert_eq!(users.into_string().unwrap(), "list");

    let user = send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{port}/user/minh%20nguyen")).call());
    assert_eq!(user.status(), 200);
    assert_eq!(user.into_string().unwrap(), "minh nguyen");

    let static_file = send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{port}/static/css/app.css")).call());
    assert_eq!(static_file.status(), 200);
    assert_eq!(static_file.into_string().unwrap(), "css/app.css");

    let login = send_with_retry(|| {
        ureq::post(&format!("http://127.0.0.1:{port}/login"))
            .set("content-type", "application/json")
            .send_string(r#"{"user":"minh"}"#)
    });
    assert_eq!(login.status(), 200);
    assert_eq!(login.header("content-type"), Some("application/json"));
    let body: serde_json::Value = serde_json::from_str(&login.into_string().unwrap()).unwrap();
    assert_eq!(body["user"], "minh");
    assert_eq!(body["ok"], true);

    handle.join().unwrap();
}

#[test]
fn http_server_rejects_oversized_request_bodies() {
    let port = free_port();
    let source = format!(r#"
imprt http
http.serve({port}, fn(req) {{
    retrun {{
        "status": 200,
        "body": "ok"
    }}
}}, {{
    "max_requests": 1,
    "timeout_ms": 100,
    "max_body_size": 8
}})
"#);

    let handle = thread::spawn(move || {
        run_source(&source).unwrap();
    });
    let response = send_with_retry(|| {
        ureq::post(&format!("http://127.0.0.1:{port}/upload"))
            .set("content-type", "text/plain")
            .send_string("0123456789abcdef")
    });

    assert_eq!(response.status(), 413);
    assert!(response.into_string().unwrap().contains("request body exceeds 8 bytes"));
    handle.join().unwrap();
}

#[test]
fn spawn_and_channel_support_concurrent_tasks() {
    let source = r#"
imprt thread
let ch = channel()
let task = spawn(fn() {
    thread.sleep(80)
    ch.send("slow")
    retrun 11
})
spawn(fn() {
    thread.sleep(10)
    ch.send("fast")
})
assert_eq(ch.recv(), "fast")
assert_eq(ch.recv(), "slow")
assert_eq(task.join(), 11)
"#;

    run_source(source).unwrap();
}

#[test]
fn spawn_propagates_errors_through_join() {
    let source = r#"
let task = spawn(fn() {
    panic("boom")
})
task.join()
"#;

    let err = run_source(source).err().unwrap();
    assert!(format!("{}", err).contains("task failed: boom"));
}

#[test]
fn channel_try_recv_and_recv_timeout_do_not_block_forever() {
    let source = r#"
let ch = channel()
assert_eq(ch.try_recv(), nul)
assert_eq(ch.recv_timeout(10), nul)
ch.send(42)
assert_eq(ch.try_recv(), 42)
"#;

    run_source(source).unwrap();
}

#[test]
fn http_server_handles_requests_concurrently() {
    let port = free_port();
    let source = format!(r#"
imprt http
imprt thread

http.serve({port}, fn(req) {{
    if req.path == "/slow" {{
        thread.sleep(250)
        retrun {{
            "status": 200,
            "body": "slow"
        }}
    }}
    retrun {{
        "status": 200,
        "body": "fast"
    }}
}}, {{
    "max_requests": 2,
    "timeout_ms": 100
}})
"#);

    let server = thread::spawn(move || {
        run_source(&source).unwrap();
    });

    let slow_port = port;
    let slow = thread::spawn(move || {
        send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{slow_port}/slow")).call())
            .into_string()
            .unwrap()
    });

    thread::sleep(Duration::from_millis(100));
    let fast_start = Instant::now();
    let fast = send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{port}/fast")).call());
    let fast_elapsed = fast_start.elapsed();

    assert_eq!(fast.status(), 200);
    assert_eq!(fast.into_string().unwrap(), "fast");
    assert!(fast_elapsed < Duration::from_millis(200));
    assert_eq!(slow.join().unwrap(), "slow");
    server.join().unwrap();
}

#[test]
fn http_request_context_is_isolated_between_requests() {
    let port = free_port();
    let source = format!(r#"
imprt http

let counter = 0

http.serve({port}, fn(req) {{
    counter = counter + 1
    retrun {{
        "status": 200,
        "body": str(counter)
    }}
}}, {{
    "max_requests": 2,
    "timeout_ms": 100
}})
"#);

    let server = thread::spawn(move || {
        run_source(&source).unwrap();
    });

    let first = send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{port}/one")).call());
    let second = send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{port}/two")).call());

    assert_eq!(first.into_string().unwrap(), "1");
    assert_eq!(second.into_string().unwrap(), "1");
    server.join().unwrap();
}

#[test]
fn http_server_respects_max_threads_limit() {
    let port = free_port();
    let source = format!(r#"
imprt http
imprt thread

http.serve({port}, fn(req) {{
    thread.sleep(220)
    retrun {{
        "status": 200,
        "body": req.path
    }}
}}, {{
    "max_requests": 2,
    "timeout_ms": 100,
    "max_threads": 1
}})
"#);

    let server = thread::spawn(move || {
        run_source(&source).unwrap();
    });

    let slow_port = port;
    let slow = thread::spawn(move || {
        send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{slow_port}/one")).call())
            .into_string()
            .unwrap()
    });

    thread::sleep(Duration::from_millis(40));
    let fast_start = Instant::now();
    let fast = send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{port}/two")).call());
    let fast_elapsed = fast_start.elapsed();

    assert_eq!(fast.status(), 200);
    assert_eq!(fast.into_string().unwrap(), "/two");
    assert!(fast_elapsed >= Duration::from_millis(180));
    assert_eq!(slow.join().unwrap(), "/one");
    server.join().unwrap();
}

#[test]
fn await_http_get_uses_async_runtime() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        let _ = stream.read(&mut buf).unwrap();
        let body = r#"{"ok":true,"name":"dgm"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).unwrap();
    });

    let source = format!(r#"
imprt http
let res = awt http.get("http://{addr}/ping")
assert_eq(res.status, 200)
assert(res.ok)
assert(res.json.ok)
assert_eq(res.json.name, "dgm")
"#);

    run_source(&source).unwrap();
    handle.join().unwrap();
}

#[test]
fn await_task_handle_returns_task_result() {
    let source = r#"
imprt thread
let task = spawn(fn() {
    thread.sleep(20)
    retrun 99
})
assert_eq(awt task, 99)
"#;

    run_source(source).unwrap();
}

#[test]
fn task_cancel_and_runtime_snapshots_work() {
    let home = temp_dir("cancel-home");
    let project = temp_dir("cancel-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        r#"
imprt runtime
imprt thread

runtime.set_max_threads(1)
let ch = channel()
let blocker = spawn(fn() {
    thread.sleep(80)
    retrun 1
})
whl runtime.async_stats().running_jobs == 0 {
    thread.sleep(1)
}
let task = spawn(fn() {
    ch.send("ran")
    retrun 2
})
assert(task.cancel())
assert(task.done())
let stats = runtime.async_stats()
assert_eq(stats.max_threads, 1)
let state = task.state()
assert_eq(state.kind, "task")
assert_eq(state.status, "cancelled")
assert_eq(ch.recv_timeout(20), nul)
let tasks = runtime.tasks()
assert(len(tasks) >= 1)
try {
    task.join()
    assert(fals)
} catch err {
    assert(contains(str(err), "task failed: task cancelled"))
}
assert_eq(awt blocker, 1)
"#,
    )
    .unwrap();

    let output = run_dgm(&project, &home, None, &["run"]);
    assert_ok(&output);
}

#[test]
fn await_net_connect_and_recv_work() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(b"pong").unwrap();
    });

    let source = format!(
        r#"
imprt net
let sock = awt net.connect("127.0.0.1", {port})
let data = awt net.recv(sock, 4)
assert_eq(data, "pong")
net.close(sock)
"#
    );

    run_source(&source).unwrap();
    handle.join().unwrap();
}

#[test]
fn runtime_wall_clock_limit_stops_busy_loop() {
    let source = r#"
imprt runtime
runtime.set_max_wall_time_ms(5)
whl tru {
}
"#;

    let err = run_source(source).err().unwrap();
    assert!(format!("{}", err).contains("wall-clock limit exceeded"));
}

#[test]
fn deep_await_chain_remains_stable() {
    let source = r#"
imprt runtime
runtime.set_max_threads(4)

let task = spawn(fn() {
    retrun 0
})

let i = 0
whl i < 250 {
    let prev = task
    task = spawn(fn() {
        retrun awt prev + 1
    })
    i = i + 1
}

assert_eq(awt task, 250)
"#;

    run_source(source).unwrap();
}

#[test]
fn net_open_socket_limit_is_enforced() {
    let listener_one = TcpListener::bind("127.0.0.1:0").unwrap();
    let port_one = listener_one.local_addr().unwrap().port();
    let handle_one = thread::spawn(move || {
        let (_stream, _) = listener_one.accept().unwrap();
        thread::sleep(Duration::from_millis(150));
    });

    let listener_two = TcpListener::bind("127.0.0.1:0").unwrap();
    let port_two = listener_two.local_addr().unwrap().port();
    let handle_two = thread::spawn(move || {
        let (_stream, _) = listener_two.accept().unwrap();
        thread::sleep(Duration::from_millis(150));
    });

    let source = format!(r#"
imprt net
imprt runtime

runtime.set_max_open_sockets(1)
let first = net.connect("127.0.0.1", {port_one})
try {{
    net.connect("127.0.0.1", {port_two})
    assert(fals)
}} catch err {{
    assert(contains(str(err), "open socket limit exceeded"))
}}
net.close(first)
"#);

    run_source(&source).unwrap();
    handle_one.join().unwrap();
    handle_two.join().unwrap();
}

#[test]
fn http_server_times_out_slowloris_clients() {
    let port = free_port();
    let source = format!(r#"
imprt http

http.serve({port}, fn(req) {{
    retrun {{
        "status": 200,
        "body": "ok"
    }}
}}, {{
    "max_requests": 1,
    "read_timeout_ms": 50,
    "write_timeout_ms": 50
}})
"#);

    let server = thread::spawn(move || {
        run_source(&source).unwrap();
    });

    let mut stream = None;
    for _ in 0..40 {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(conn) => {
                stream = Some(conn);
                break;
            }
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(err) => panic!("connect failed: {}", err),
        }
    }
    let mut stream = stream.expect("server did not become ready");
    stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n")
        .unwrap();
    thread::sleep(Duration::from_millis(120));

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    assert!(response.contains("504"));
    assert!(response.contains("request timed out"));
    server.join().unwrap();
}

#[test]
fn http_server_rejects_connections_over_limit() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let port = free_port();
    let source = format!(r#"
imprt http

http.serve({port}, fn(req) {{
    retrun {{
        "status": 200,
        "body": req.path
    }}
}}, {{
    "max_requests": 2,
    "timeout_ms": 100,
    "max_threads": 2,
    "max_connections": 1
}})
"#);

    let server = thread::spawn(move || {
        run_source(&source).unwrap();
    });

    let mut held = None;
    for _ in 0..40 {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => {
                held = Some(stream);
                break;
            }
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(err) => panic!("connect failed: {}", err),
        }
    }
    let mut held = held.expect("server did not become ready");
    held.write_all(b"GET /one HTTP/1.1\r\nHost: localhost\r\n").unwrap();
    thread::sleep(Duration::from_millis(50));

    let busy = send_with_retry(|| ureq::get(&format!("http://127.0.0.1:{port}/two")).call());
    assert_eq!(busy.status(), 503);
    assert_eq!(busy.into_string().unwrap(), "server busy");
    held.write_all(b"\r\n").unwrap();
    let mut response = String::new();
    held.read_to_string(&mut response).unwrap();
    assert!(response.contains("/one"));
    server.join().unwrap();
}

#[test]
fn async_http_retries_with_backoff() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = Arc::clone(&attempts);
    let handle = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = listener.accept().unwrap();
            let current = attempts_clone.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            if current == 0 {
                drop(stream);
                continue;
            }
            let body = "retry-ok";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        }
    });

    let source = format!(r#"
imprt http
let res = awt http.get("http://{addr}/retry", {{
    "retries": 2,
    "retry_backoff_ms": 15,
    "retry_backoff_max_ms": 15
}})
assert_eq(res.status, 200)
assert_eq(res.body, "retry-ok")
"#);

    let started = Instant::now();
    run_source(&source).unwrap();
    let elapsed = started.elapsed();
    handle.join().unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(elapsed >= Duration::from_millis(10));
}

#[test]
fn package_manager_installs_from_registry_and_runs_project() {
    let home = temp_dir("pkg-home");
    let project = temp_dir("pkg-project");

    let utils_pkg = package_with_files(
        "utils",
        "0.3.1",
        r#"
def answer() { retrun 41 }
"#,
        vec![],
    );
    let httpx_pkg = package_with_files(
        "httpx",
        "1.2.0",
        r#"
imprt utils
def answer() { retrun utils.answer() + 1 }
"#,
        vec![],
    );

    let utils_hash = sha256_hex(&utils_pkg);
    let httpx_hash = sha256_hex(&httpx_pkg);
    let utils_meta = format!(
        r#"{{"name":"utils","versions":[{{"version":"0.3.1","url":"__BASE__/archives/utils-0.3.1.tar.gz","sha256":"{utils_hash}","size":{},"dependencies":{{}}}}]}}"#,
        utils_pkg.len()
    );
    let httpx_meta = format!(
        r#"{{"name":"httpx","versions":[{{"version":"1.2.0","url":"__BASE__/archives/httpx-1.2.0.tar.gz","sha256":"{httpx_hash}","size":{},"dependencies":{{"utils":"0.3.1"}}}}]}}"#,
        httpx_pkg.len()
    );

    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/utils.json".to_string(),
                utils_meta.replace("__BASE__", base).into_bytes(),
                "application/json",
            ),
            (
                "/packages/httpx.json".to_string(),
                httpx_meta.replace("__BASE__", base).into_bytes(),
                "application/json",
            ),
            ("/archives/utils-0.3.1.tar.gz".to_string(), utils_pkg, "application/gzip"),
            ("/archives/httpx-1.2.0.tar.gz".to_string(), httpx_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["add", "httpx"]));

    fs::write(
        project.join("main.dgm"),
        r#"
imprt httpx
assert_eq(httpx.answer(), 42)
"#,
    )
    .unwrap();

    let run = run_dgm(&project, &home, Some(&server.base_url), &["run"]);
    assert_ok(&run);

    let lock = fs::read_to_string(project.join("dgm.lock")).unwrap();
    assert!(lock.contains("httpx"));
    assert!(lock.contains("utils"));
    assert!(lock.contains(&httpx_hash));
    assert!(project.join(".dgm/packages/httpx/1.2.0/main.dgm").exists());
    assert!(project.join(".dgm/packages/utils/0.3.1/main.dgm").exists());
}

#[test]
fn package_manager_supports_offline_install_from_cache() {
    let home = temp_dir("pkg-home-offline");
    let online_project = temp_dir("pkg-online");
    let offline_project = temp_dir("pkg-offline");

    let helper_pkg = package_with_files(
        "helper",
        "1.0.0",
        r#"
def value() { retrun 7 }
"#,
        vec![],
    );
    let app_pkg = package_with_files(
        "appdep",
        "2.0.0",
        r#"
imprt helper
def value() { retrun helper.value() * 6 }
"#,
        vec![],
    );
    let helper_hash = sha256_hex(&helper_pkg);
    let app_hash = sha256_hex(&app_pkg);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/helper.json".into(),
                format!(
                    r#"{{"name":"helper","versions":[{{"version":"1.0.0","url":"{}/archives/helper-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    helper_hash,
                    helper_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            (
                "/packages/appdep.json".into(),
                format!(
                    r#"{{"name":"appdep","versions":[{{"version":"2.0.0","url":"{}/archives/appdep-2.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{"helper":"1.0.0"}}}}]}}"#,
                    base,
                    app_hash,
                    app_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/helper-1.0.0.tar.gz".into(), helper_pkg, "application/gzip"),
            ("/archives/appdep-2.0.0.tar.gz".into(), app_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&online_project, &home, Some(&server.base_url), &["init"]));
    assert_ok(&run_dgm(&online_project, &home, Some(&server.base_url), &["add", "appdep@2.0.0"]));

    assert_ok(&run_dgm(&offline_project, &home, None, &["init"]));
    write_project_manifest(&offline_project, "offline_app", &[("appdep", "2.0.0")]);
    let install = run_dgm(&offline_project, &home, None, &["install", "--offline"]);
    assert_ok(&install);

    fs::write(
        offline_project.join("main.dgm"),
        r#"
imprt appdep
assert_eq(appdep.value(), 42)
"#,
    )
    .unwrap();

    let run = run_dgm(&offline_project, &home, None, &["run"]);
    assert_ok(&run);
}

#[test]
fn package_manager_rejects_hash_mismatch_and_circular_dependencies() {
    let home = temp_dir("pkg-home-bad");
    let hash_project = temp_dir("pkg-hash");
    let cycle_project = temp_dir("pkg-cycle");

    let bad_pkg = package_with_files("badpkg", "1.0.0", "def ok() { retrun 1 }\n", vec![]);
    let cycle_a_pkg = package_with_files("cyclea", "1.0.0", "imprt cycleb\n", vec![]);
    let cycle_b_pkg = package_with_files("cycleb", "1.0.0", "imprt cyclea\n", vec![]);
    let cycle_a_hash = sha256_hex(&cycle_a_pkg);
    let cycle_b_hash = sha256_hex(&cycle_b_pkg);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/badpkg.json".into(),
                format!(
                    r#"{{"name":"badpkg","versions":[{{"version":"1.0.0","url":"{}/archives/badpkg-1.0.0.tar.gz","sha256":"deadbeef","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    bad_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            (
                "/packages/cyclea.json".into(),
                format!(
                    r#"{{"name":"cyclea","versions":[{{"version":"1.0.0","url":"{}/archives/cyclea-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{"cycleb":"1.0.0"}}}}]}}"#,
                    base,
                    cycle_a_hash,
                    cycle_a_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            (
                "/packages/cycleb.json".into(),
                format!(
                    r#"{{"name":"cycleb","versions":[{{"version":"1.0.0","url":"{}/archives/cycleb-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{"cyclea":"1.0.0"}}}}]}}"#,
                    base,
                    cycle_b_hash,
                    cycle_b_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/badpkg-1.0.0.tar.gz".into(), bad_pkg, "application/gzip"),
            ("/archives/cyclea-1.0.0.tar.gz".into(), cycle_a_pkg, "application/gzip"),
            ("/archives/cycleb-1.0.0.tar.gz".into(), cycle_b_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&hash_project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&hash_project, "hash_app", &[("badpkg", "1.0.0")]);
    let bad_install = run_dgm(&hash_project, &home, Some(&server.base_url), &["install"]);
    assert!(!bad_install.status.success());
    assert!(String::from_utf8_lossy(&bad_install.stderr).contains("hash mismatch"));

    assert_ok(&run_dgm(&cycle_project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&cycle_project, "cycle_app", &[("cyclea", "1.0.0")]);
    let cycle_install = run_dgm(&cycle_project, &home, Some(&server.base_url), &["install"]);
    assert!(!cycle_install.status.success());
    assert!(String::from_utf8_lossy(&cycle_install.stderr).contains("circular dependency"));
}

#[test]
fn package_manager_rejects_path_traversal_archives() {
    let home = temp_dir("pkg-home-traversal");
    let project = temp_dir("pkg-traversal");

    let evil_pkg = raw_package_archive(vec![
        ("dgm.toml", b"[package]\nname = \"evilpkg\"\nversion = \"1.0.0\"\n".to_vec()),
        ("main.dgm", b"def ok() { retrun 1 }\n".to_vec()),
        ("../../escape.txt", b"bad".to_vec()),
    ]);
    let evil_hash = sha256_hex(&evil_pkg);
    let evil_size = evil_pkg.len();
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/evilpkg.json".into(),
                format!(
                    r#"{{"name":"evilpkg","versions":[{{"version":"1.0.0","url":"{}/archives/evilpkg-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    evil_hash,
                    evil_size
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/evilpkg-1.0.0.tar.gz".into(), evil_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "evil_app", &[("evilpkg", "1.0.0")]);
    let output = run_dgm(&project, &home, Some(&server.base_url), &["install"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unsafe path") || stderr.contains("invalid archive path") || stderr.contains("must not have `..`"),
        "{}",
        stderr
    );
    assert!(!project.join("escape.txt").exists());
}

#[test]
fn package_manager_update_supports_dry_run_and_selective_updates() {
    let home = temp_dir("pkg-home-update");
    let project = temp_dir("pkg-update");

    let httpx_old = package_with_files("httpx", "1.2.0", "def version() { retrun \"1.2.0\" }\n", vec![]);
    let httpx_new = package_with_files("httpx", "1.3.1", "def version() { retrun \"1.3.1\" }\n", vec![]);
    let utils_old = package_with_files("utils", "1.0.0", "def version() { retrun \"1.0.0\" }\n", vec![]);
    let utils_new = package_with_files("utils", "1.1.0", "def version() { retrun \"1.1.0\" }\n", vec![]);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/httpx.json".into(),
                format!(
                    r#"{{"name":"httpx","versions":[{{"version":"1.2.0","url":"{}/archives/httpx-1.2.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}},{{"version":"1.3.1","url":"{}/archives/httpx-1.3.1.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    sha256_hex(&httpx_old),
                    httpx_old.len(),
                    base,
                    sha256_hex(&httpx_new),
                    httpx_new.len()
                )
                .into_bytes(),
                "application/json",
            ),
            (
                "/packages/utils.json".into(),
                format!(
                    r#"{{"name":"utils","versions":[{{"version":"1.0.0","url":"{}/archives/utils-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}},{{"version":"1.1.0","url":"{}/archives/utils-1.1.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    sha256_hex(&utils_old),
                    utils_old.len(),
                    base,
                    sha256_hex(&utils_new),
                    utils_new.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/httpx-1.2.0.tar.gz".into(), httpx_old, "application/gzip"),
            ("/archives/httpx-1.3.1.tar.gz".into(), httpx_new, "application/gzip"),
            ("/archives/utils-1.0.0.tar.gz".into(), utils_old, "application/gzip"),
            ("/archives/utils-1.1.0.tar.gz".into(), utils_new, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "update_app", &[("httpx", "1.2.0"), ("utils", "1.0.0")]);
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));

    write_project_manifest(&project, "update_app", &[("httpx", "^1.2.0"), ("utils", "^1.0.0")]);
    let before_lock = fs::read_to_string(project.join("dgm.lock")).unwrap();

    let dry_run = run_dgm(&project, &home, Some(&server.base_url), &["update", "--dry-run", "httpx"]);
    assert_ok(&dry_run);
    let dry_stdout = String::from_utf8_lossy(&dry_run.stdout);
    assert!(dry_stdout.contains("httpx 1.2.0 -> 1.3.1"));
    assert!(!dry_stdout.contains("utils 1.0.0 -> 1.1.0"));
    assert_eq!(fs::read_to_string(project.join("dgm.lock")).unwrap(), before_lock);

    let apply = run_dgm(&project, &home, Some(&server.base_url), &["update", "httpx"]);
    assert_ok(&apply);
    let lock = fs::read_to_string(project.join("dgm.lock")).unwrap();
    assert!(lock.contains("version = \"1.3.1\""));
    assert!(lock.contains("version = \"1.0.0\""));
    assert!(!lock.contains("version = \"1.1.0\""));
}

#[test]
fn package_manager_update_rolls_back_on_failed_test_gate() {
    let home = temp_dir("pkg-home-rollback");
    let project = temp_dir("pkg-rollback");

    let lib_old = package_with_files("libx", "1.0.0", "def value() { retrun 1 }\n", vec![]);
    let lib_new = package_with_files("libx", "1.1.0", "def value() { retrun 2 }\n", vec![]);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/libx.json".into(),
                format!(
                    r#"{{"name":"libx","versions":[{{"version":"1.0.0","url":"{}/archives/libx-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}},{{"version":"1.1.0","url":"{}/archives/libx-1.1.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    sha256_hex(&lib_old),
                    lib_old.len(),
                    base,
                    sha256_hex(&lib_new),
                    lib_new.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/libx-1.0.0.tar.gz".into(), lib_old, "application/gzip"),
            ("/archives/libx-1.1.0.tar.gz".into(), lib_new, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "rollback_app", &[("libx", "1.0.0")]);
    fs::write(
        project.join("main.dgm"),
        r#"
imprt libx
assert_eq(libx.value(), 1)
"#,
    )
    .unwrap();
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));
    write_project_manifest(&project, "rollback_app", &[("libx", "^1.0.0")]);

    let update = run_dgm(&project, &home, Some(&server.base_url), &["update", "--test"]);
    assert!(!update.status.success());
    let stderr = String::from_utf8_lossy(&update.stderr);
    assert!(stderr.contains("rolled back"));

    let lock = fs::read_to_string(project.join("dgm.lock")).unwrap();
    assert!(lock.contains("version = \"1.0.0\""));
    assert!(!lock.contains("version = \"1.1.0\""));
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["run"]));
}

#[test]
fn package_manager_update_blocks_major_and_doctor_reports_lock_mismatch() {
    let home = temp_dir("pkg-home-major");
    let project = temp_dir("pkg-major");

    let old_pkg = package_with_files("httpx", "1.2.0", "def version() { retrun \"1.2.0\" }\n", vec![]);
    let new_pkg = package_with_files("httpx", "2.0.0", "def version() { retrun \"2.0.0\" }\n", vec![]);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/httpx.json".into(),
                format!(
                    r#"{{"name":"httpx","versions":[{{"version":"1.2.0","url":"{}/archives/httpx-1.2.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}},{{"version":"2.0.0","url":"{}/archives/httpx-2.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    sha256_hex(&old_pkg),
                    old_pkg.len(),
                    base,
                    sha256_hex(&new_pkg),
                    new_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/httpx-1.2.0.tar.gz".into(), old_pkg, "application/gzip"),
            ("/archives/httpx-2.0.0.tar.gz".into(), new_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "major_app", &[("httpx", "1.2.0")]);
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));

    write_project_manifest(&project, "major_app", &[("httpx", "^2.0.0")]);
    let update = run_dgm(&project, &home, Some(&server.base_url), &["update"]);
    assert!(!update.status.success());
    assert!(String::from_utf8_lossy(&update.stderr).contains("major version update blocked"));

    let doctor = run_dgm(&project, &home, Some(&server.base_url), &["doctor"]);
    assert!(!doctor.status.success());
    let stdout = String::from_utf8_lossy(&doctor.stdout);
    assert!(stdout.contains("dgm.lock does not match dgm.toml dependencies"));
}

#[test]
fn package_manager_outdated_and_why_report_dependency_info() {
    let home = temp_dir("pkg-home-info");
    let project = temp_dir("pkg-info");

    let leaf = package_with_files("leaf", "1.0.0", "def v() { retrun \"1.0.0\" }\n", vec![]);
    let service = package_with_files("service", "0.2.0", "imprt leaf\ndef v() { retrun leaf.v() }\n", vec![]);
    let service_new = package_with_files("service", "0.3.0", "imprt leaf\ndef v() { retrun leaf.v() }\n", vec![]);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/leaf.json".into(),
                format!(
                    r#"{{"name":"leaf","versions":[{{"version":"1.0.0","url":"{}/archives/leaf-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    sha256_hex(&leaf),
                    leaf.len()
                )
                .into_bytes(),
                "application/json",
            ),
            (
                "/packages/service.json".into(),
                format!(
                    r#"{{"name":"service","versions":[{{"version":"0.2.0","url":"{}/archives/service-0.2.0.tar.gz","sha256":"{}","size":{},"dependencies":{{"leaf":"1.0.0"}}}},{{"version":"0.3.0","url":"{}/archives/service-0.3.0.tar.gz","sha256":"{}","size":{},"dependencies":{{"leaf":"1.0.0"}}}}]}}"#,
                    base,
                    sha256_hex(&service),
                    service.len(),
                    base,
                    sha256_hex(&service_new),
                    service_new.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/leaf-1.0.0.tar.gz".into(), leaf, "application/gzip"),
            ("/archives/service-0.2.0.tar.gz".into(), service, "application/gzip"),
            ("/archives/service-0.3.0.tar.gz".into(), service_new, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "info_app", &[("service", "0.2.0")]);
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));
    write_project_manifest(&project, "info_app", &[("service", "^0.2.0")]);

    let outdated = run_dgm(&project, &home, Some(&server.base_url), &["outdated"]);
    assert_ok(&outdated);
    let stdout = String::from_utf8_lossy(&outdated.stdout);
    assert!(stdout.contains("service 0.2.0 -> 0.3.0"));

    let why = run_dgm(&project, &home, Some(&server.base_url), &["why", "leaf"]);
    assert_ok(&why);
    assert!(String::from_utf8_lossy(&why.stdout).contains("app -> service -> leaf"));
}

#[test]
fn package_manager_project_test_entry_and_lock_file_work() {
    let home = temp_dir("pkg-home-test-entry");
    let project = temp_dir("pkg-test-entry");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    let manifest = r#"[package]
name = "test_entry_app"
version = "0.1.0"
test = "qa.dgm"
"#;
    fs::write(project.join("dgm.toml"), manifest).unwrap();
    fs::write(project.join("qa.dgm"), "assert_eq(1 + 1, 2)\n").unwrap();

    let test_output = run_dgm(&project, &home, None, &["test"]);
    assert_ok(&test_output);
    assert!(String::from_utf8_lossy(&test_output.stdout).contains("PASS"));

    fs::create_dir_all(project.join(".dgm")).unwrap();
    fs::write(project.join(".dgm/.lock"), "pid=999\n").unwrap();
    let doctor = run_dgm(&project, &home, None, &["doctor"]);
    assert!(!doctor.status.success());
    assert!(String::from_utf8_lossy(&doctor.stderr).contains("another DGM package command"));
}

#[test]
fn package_manager_clean_removes_packages_but_keeps_cache() {
    let home = temp_dir("pkg-home-clean");
    let project = temp_dir("pkg-clean");

    let util_pkg = package_with_files("utilx", "1.0.0", "def v() { retrun 1 }\n", vec![]);
    let util_hash = sha256_hex(&util_pkg);
    let archive_name = format!("utilx-1.0.0-{}.tar.gz", &util_hash[..16]);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/utilx.json".into(),
                format!(
                    r#"{{"name":"utilx","versions":[{{"version":"1.0.0","url":"{}/archives/utilx-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    util_hash,
                    util_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/utilx-1.0.0.tar.gz".into(), util_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "clean_app", &[("utilx", "1.0.0")]);
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));

    fs::create_dir_all(project.join(".dgm/packages_tmp/stale")).unwrap();
    fs::write(project.join(".dgm/packages_tmp/stale/file.tmp"), "x").unwrap();

    let clean = run_dgm(&project, &home, Some(&server.base_url), &["clean"]);
    assert_ok(&clean);
    assert!(!project.join(".dgm/packages").exists());
    assert!(!project.join(".dgm/packages_tmp").exists());
    assert!(home.join(".dgm/cache/archives").join(archive_name).exists());
}

#[test]
fn package_manager_cache_prune_removes_unused_cache_entries() {
    let home = temp_dir("pkg-home-prune");
    let project = temp_dir("pkg-prune");

    let keep_pkg = package_with_files("keepx", "1.0.0", "def v() { retrun 1 }\n", vec![]);
    let keep_hash = sha256_hex(&keep_pkg);
    let keep_archive_name = format!("keepx-1.0.0-{}.tar.gz", &keep_hash[..16]);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/keepx.json".into(),
                format!(
                    r#"{{"name":"keepx","versions":[{{"version":"1.0.0","url":"{}/archives/keepx-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    keep_hash,
                    keep_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/keepx-1.0.0.tar.gz".into(), keep_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "prune_app", &[("keepx", "1.0.0")]);
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));

    let metadata_dir = home.join(".dgm/cache/metadata");
    let archives_dir = home.join(".dgm/cache/archives");
    fs::create_dir_all(&metadata_dir).unwrap();
    fs::create_dir_all(&archives_dir).unwrap();
    let orphan_meta = metadata_dir.join("orphan.json");
    let orphan_archive = archives_dir.join("orphan-1.0.0-deadbeef.tar.gz");
    fs::write(&orphan_meta, "{}").unwrap();
    fs::write(&orphan_archive, "dead").unwrap();

    let prune = run_dgm(&project, &home, Some(&server.base_url), &["cache", "prune"]);
    assert_ok(&prune);
    assert!(!orphan_meta.exists());
    assert!(!orphan_archive.exists());
    assert!(archives_dir.join(keep_archive_name).exists());
    assert!(metadata_dir.join("keepx.json").exists());
}

#[test]
fn package_manager_install_frozen_requires_matching_lock() {
    let home = temp_dir("pkg-home-frozen");
    let project = temp_dir("pkg-frozen");

    let lock_pkg = package_with_files("lockx", "1.0.0", "def v() { retrun 1 }\n", vec![]);
    let lock_hash = sha256_hex(&lock_pkg);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/lockx.json".into(),
                format!(
                    r#"{{"name":"lockx","versions":[{{"version":"1.0.0","url":"{}/archives/lockx-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    lock_hash,
                    lock_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/lockx-1.0.0.tar.gz".into(), lock_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "frozen_app", &[("lockx", "1.0.0")]);
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));

    let frozen_ok = run_dgm(&project, &home, Some(&server.base_url), &["install", "--frozen"]);
    assert_ok(&frozen_ok);

    write_project_manifest(&project, "frozen_app", &[("lockx", "^1.0.0")]);
    let frozen_bad = run_dgm(&project, &home, Some(&server.base_url), &["install", "--frozen"]);
    assert!(!frozen_bad.status.success());
    assert!(String::from_utf8_lossy(&frozen_bad.stderr).contains("frozen install requires"));
}

#[test]
fn build_bundle_runs_without_project_sources_or_packages() {
    let home = temp_dir("build-home");
    let project = temp_dir("build-project");
    let runner = temp_dir("build-runner");

    let util_pkg = package_with_files("utilx", "1.0.0", "def answer() { retrun \"pkg\" }\n", vec![]);
    let util_hash = sha256_hex(&util_pkg);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/utilx.json".into(),
                format!(
                    r#"{{"name":"utilx","versions":[{{"version":"1.0.0","url":"{}/archives/utilx-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    util_hash,
                    util_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/utilx-1.0.0.tar.gz".into(), util_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "bundle_app", &[("utilx", "1.0.0")]);
    fs::write(project.join("lib.dgm"), "def label() { retrun \"local\" }\n").unwrap();
    fs::write(
        project.join("main.dgm"),
        "imprt utilx\nimprt \"lib.dgm\"\nwrit(utilx.answer() + \":\" + lib.label())\n",
    )
    .unwrap();
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));

    let normal = run_dgm(&project, &home, Some(&server.base_url), &["run"]);
    assert_ok(&normal);
    let expected = String::from_utf8_lossy(&normal.stdout).to_string();

    let build = run_dgm(&project, &home, Some(&server.base_url), &["build", "--output", "dist/app.dgm"]);
    assert_ok(&build);
    let bundle_path = project.join("dist/app.dgm");
    assert!(bundle_path.exists());

    let inspect = run_dgm(&project, &home, Some(&server.base_url), &["inspect", bundle_path.to_string_lossy().as_ref()]);
    assert_ok(&inspect);
    let inspect_stdout = String::from_utf8_lossy(&inspect.stdout);
    assert!(inspect_stdout.contains("entry:"));
    assert!(inspect_stdout.contains("modules:"));

    fs::remove_file(project.join("main.dgm")).unwrap();
    fs::remove_file(project.join("lib.dgm")).unwrap();
    fs::remove_dir_all(project.join(".dgm/packages")).unwrap();

    let bundle_arg = bundle_path.to_string_lossy().to_string();
    let bundle_run = run_dgm(&runner, &home, None, &["run", bundle_arg.as_str()]);
    assert_ok(&bundle_run);
    assert_eq!(String::from_utf8_lossy(&bundle_run.stdout), expected);
}

#[test]
fn build_fails_when_lock_file_is_stale() {
    let home = temp_dir("build-home-stale");
    let project = temp_dir("build-project-stale");

    let util_pkg = package_with_files("utilx", "1.0.0", "def answer() { retrun 1 }\n", vec![]);
    let util_hash = sha256_hex(&util_pkg);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/utilx.json".into(),
                format!(
                    r#"{{"name":"utilx","versions":[{{"version":"1.0.0","url":"{}/archives/utilx-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    util_hash,
                    util_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/utilx-1.0.0.tar.gz".into(), util_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "stale_build_app", &[("utilx", "1.0.0")]);
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));

    write_project_manifest(&project, "stale_build_app", &[("utilx", "^1.0.0")]);
    let build = run_dgm(&project, &home, Some(&server.base_url), &["build"]);
    assert!(!build.status.success());
    assert!(String::from_utf8_lossy(&build.stderr).contains("build requires dgm.lock to match dgm.toml"));
}

#[test]
fn failed_build_keeps_previous_bundle_intact() {
    let home = temp_dir("build-home-atomic");
    let project = temp_dir("build-project-atomic");

    let util_pkg = package_with_files("utilx", "1.0.0", "def answer() { retrun 1 }\n", vec![]);
    let util_hash = sha256_hex(&util_pkg);
    let server = start_registry_server(move |base| {
        vec![
            (
                "/packages/utilx.json".into(),
                format!(
                    r#"{{"name":"utilx","versions":[{{"version":"1.0.0","url":"{}/archives/utilx-1.0.0.tar.gz","sha256":"{}","size":{},"dependencies":{{}}}}]}}"#,
                    base,
                    util_hash,
                    util_pkg.len()
                )
                .into_bytes(),
                "application/json",
            ),
            ("/archives/utilx-1.0.0.tar.gz".into(), util_pkg, "application/gzip"),
        ]
    });

    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["init"]));
    write_project_manifest(&project, "atomic_build_app", &[("utilx", "1.0.0")]);
    fs::write(project.join("main.dgm"), "imprt utilx\nwrit(utilx.answer())\n").unwrap();
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["install"]));
    assert_ok(&run_dgm(&project, &home, Some(&server.base_url), &["build"]));

    let bundle_path = project.join("build/bundle.dgm");
    let before = fs::read(&bundle_path).unwrap();

    write_project_manifest(&project, "atomic_build_app", &[("utilx", "^1.0.0")]);
    let failed = run_dgm(&project, &home, Some(&server.base_url), &["build"]);
    assert!(!failed.status.success());
    let after = fs::read(&bundle_path).unwrap();
    assert_eq!(before, after);
}

#[test]
fn corrupt_bundle_fails_checksum_validation() {
    let home = temp_dir("bundle-home-corrupt");
    let project = temp_dir("bundle-project-corrupt");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("main.dgm"), "writ(\"ok\")\n").unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let bundle_path = project.join("build/bundle.dgm");
    let mut bytes = fs::read(&bundle_path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    fs::write(&bundle_path, bytes).unwrap();

    let output = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("BundleError:"));
    assert!(stderr.contains("checksum mismatch"));
}

#[test]
fn bundle_version_mismatch_fails_cleanly() {
    let home = temp_dir("bundle-home-version");
    let project = temp_dir("bundle-project-version");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("main.dgm"), "writ(\"ok\")\n").unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let bundle_path = project.join("build/bundle.dgm");
    let (version, _, mut manifest, index, modules) = bundle_parts(&bundle_path);
    manifest["dgm_version"] = Value::String("9.0.0".into());
    write_bundle_parts(&bundle_path, version, &manifest, &index, &modules);

    let output = run_dgm(&project, &home, None, &["inspect", bundle_path.to_string_lossy().as_ref()]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("BundleError:"));
    assert!(stderr.contains("targets DGM 9.0.0"));
}

#[test]
fn oversized_bundle_is_rejected_before_deserialize() {
    let home = temp_dir("bundle-home-large");
    let project = temp_dir("bundle-project-large");
    let bundle_path = project.join("huge.dgm");

    fs::create_dir_all(&project).unwrap();
    let file = File::create(&bundle_path).unwrap();
    file.set_len(101 * 1024 * 1024).unwrap();

    let output = run_dgm(&project, &home, None, &["inspect", bundle_path.to_string_lossy().as_ref()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exceeds"));
}

#[test]
fn runtime_errors_include_source_location() {
    let home = temp_dir("err-home-source");
    let project = temp_dir("err-project-source");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("main.dgm"), "let x = missing_value\n").unwrap();

    let output = run_dgm(&project, &home, None, &["run"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RuntimeError: undefined variable 'missing_value'"));
    assert!(stderr.contains("main.dgm:1:1"));
    assert!(stderr.contains("1 | let x = missing_value"));
    assert!(stderr.contains("^"));
}

#[test]
fn bundled_runtime_errors_include_source_location() {
    let home = temp_dir("err-home-bundle");
    let project = temp_dir("err-project-bundle");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("main.dgm"), "let x = missing_value\n").unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let bundle_path = project.join("build/bundle.dgm");
    let output = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RuntimeError: undefined variable 'missing_value'"));
    assert!(stderr.contains("bundle/project/main.dgm:1:1"));
    assert!(stderr.contains("1 | let x = missing_value"));
    assert!(stderr.contains("^"));
}

#[test]
fn cli_profile_reports_bundle_hotspots() {
    let home = temp_dir("profile-home-bundle");
    let project = temp_dir("profile-project-bundle");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        r#"
def foo(n) {
    retrun n + 1
}
let items = []
push(items, foo(1))
push(items, foo(2))
"#,
    )
    .unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let bundle_path = project.join("build/bundle.dgm");
    let output = run_dgm(&project, &home, None, &["profile", bundle_path.to_string_lossy().as_ref()]);
    assert_ok(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("total_time:"));
    assert!(stdout.contains("allocations:"));
    assert!(stdout.contains("top_functions:"));
    assert!(stdout.contains("foo:"));
    assert!(stdout.contains("top_modules:"));
    assert!(stdout.contains("bundle/project/main.dgm"));
    assert!(stdout.contains("vm_instruction_count:"));
    assert!(stdout.contains("vm_frames:"));
}

#[test]
fn same_input_produces_same_output_across_runs() {
    let home = temp_dir("det-home");
    let project = temp_dir("det-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        r#"
let data = {
    "b": 2,
    "a": 1,
    "c": 3
}
writ(keys(data))
writ(values(data))
fr key in data {
    writ(key)
}
writ(data)
"#,
    )
    .unwrap();

    let first = run_dgm(&project, &home, None, &["run"]);
    let second = run_dgm(&project, &home, None, &["run"]);
    assert_ok(&first);
    assert_ok(&second);
    let expected = "[a, b, c]\n[1, 2, 3]\na\nb\nc\n{a: 1, b: 2, c: 3}\n";
    assert_eq!(String::from_utf8_lossy(&first.stdout), expected);
    assert_eq!(first.stdout, second.stdout);
}

#[test]
fn bundle_run_is_consistent_with_source_run() {
    let home = temp_dir("det-home-bundle");
    let project = temp_dir("det-project-bundle");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        r#"
let data = {
    "b": 2,
    "a": 1,
    "c": 3
}
writ(keys(data))
writ(values(data))
fr key in data {
    writ(key)
}
writ(data)
"#,
    )
    .unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let source_run = run_dgm(&project, &home, None, &["run"]);
    let bundle_path = project.join("build/bundle.dgm");
    let bundle_once = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    let bundle_twice = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    assert_ok(&source_run);
    assert_ok(&bundle_once);
    assert_ok(&bundle_twice);
    assert_eq!(source_run.stdout, bundle_once.stdout);
    assert_eq!(bundle_once.stdout, bundle_twice.stdout);
}

#[test]
fn large_bundle_with_many_modules_runs_deterministically() {
    let home = temp_dir("large-bundle-home");
    let project = temp_dir("large-bundle-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::create_dir_all(project.join("mods")).unwrap();

    let mut main = String::new();
    let mut expected_total = 0i64;
    for index in 0..120 {
        let module_name = format!("m{:03}", index);
        let value = index as i64 + 1;
        expected_total += value;
        fs::write(
            project.join("mods").join(format!("{}.dgm", module_name)),
            format!("def value() {{ retrun {} }}\n", value),
        )
        .unwrap();
        main.push_str(&format!("imprt \"./mods/{}.dgm\"\n", module_name));
    }
    main.push_str("let total = 0\n");
    for index in 0..120 {
        main.push_str(&format!("total = total + m{:03}.value()\n", index));
    }
    main.push_str("writ(total)\n");
    fs::write(project.join("main.dgm"), main).unwrap();

    let source_run = run_dgm(&project, &home, None, &["run"]);
    assert_ok(&source_run);
    assert_eq!(
        String::from_utf8_lossy(&source_run.stdout),
        format!("{}\n", expected_total)
    );

    assert_ok(&run_dgm(&project, &home, None, &["build"]));
    let bundle_path = project.join("build/bundle.dgm");

    let bundle_once = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    let bundle_twice = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    assert_ok(&bundle_once);
    assert_ok(&bundle_twice);
    assert_eq!(source_run.stdout, bundle_once.stdout);
    assert_eq!(bundle_once.stdout, bundle_twice.stdout);
}

#[test]
fn lazy_bundle_load_keeps_unused_modules_unloaded() {
    let home = temp_dir("lazy-home");
    let project = temp_dir("lazy-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        r#"
iff fals {
    imprt "./util.dgm"
}
writ("main")
"#,
    )
    .unwrap();
    fs::write(project.join("util.dgm"), "writ(\"util\")\n").unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let bundle_path = project.join("build/bundle.dgm");
    let image = bundle::load_bundle(&bundle_path).unwrap();
    let before = image.runtime_stats();
    assert_eq!(before.loaded_module_count, 0);
    let interp = run_bundle(image).unwrap();
    let after = interp.bundle_stats().unwrap();
    assert_eq!(after.loaded_module_count, 1);
}

#[test]
fn debug_bundle_can_fallback_to_source_for_missing_module() {
    let home = temp_dir("fallback-home");
    let project = temp_dir("fallback-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("util.dgm"), "let value = 42\n").unwrap();
    fs::write(project.join("main.dgm"), "imprt \"./util.dgm\"\nwrit(util.value)\n").unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build", "--debug"]));

    let bundle_path = project.join("build/bundle.dgm");
    let (version, _, mut manifest, index, modules) = bundle_parts(&bundle_path);
    assert_eq!(version, 2);
    let mut index_json: Value = serde_json::from_slice(&index).unwrap();
    if let Some(module_ids) = manifest["modules"].as_object_mut() {
        module_ids.remove("project:util.dgm");
    }
    if let Some(entries) = index_json["modules"].as_array_mut() {
        entries.retain(|entry| entry["key"].as_str() != Some("project:util.dgm"));
        for entry in entries.iter_mut() {
            if entry["key"].as_str() == Some("project:main.dgm") {
                if let Some(imports) = entry["imports"].as_object_mut() {
                    imports.remove("./util.dgm");
                }
            }
        }
    }
    let new_index = serde_json::to_vec(&index_json).unwrap();
    write_bundle_parts(&bundle_path, version, &manifest, &new_index, &modules);

    let output = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    assert_ok(&output);
    assert_eq!(String::from_utf8_lossy(&output.stdout), "42\n");
}

#[test]
fn cli_profile_reports_bundle_startup_metrics() {
    let home = temp_dir("profile-startup-home");
    let project = temp_dir("profile-startup-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("util.dgm"), "def value() { retrun 7 }\n").unwrap();
    fs::write(
        project.join("main.dgm"),
        "imprt \"./util.dgm\"\nwrit(util.value())\n",
    )
    .unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let bundle_path = project.join("build/bundle.dgm");
    let output = run_dgm(&project, &home, None, &["profile", bundle_path.to_string_lossy().as_ref()]);
    assert_ok(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("bundle_load_time:"));
    assert!(stdout.contains("bundle_entry_decode_time:"));
    assert!(stdout.contains("bundle_loaded_modules:"));
    assert!(stdout.contains("bundle_peak_loaded_module_bytes:"));
    assert!(stdout.contains("bundle_memory_mapped:"));
}

#[test]
fn release_bundle_serializes_bytecode_payload() {
    let home = temp_dir("bundle-bytecode-home");
    let project = temp_dir("bundle-bytecode-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        "def add(a, b) {\n    retrun a + b\n}\nwrit(add(2, 3))\n",
    )
    .unwrap();

    assert_ok(&run_dgm(&project, &home, None, &["build", "--release"]));

    let bundle_path = project.join("build/bundle.dgm");
    let (_version, _checksum, _manifest, index, modules) = bundle_parts(&bundle_path);
    let index_json: Value = serde_json::from_slice(&index).unwrap();
    let first = index_json["modules"][0].clone();
    let offset = first["offset"].as_u64().unwrap() as usize;
    let len = first["len"].as_u64().unwrap() as usize;
    let module_json: Value = serde_json::from_slice(&modules[offset..offset + len]).unwrap();
    assert!(module_json["bytecode"].is_object());
    assert!(module_json["stmts"].as_array().unwrap().is_empty());
}

#[test]
fn bundle_v1_remains_loadable_for_backward_compatibility() {
    let home = temp_dir("bundle-v1-home");
    let project = temp_dir("bundle-v1-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("util.dgm"), "let value = 21\n").unwrap();
    fs::write(project.join("main.dgm"), "imprt \"./util.dgm\"\nwrit(util.value * 2)\n").unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let bundle_path = project.join("build/bundle.dgm");
    let (version, _, mut manifest, index, modules) = bundle_parts(&bundle_path);
    assert_eq!(version, 2);
    manifest["bundle_format_version"] = Value::from(1u64);
    let v1_modules = v2_modules_to_v1_blob(&index, &modules);
    write_bundle_parts(&bundle_path, 1, &manifest, &[], &v1_modules);

    let output = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    assert_ok(&output);
    assert_eq!(String::from_utf8_lossy(&output.stdout), "42\n");
}

#[test]
fn bundle_run_matches_source_run_for_async_tasks() {
    let home = temp_dir("det-home-bundle-async");
    let project = temp_dir("det-project-bundle-async");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        r#"
imprt thread
let task = spawn(fn() {
    thread.sleep(20)
    retrun "done"
})
writ(awt task)
"#,
    )
    .unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build"]));

    let source_run = run_dgm(&project, &home, None, &["run"]);
    let bundle_path = project.join("build/bundle.dgm");
    let bundle_run = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);

    assert_ok(&source_run);
    assert_ok(&bundle_run);
    assert_eq!(source_run.stdout, bundle_run.stdout);
    assert_eq!(String::from_utf8_lossy(&bundle_run.stdout), "done\n");
}

#[test]
fn native_panic_is_wrapped_without_panic_leak() {
    let home = temp_dir("panic-home");
    let project = temp_dir("panic-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        "imprt debug\ndebug.native_panic(\"boom\")\n",
    )
    .unwrap();

    let output = run_dgm(&project, &home, None, &["run"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RuntimeError: internal panic in native function 'debug.native_panic': boom"));
    assert!(stderr.contains("main.dgm:2:1"));
    assert!(stderr.contains("2 | debug.native_panic(\"boom\")"));
    assert!(!stderr.contains("panicked at"));
    assert!(!stderr.contains("thread 'main' panicked"));
}

#[test]
fn async_errors_use_runtime_format_with_stack() {
    let home = temp_dir("async-home");
    let project = temp_dir("async-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        "imprt http\nawt http.get(\"http://127.0.0.1:1\")\n",
    )
    .unwrap();

    let output = run_dgm(&project, &home, None, &["run"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RuntimeError:"));
    assert!(stderr.contains("http.GET:"));
    assert!(stderr.contains("Connection Failed"));
    assert!(stderr.contains("main.dgm:2:1"));
}

#[test]
fn task_errors_keep_wrapped_message_and_stack() {
    let home = temp_dir("taskerr-home");
    let project = temp_dir("taskerr-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(
        project.join("main.dgm"),
        r#"imprt debug
let task = spawn(fn() {
    debug.native_panic("boom")
})
task.join()
"#,
    )
    .unwrap();

    let output = run_dgm(&project, &home, None, &["run"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RuntimeError: task failed: internal panic in native function 'debug.native_panic': boom"));
    assert!(stderr.contains("<lambda>()"));
    assert!(stderr.contains("main.dgm:3:5"));
    assert!(!stderr.contains("thread '"));
}

#[test]
fn import_errors_use_consistent_format_and_snippet() {
    let home = temp_dir("importerr-home");
    let project = temp_dir("importerr-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("main.dgm"), "imprt \"./missing\"\n").unwrap();

    let output = run_dgm(&project, &home, None, &["run"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ImportError:"));
    assert!(stderr.contains("main.dgm:1:1"));
    assert!(stderr.contains("1 | imprt \"./missing\""));
    assert!(stderr.contains("^"));
}

#[test]
fn release_bundle_falls_back_when_source_is_unavailable() {
    let home = temp_dir("release-bundle-home");
    let project = temp_dir("release-bundle-project");

    assert_ok(&run_dgm(&project, &home, None, &["init"]));
    fs::write(project.join("main.dgm"), "let x = missing_value\n").unwrap();
    assert_ok(&run_dgm(&project, &home, None, &["build", "--release"]));
    fs::remove_file(project.join("main.dgm")).unwrap();

    let bundle_path = project.join("build/bundle.dgm");
    let output = run_dgm(&project, &home, None, &["run", bundle_path.to_string_lossy().as_ref()]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("RuntimeError: undefined variable 'missing_value'"));
    assert!(stderr.contains("bundle/project/main.dgm:1:1"));
    assert!(stderr.contains("source unavailable"));
}

#[test]
fn thrown_values_survive_catch_without_stringifying() {
    let source = r#"
let caught = nul
try {
    thro {"code": 7, "items": [1, 2, 3]}
} catch err {
    caught = err
}
assert_eq(caught.code, 7)
assert_eq(caught.items[1], 2)

let text = ""
try {
    thro "boom"
} catch err {
    text = err
}
assert_eq(text, "boom")
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn thrown_errors_keep_stack_through_function_calls() {
    let dir = temp_dir("throw-stack");
    let main = dir.join("main.dgm");
    let source = r#"
def inner() {
    thro "boom"
}

def outer() {
    inner()
}

outer()
"#;
    fs::write(&main, source).unwrap();

    let err = match run_source_with_path(source, Some(main.clone())) {
        Ok(_) => panic!("expected thrown error"),
        Err(err) => err,
    };
    let rendered = format!("{}", err);
    assert!(rendered.contains("ThrownError: boom"));
    assert!(rendered.contains("inner()"));
    assert!(rendered.contains("outer()"));
    assert!(rendered.contains(&format!("{}:3:5", main.to_string_lossy())));
}

#[test]
fn task_and_await_preserve_thrown_values_for_catch() {
    let source = r#"
let task_value = nul
try {
    let task = spawn(fn() {
        thro {"code": 41}
    })
    awt task
} catch err {
    task_value = err
}
assert_eq(task_value.code, 41)
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn optional_chaining_and_null_coalescing_short_circuit_correctly() {
    let source = r#"
let ticks = 0

def mark() {
    ticks += 1
    retrun 9
}

let missing = nul
assert_eq(missing?.value, nul)
assert_eq(missing?.call(mark()), nul)
assert_eq(ticks, 0)

let profile = {"user": {"name": "DGM"}}
assert_eq(profile?.user.name, "DGM")

assert_eq(5 ?? mark(), 5)
assert_eq(ticks, 0)
assert_eq(nul ?? mark(), 9)
assert_eq(ticks, 1)
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn default_params_named_args_and_class_init_work() {
    let source = r#"
def connect(host, port=80, timeout=1000) {
    retrun f"{host}:{port}:{timeout}"
}

cls Client {
    def init(host, port=80, timeout=1000) {
        ths.value = connect(host, port=port, timeout=timeout)
    }
}

let decorate = lam(prefix, suffix="!") => prefix + suffix

assert_eq(connect("a.com"), "a.com:80:1000")
assert_eq(connect("a.com", timeout=500), "a.com:80:500")
assert_eq(connect(host="b.com", port=81), "b.com:81:1000")
assert_eq(connect("c.com", port=82, timeout=600), "c.com:82:600")
assert_eq(decorate("ok"), "ok!")

let client = new Client("svc", timeout=250)
assert_eq(client.value, "svc:80:250")
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn named_argument_failures_are_clear() {
    let missing = match run_source(
        r#"
def connect(host, port=80) {
    retrun host
}

connect(port=1)
"#,
    ) {
        Ok(_) => panic!("expected missing-argument error"),
        Err(err) => err,
    };
    let missing_rendered = format!("{}", missing);
    assert!(missing_rendered.contains("missing required argument 'host'"));

    let mixed = match run_source(
        r#"
def connect(host, port=80) {
    retrun host
}

connect(port=1, "svc")
"#,
    ) {
        Ok(_) => panic!("expected mixed-argument error"),
        Err(err) => err,
    };
    let mixed_rendered = format!("{}", mixed);
    assert!(mixed_rendered.contains("positional argument cannot follow named arguments"));

    let unknown = match run_source(
        r#"
def connect(host, port=80) {
    retrun host
}

connect("svc", tls=tru)
"#,
    ) {
        Ok(_) => panic!("expected unknown-argument error"),
        Err(err) => err,
    };
    let unknown_rendered = format!("{}", unknown);
    assert!(unknown_rendered.contains("unknown argument 'tls'"));

    let native = match run_source("len(list=[1, 2, 3])\n") {
        Ok(_) => panic!("expected native named-argument error"),
        Err(err) => err,
    };
    let native_rendered = format!("{}", native);
    assert!(native_rendered.contains("named argument 'list' is not supported for len"));
}

#[test]
fn destructuring_list_map_defaults_nested_and_ignore_work() {
    let source = r#"
let [_, second] = [1, 2, 3]
assert_eq(second, 2)

let {x = 0, y, a: {b}} = {"y": 7, "a": {"b": 9}}
assert_eq(x, 0)
assert_eq(y, 7)
assert_eq(b, 9)

let {user: profile} = {"user": {"name": "DGM"}}
assert_eq(profile.name, "DGM")
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn destructuring_evaluates_rhs_once_and_errors_cleanly() {
    let source = r#"
let calls = 0

def build() {
    calls += 1
    retrun {"x": 4}
}

let {x} = build()
assert_eq(x, 4)
assert_eq(calls, 1)
"#;

    run_source_with_path(source, None).unwrap();

    let null_err = match run_source("let {x} = nul\n") {
        Ok(_) => panic!("expected map destructuring null error"),
        Err(err) => err,
    };
    assert!(format!("{}", null_err).contains("map destructuring requires map value"));

    let missing_list_err = match run_source("let [a, b] = [1]\n") {
        Ok(_) => panic!("expected missing list binding error"),
        Err(err) => err,
    };
    assert!(format!("{}", missing_list_err).contains("missing value for binding 'b'"));

    let missing_map_err = match run_source("let {x} = {}\n") {
        Ok(_) => panic!("expected missing map binding error"),
        Err(err) => err,
    };
    assert!(format!("{}", missing_map_err).contains("missing value for binding 'x'"));
}

#[test]
fn match_binding_guard_and_fallback_are_predictable() {
    let source = r#"
let result = ""

mtch 3 {
    n whn n > 10 => result = "big"
    n whn n > 0 => result = f"small:{n}"
    _ => result = "other"
}

assert_eq(result, "small:3")

let only_first = ""
let guard_calls = 0

def expensive() {
    guard_calls += 1
    retrun tru
}

mtch 0 {
    0 => only_first = "zero"
    n whn expensive() => only_first = f"late:{n}"
    _ => only_first = "fallback"
}

assert_eq(only_first, "zero")
assert_eq(guard_calls, 0)

let seen_null = ""
mtch nul {
    nul => seen_null = "nul"
    _ => seen_null = "bad"
}
assert_eq(seen_null, "nul")
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn stdlib_core_modules_cover_string_math_time_and_collections() {
    let source = r#"
imprt strutils
imprt math
imprt time
imprt collections

assert_eq(strutils.lower(strutils.trim("  HeLLo  ")), "hello")
assert_eq(strutils.replace("a-b-c", "-", ":"), "a:b:c")
assert_eq(strutils.join(strutils.split("x,y,z", ","), "|"), "x|y|z")

assert_eq(math.abs(-5), 5)
assert_eq(math.floor(3.7), 3)
assert_eq(math.ceil(3.2), 4)
let rand = math.random()
assert(rand >= 0 and rand < 1)

let started = time.now_ms()
time.sleep(20)
assert(time.now_ms() - started >= 10)

let total = collections.reduce(collections.map(collections.filter([1, 2, 3, 4], lam(x) => x % 2 == 0), lam(x) => x * 10), 0, lam(acc, x) => acc + x)
assert_eq(total, 60)
"#;

    run_source_with_path(source, None).unwrap();
}

#[test]
fn log_module_emits_predictable_masked_output() {
    let home = temp_dir("log-home");
    let project = temp_dir("log-project");
    fs::write(
        project.join("main.dgm"),
        r#"imprt log
log.info("hello", {"Authorization": "Bearer super-secret-token"})
log.warn("watch")
log.error("fail")
"#,
    )
    .unwrap();

    let output = run_dgm(&project, &home, None, &["run", "main.dgm"]);
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("[info]"));
    assert!(stderr.contains("[warn]"));
    assert!(stderr.contains("[error]"));
    assert!(!stderr.contains("super-secret-token"));
    assert!(stderr.contains("****oken"));
}

#[test]
fn api_server_example_handles_auth_persistence_and_1024_concurrent_requests() {
    let home = temp_dir("api-server-home");
    let project = example_project("api-server-project");
    let data_dir = temp_dir("api-server-data");
    let port = free_port();
    let token = "integration-secret-token";
    let max_requests = 1030usize;

    let envs = vec![
        ("PORT", port.to_string()),
        ("AUTH_TOKEN", token.to_string()),
        ("DATA_DIR", data_dir.to_string_lossy().to_string()),
        ("MAX_THREADS", "64".to_string()),
        ("MAX_CONNECTIONS", "2048".to_string()),
        ("TIMEOUT_MS", "20000".to_string()),
        ("READ_TIMEOUT_MS", "20000".to_string()),
        ("WRITE_TIMEOUT_MS", "20000".to_string()),
        ("MAX_BODY_SIZE", "131072".to_string()),
        ("MAX_REQUESTS", max_requests.to_string()),
        ("LOG_REQUESTS", "fals".to_string()),
        ("COLLECT_METRICS", "fals".to_string()),
    ];

    let mut child = spawn_dgm(&project, &home, &["run"], &envs);
    let base = format!("http://127.0.0.1:{port}");

    let unauthorized = send_with_retry_timeout(|| ureq::get(&format!("{base}/users")).call(), Duration::from_secs(5));
    assert_eq!(unauthorized.status(), 401);
    let unauthorized_body: Value = serde_json::from_str(&unauthorized.into_string().unwrap()).unwrap();
    assert_eq!(unauthorized_body["error"]["code"], "unauthorized");

    let create_one = send_with_retry_timeout(|| {
        ureq::post(&format!("{base}/users"))
            .set("authorization", &format!("Bearer {token}"))
            .set("content-type", "application/json")
            .send_string(r#"{"name":"Minh","email":"minh@example.com"}"#)
    }, Duration::from_secs(5));
    assert_eq!(create_one.status(), 201);
    let create_one_body: Value = serde_json::from_str(&create_one.into_string().unwrap()).unwrap();
    assert_eq!(create_one_body["user"]["id"], "1");
    assert_eq!(create_one_body["user"]["name"], "Minh");

    let create_two = send_with_retry_timeout(|| {
        ureq::post(&format!("{base}/users"))
            .set("authorization", &format!("Bearer {token}"))
            .set("content-type", "application/json")
            .send_string(r#"{"name":"Lan","email":"lan@example.com"}"#)
    }, Duration::from_secs(5));
    assert_eq!(create_two.status(), 201);
    let create_two_body: Value = serde_json::from_str(&create_two.into_string().unwrap()).unwrap();
    assert_eq!(create_two_body["user"]["id"], "2");
    assert_eq!(create_two_body["user"]["email"], "lan@example.com");

    let user_one = send_with_retry_timeout(|| {
        ureq::get(&format!("{base}/users/1"))
            .set("authorization", &format!("Bearer {token}"))
            .call()
    }, Duration::from_secs(5));
    assert_eq!(user_one.status(), 200);
    let user_one_body: Value = serde_json::from_str(&user_one.into_string().unwrap()).unwrap();
    assert_eq!(user_one_body["user"]["name"], "Minh");

    let list = send_with_retry_timeout(|| {
        ureq::get(&format!("{base}/users"))
            .set("authorization", &format!("Bearer {token}"))
            .call()
    }, Duration::from_secs(5));
    assert_eq!(list.status(), 200);
    let list_body: Value = serde_json::from_str(&list.into_string().unwrap()).unwrap();
    assert_eq!(list_body["users"].as_array().unwrap().len(), 2);

    let concurrent = concurrent_json_gets(&format!("{base}/users"), token, 1024);
    assert_eq!(concurrent.len(), 1024);
    for (status, body) in concurrent {
        assert_eq!(status, 200);
        assert_eq!(body["ok"], true);
        assert_eq!(body["users"].as_array().unwrap().len(), 2);
    }

    let health = send_with_retry_timeout(|| ureq::get(&format!("{base}/health")).call(), Duration::from_secs(5));
    assert_eq!(health.status(), 200);
    let health_body: Value = serde_json::from_str(&health.into_string().unwrap()).unwrap();
    assert_eq!(health_body["status"], "ready");

    let output = wait_for_child_output(&mut child, Duration::from_secs(30));
    assert_ok(&output);

    let persisted: Value = serde_json::from_str(&fs::read_to_string(data_dir.join("users.json")).unwrap()).unwrap();
    assert_eq!(persisted.as_array().unwrap().len(), 2);
    assert_eq!(persisted[0]["name"], "Minh");
    assert_eq!(persisted[1]["email"], "lan@example.com");
}

#[test]
fn api_server_example_bundle_run_serves_same_routes() {
    let home = temp_dir("api-server-bundle-home");
    let project = example_project("api-server-bundle-project");
    let data_dir = temp_dir("api-server-bundle-data");

    let build = run_dgm(&project, &home, None, &["build", "--output", "dist/api-server.dgm"]);
    assert_ok(&build);
    let bundle = project.join("dist/api-server.dgm");
    assert!(bundle.exists());

    let port = free_port();
    let token = "bundle-secret-token";
    let envs = vec![
        ("PORT", port.to_string()),
        ("AUTH_TOKEN", token.to_string()),
        ("DATA_DIR", data_dir.to_string_lossy().to_string()),
        ("MAX_THREADS", "16".to_string()),
        ("MAX_CONNECTIONS", "256".to_string()),
        ("TIMEOUT_MS", "10000".to_string()),
        ("READ_TIMEOUT_MS", "10000".to_string()),
        ("WRITE_TIMEOUT_MS", "10000".to_string()),
        ("MAX_BODY_SIZE", "131072".to_string()),
        ("MAX_REQUESTS", "4".to_string()),
        ("LOG_REQUESTS", "fals".to_string()),
        ("COLLECT_METRICS", "fals".to_string()),
    ];

    let bundle_arg = bundle.to_string_lossy().to_string();
    let mut child = spawn_dgm(&project, &home, &["run", &bundle_arg], &envs);
    let base = format!("http://127.0.0.1:{port}");

    let created = send_with_retry_timeout(|| {
        ureq::post(&format!("{base}/users"))
            .set("authorization", &format!("Bearer {token}"))
            .set("content-type", "application/json")
            .send_string(r#"{"name":"Bundle","email":"bundle@example.com"}"#)
    }, Duration::from_secs(5));
    assert_eq!(created.status(), 201);
    let created_body: Value = serde_json::from_str(&created.into_string().unwrap()).unwrap();
    assert_eq!(created_body["user"]["id"], "1");

    let user = send_with_retry_timeout(|| {
        ureq::get(&format!("{base}/users/1"))
            .set("authorization", &format!("Bearer {token}"))
            .call()
    }, Duration::from_secs(5));
    assert_eq!(user.status(), 200);
    let user_body: Value = serde_json::from_str(&user.into_string().unwrap()).unwrap();
    assert_eq!(user_body["user"]["email"], "bundle@example.com");

    let list = send_with_retry_timeout(|| {
        ureq::get(&format!("{base}/users"))
            .set("authorization", &format!("Bearer {token}"))
            .call()
    }, Duration::from_secs(5));
    assert_eq!(list.status(), 200);
    let list_body: Value = serde_json::from_str(&list.into_string().unwrap()).unwrap();
    assert_eq!(list_body["users"].as_array().unwrap().len(), 1);

    let health = send_with_retry_timeout(|| ureq::get(&format!("{base}/health")).call(), Duration::from_secs(5));
    assert_eq!(health.status(), 200);

    let output = wait_for_child_output(&mut child, Duration::from_secs(20));
    assert_ok(&output);

    let persisted: Value = serde_json::from_str(&fs::read_to_string(data_dir.join("users.json")).unwrap()).unwrap();
    assert_eq!(persisted.as_array().unwrap().len(), 1);
    assert_eq!(persisted[0]["name"], "Bundle");
}

#[test]
fn api_server_example_metrics_expose_runtime_and_route_counters() {
    let home = temp_dir("api-server-metrics-home");
    let project = example_project("api-server-metrics-project");
    let data_dir = temp_dir("api-server-metrics-data");
    let port = free_port();
    let token = "metrics-secret-token";
    let envs = vec![
        ("PORT", port.to_string()),
        ("AUTH_TOKEN", token.to_string()),
        ("DATA_DIR", data_dir.to_string_lossy().to_string()),
        ("MAX_THREADS", "8".to_string()),
        ("MAX_CONNECTIONS", "128".to_string()),
        ("TIMEOUT_MS", "10000".to_string()),
        ("READ_TIMEOUT_MS", "10000".to_string()),
        ("WRITE_TIMEOUT_MS", "10000".to_string()),
        ("MAX_BODY_SIZE", "131072".to_string()),
        ("MAX_REQUESTS", "4".to_string()),
        ("LOG_REQUESTS", "fals".to_string()),
        ("COLLECT_METRICS", "tru".to_string()),
    ];

    let mut child = spawn_dgm(&project, &home, &["run"], &envs);
    let base = format!("http://127.0.0.1:{port}");

    let health = send_with_retry_timeout(|| ureq::get(&format!("{base}/health")).call(), Duration::from_secs(5));
    assert_eq!(health.status(), 200);

    let unauthorized = send_with_retry_timeout(|| ureq::get(&format!("{base}/users")).call(), Duration::from_secs(5));
    assert_eq!(unauthorized.status(), 401);

    let list = send_with_retry_timeout(|| {
        ureq::get(&format!("{base}/users"))
            .set("authorization", &format!("Bearer {token}"))
            .call()
    }, Duration::from_secs(5));
    assert_eq!(list.status(), 200);

    let metrics = send_with_retry_timeout(|| {
        ureq::get(&format!("{base}/metrics"))
            .set("authorization", &format!("Bearer {token}"))
            .call()
    }, Duration::from_secs(5));
    assert_eq!(metrics.status(), 200);
    let body: Value = serde_json::from_str(&metrics.into_string().unwrap()).unwrap();
    assert!(body["requests"].as_i64().unwrap() >= 3);
    assert_eq!(body["auth_failures"], 1);
    assert!(body["runtime"]["elapsed_wall_time_ms"].as_i64().unwrap() >= 0);
    assert!(body["profile"]["alloc_count"].as_i64().unwrap() >= 0);
    assert!(body["profile"]["alloc_by_label"].is_object());
    assert!(body["profile"]["alloc_by_label"].as_object().unwrap().len() > 0);
    assert!(body["profile"]["alloc_by_label"]["concurrency.restore.payload"]["bytes"].as_i64().unwrap() > 0);
    assert!(body["request_profile"]["alloc_count"].as_i64().unwrap() >= 0);
    assert!(body["async"]["retained_tasks"].as_i64().unwrap() >= 0);
    assert!(body["snapshot"]["prepared_callable_count"].as_i64().unwrap() >= 1);
    assert!(body["snapshot"]["env_snapshot_count"].as_i64().unwrap() >= 1);
    assert!(body["snapshot"]["value_snapshot_count"].as_i64().unwrap() >= 1);
    assert_eq!(body["server"]["max_threads"], 8);
    assert!(body["server"]["idle_threads"].as_i64().unwrap() >= 0);
    assert!(body["server"]["trim_calls"].as_i64().unwrap() >= 0);
    assert!(body["server"]["trim_successes"].as_i64().unwrap() >= 0);
    assert!(body["server"]["trim_failures"].as_i64().unwrap() >= 0);
    assert!(body["server"]["profile"]["alloc_by_label"].is_object());
    assert_eq!(body["routes"]["users.index"]["requests"], 2);
    assert_eq!(body["routes"]["users.index"]["auth_failures"], 1);
    assert_eq!(body["routes"]["health"]["requests"], 1);

    let trim = send_with_retry_timeout(|| {
        ureq::post(&format!("{base}/metrics/trim"))
            .set("authorization", &format!("Bearer {token}"))
            .call()
    }, Duration::from_secs(5));
    assert_eq!(trim.status(), 200);
    let trim_body: Value = serde_json::from_str(&trim.into_string().unwrap()).unwrap();
    assert!(trim_body["trimmed"].is_boolean());
    assert!(trim_body["server"]["trim_calls"].as_i64().unwrap() >= 1);
    assert!(trim_body["server"]["manual_trim_calls"].as_i64().unwrap() >= 1);

    let output = wait_for_child_output(&mut child, Duration::from_secs(20));
    assert_ok(&output);
}

#[test]
fn api_server_example_handles_concurrent_posts_without_store_corruption() {
    let home = temp_dir("api-server-posts-home");
    let project = example_project("api-server-posts-project");
    let data_dir = temp_dir("api-server-posts-data");
    let port = free_port();
    let token = "concurrent-posts-token";
    let post_count = 48usize;
    let max_requests = post_count + 2;

    let envs = vec![
        ("PORT", port.to_string()),
        ("AUTH_TOKEN", token.to_string()),
        ("DATA_DIR", data_dir.to_string_lossy().to_string()),
        ("MAX_THREADS", "64".to_string()),
        ("MAX_CONNECTIONS", "2048".to_string()),
        ("TIMEOUT_MS", "20000".to_string()),
        ("READ_TIMEOUT_MS", "20000".to_string()),
        ("WRITE_TIMEOUT_MS", "20000".to_string()),
        ("MAX_BODY_SIZE", "131072".to_string()),
        ("MAX_REQUESTS", max_requests.to_string()),
        ("LOG_REQUESTS", "fals".to_string()),
        ("COLLECT_METRICS", "fals".to_string()),
    ];

    let mut child = spawn_dgm(&project, &home, &["run"], &envs);
    let base = format!("http://127.0.0.1:{port}");

    let health = send_with_retry_timeout(|| ureq::get(&format!("{base}/health")).call(), Duration::from_secs(5));
    assert_eq!(health.status(), 200);

    let created = concurrent_json_posts(&format!("{base}/users"), token, post_count);
    assert_eq!(created.len(), post_count);
    for (status, body) in created {
        assert_eq!(status, 201, "{body}");
        assert_eq!(body["ok"], true);
    }

    let list = send_with_retry_timeout(|| {
        ureq::get(&format!("{base}/users"))
            .set("authorization", &format!("Bearer {token}"))
            .call()
    }, Duration::from_secs(5));
    assert_eq!(list.status(), 200);
    let list_body: Value = serde_json::from_str(&list.into_string().unwrap()).unwrap();
    assert_eq!(list_body["users"].as_array().unwrap().len(), post_count);

    let output = wait_for_child_output(&mut child, Duration::from_secs(30));
    assert_ok(&output);

    let persisted: Value = serde_json::from_str(&fs::read_to_string(data_dir.join("users.json")).unwrap()).unwrap();
    assert_eq!(persisted.as_array().unwrap().len(), post_count);
}
