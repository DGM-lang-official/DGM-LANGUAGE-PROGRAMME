use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock, TryLockError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::concurrency::{
    owned_payload, prepare_isolated_call, prepare_isolated_callable, run_prepared_call_for_response, run_prepared_callable,
    schedule_named_future, OwnedPayload, OwnedValue, PreparedCallable,
};
use crate::error::{trap_dgm, DgmError};
use crate::interpreter::{
    runtime_begin_request, runtime_check_limits, runtime_end_request, runtime_record_io, runtime_reserve_labeled,
    runtime_merge_profile_into, runtime_profile_snapshot, runtime_profile_to_value, runtime_reserve_value,
    runtime_reserve_value_labeled, runtime_reset_usage, request_value_from_parts, DgmValue, RequestBacking,
    RuntimeProfile,
};
use crate::io_runtime;
use crate::memory::{maybe_trim_allocator, memory_snapshot, reset_memory_snapshot, TrimTrigger};
use crate::request_scope::{begin_request_scope, mark_response_owned_raw_json};
use crate::stdlib::json_mod::{dgm_to_json_string, encode_dgm_json_into, json_to_dgm, parse_json_limited};
use reqwest::redirect::Policy;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

const DEFAULT_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_MAX_BODY_SIZE: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_REDIRECTS: u32 = 5;
const DEFAULT_RETRIES: u32 = 3;
const DEFAULT_RETRY_BACKOFF_MS: u64 = 50;
const DEFAULT_RETRY_BACKOFF_MAX_MS: u64 = 500;
const DEFAULT_MAX_THREADS_MULTIPLIER: usize = 2;
const DEFAULT_MAX_CONNECTIONS_MULTIPLIER: usize = 8;
const DEFAULT_MAX_THREADS_CAP: usize = 16;
const SERVER_WORKER_STACK_SIZE: usize = 512 * 1024;
const JSON_BODY_BUFFER_INITIAL_CAPACITY: usize = 4 * 1024;
const JSON_BODY_POOL_MAX_BUFFERS: usize = 64;
const JSON_BODY_POOL_MAX_CAPACITY: usize = 256 * 1024;

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("get", http_get),
        ("post", http_post),
        ("put", http_put),
        ("delete", http_delete),
        ("request", http_request),
        ("serve", http_serve),
        ("new", http_new),
        ("stats", http_stats),
        ("reset_stats", http_reset_stats),
    ];
    for (name, func) in fns {
        m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("http.{}", name), func: *func });
    }
    m
}

#[derive(Debug, Clone)]
struct RequestOptions {
    headers: Vec<(String, String)>,
    query: Vec<(String, String)>,
    body: Option<String>,
    timeout_ms: u64,
    connect_timeout_ms: u64,
    max_body_size: usize,
    max_redirects: u32,
    retries: u32,
    retry_backoff_ms: u64,
    retry_backoff_max_ms: u64,
}

#[derive(Debug, Clone)]
struct ServerOptions {
    timeout_ms: u64,
    read_timeout_ms: u64,
    write_timeout_ms: u64,
    max_body_size: usize,
    max_requests: Option<usize>,
    max_threads: usize,
    max_connections: usize,
}

enum ServerHandler {
    PreparedCallback(PreparedCallable),
    Routes(Rc<RefCell<Vec<DgmValue>>>),
    StaticRoutes(HashMap<String, DgmValue>),
}

#[derive(Clone)]
enum RouteHandler {
    Live(DgmValue),
    Prepared(PreparedCallable),
}

struct ServerResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: ServerBody,
}

enum ServerBody {
    Owned(String),
    JsonBytes(PooledJsonBody),
    RawJson(Arc<crate::interpreter::RawJsonValue>),
}

impl ServerBody {
    fn len(&self) -> usize {
        match self {
            ServerBody::Owned(text) => text.len(),
            ServerBody::JsonBytes(bytes) => bytes.len(),
            ServerBody::RawJson(raw) => raw.len(),
        }
    }
}

impl From<String> for ServerBody {
    fn from(value: String) -> Self {
        ServerBody::Owned(value)
    }
}

impl From<&str> for ServerBody {
    fn from(value: &str) -> Self {
        ServerBody::Owned(value.to_string())
    }
}

#[derive(Debug, Clone)]
struct ParsedRequest {
    method: String,
    raw_url: String,
    path: String,
    query: String,
    headers: HashMap<String, String>,
    body: String,
}

enum RequestJob {
    Immediate(ServerResponse),
    Spawn { handler: DgmValue, request_value: DgmValue },
    SpawnPrepared { handler: PreparedCallable, request_payload: crate::concurrency::OwnedPayload },
}

type PoolJob = Box<dyn FnOnce() + Send + 'static>;

enum PoolMessage {
    Run(PoolJob),
    Stop,
}

struct ThreadPool {
    sender: Option<mpsc::Sender<PoolMessage>>,
    workers: Vec<JoinHandle<()>>,
}

#[derive(Debug, Clone, Default)]
struct ThreadPoolMetricsState {
    max_threads: usize,
    submitted_jobs: u64,
    completed_jobs: u64,
    rejected_jobs: u64,
    pending_jobs: usize,
    running_jobs: usize,
    profile: RuntimeProfile,
}

thread_local! {
    static PREPARED_ROUTE_HANDLERS: RefCell<HashMap<u64, PreparedCallable>> = RefCell::new(HashMap::new());
}

static NEXT_PREPARED_ROUTE_HANDLER_ID: OnceLock<AtomicU64> = OnceLock::new();
static THREAD_POOL_METRICS: OnceLock<Mutex<ThreadPoolMetricsState>> = OnceLock::new();
static JSON_BODY_POOL: OnceLock<Mutex<Vec<Vec<u8>>>> = OnceLock::new();

struct PooledJsonBody {
    bytes: Vec<u8>,
}

impl PooledJsonBody {
    fn encode(value: &DgmValue) -> Result<Self, DgmError> {
        let (mut bytes, reused) = acquire_json_body_buffer();
        let initial_capacity = bytes.capacity();
        encode_dgm_json_into(value, &mut bytes)?;
        let reserved_bytes = if reused {
            bytes.capacity().saturating_sub(initial_capacity)
        } else {
            bytes.capacity()
        };
        if reserved_bytes > 0 {
            runtime_reserve_labeled("http.server.json_body", std::mem::size_of::<Vec<u8>>() + reserved_bytes)?;
        }
        Ok(Self { bytes })
    }

    fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    fn len(&self) -> usize {
        self.bytes.len()
    }
}

impl Drop for PooledJsonBody {
    fn drop(&mut self) {
        release_json_body_buffer(std::mem::take(&mut self.bytes));
    }
}

fn json_body_pool() -> &'static Mutex<Vec<Vec<u8>>> {
    JSON_BODY_POOL.get_or_init(|| Mutex::new(Vec::new()))
}

fn acquire_json_body_buffer() -> (Vec<u8>, bool) {
    match json_body_pool().try_lock() {
        Ok(mut pool) => {
            if let Some(mut bytes) = pool.pop() {
                bytes.clear();
                (bytes, true)
            } else {
                (Vec::with_capacity(JSON_BODY_BUFFER_INITIAL_CAPACITY), false)
            }
        }
        Err(TryLockError::Poisoned(poisoned)) => {
            let mut pool = poisoned.into_inner();
            if let Some(mut bytes) = pool.pop() {
                bytes.clear();
                (bytes, true)
            } else {
                (Vec::with_capacity(JSON_BODY_BUFFER_INITIAL_CAPACITY), false)
            }
        }
        Err(TryLockError::WouldBlock) => (Vec::with_capacity(JSON_BODY_BUFFER_INITIAL_CAPACITY), false),
    }
}

fn release_json_body_buffer(mut bytes: Vec<u8>) {
    if bytes.capacity() == 0 || bytes.capacity() > JSON_BODY_POOL_MAX_CAPACITY {
        return;
    }
    bytes.clear();
    match json_body_pool().try_lock() {
        Ok(mut pool) => {
            if pool.len() < JSON_BODY_POOL_MAX_BUFFERS {
                pool.push(bytes);
            }
        }
        Err(TryLockError::Poisoned(poisoned)) => {
            let mut pool = poisoned.into_inner();
            if pool.len() < JSON_BODY_POOL_MAX_BUFFERS {
                pool.push(bytes);
            }
        }
        Err(TryLockError::WouldBlock) => {}
    }
}

fn http_get(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(url)), options) => execute_request("GET", url, None, options.cloned()),
        _ => Err(DgmError::RuntimeError { msg: "http.get(url, options?) required".into() }),
    }
}

fn http_post(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1), args.get(2)) {
        (Some(DgmValue::Str(url)), Some(body @ DgmValue::Map(_)), None) if is_option_map(body) => execute_request("POST", url, None, Some(body.clone())),
        (Some(DgmValue::Str(url)), body, options) => execute_request("POST", url, body.cloned(), options.cloned()),
        _ => Err(DgmError::RuntimeError { msg: "http.post(url, body?, options?) required".into() }),
    }
}

fn http_put(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1), args.get(2)) {
        (Some(DgmValue::Str(url)), Some(body @ DgmValue::Map(_)), None) if is_option_map(body) => execute_request("PUT", url, None, Some(body.clone())),
        (Some(DgmValue::Str(url)), body, options) => execute_request("PUT", url, body.cloned(), options.cloned()),
        _ => Err(DgmError::RuntimeError { msg: "http.put(url, body?, options?) required".into() }),
    }
}

fn http_delete(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(url)), options) => execute_request("DELETE", url, None, options.cloned()),
        _ => Err(DgmError::RuntimeError { msg: "http.delete(url, options?) required".into() }),
    }
}

fn http_request(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1), args.get(2), args.get(3)) {
        (Some(DgmValue::Str(method)), Some(DgmValue::Str(url)), Some(body @ DgmValue::Map(_)), None) if is_option_map(body) => {
            execute_request(method, url, None, Some(body.clone()))
        }
        (Some(DgmValue::Str(method)), Some(DgmValue::Str(url)), body, options) => {
            execute_request(method, url, body.cloned(), options.cloned())
        }
        _ => Err(DgmError::RuntimeError { msg: "http.request(method, url, body?, options?) required".into() }),
    }
}

pub(crate) fn try_schedule_async_call(callee: &DgmValue, args: Vec<DgmValue>) -> Result<Option<DgmValue>, DgmError> {
    match callee {
        DgmValue::NativeFunction { name, .. } if name == "http.get" => match (args.first(), args.get(1)) {
            (Some(DgmValue::Str(url)), options) => execute_request_future("GET", url, None, options.cloned()).map(Some),
            _ => Err(DgmError::RuntimeError { msg: "http.get(url, options?) required".into() }),
        },
        DgmValue::NativeFunction { name, .. } if name == "http.post" => match (args.first(), args.get(1), args.get(2)) {
            (Some(DgmValue::Str(url)), Some(body @ DgmValue::Map(_)), None) if is_option_map(body) => {
                execute_request_future("POST", url, None, Some(body.clone())).map(Some)
            }
            (Some(DgmValue::Str(url)), body, options) => execute_request_future("POST", url, body.cloned(), options.cloned()).map(Some),
            _ => Err(DgmError::RuntimeError { msg: "http.post(url, body?, options?) required".into() }),
        },
        DgmValue::NativeFunction { name, .. } if name == "http.put" => match (args.first(), args.get(1), args.get(2)) {
            (Some(DgmValue::Str(url)), Some(body @ DgmValue::Map(_)), None) if is_option_map(body) => {
                execute_request_future("PUT", url, None, Some(body.clone())).map(Some)
            }
            (Some(DgmValue::Str(url)), body, options) => execute_request_future("PUT", url, body.cloned(), options.cloned()).map(Some),
            _ => Err(DgmError::RuntimeError { msg: "http.put(url, body?, options?) required".into() }),
        },
        DgmValue::NativeFunction { name, .. } if name == "http.delete" => match (args.first(), args.get(1)) {
            (Some(DgmValue::Str(url)), options) => execute_request_future("DELETE", url, None, options.cloned()).map(Some),
            _ => Err(DgmError::RuntimeError { msg: "http.delete(url, options?) required".into() }),
        },
        DgmValue::NativeFunction { name, .. } if name == "http.request" => match (args.first(), args.get(1), args.get(2), args.get(3)) {
            (Some(DgmValue::Str(method)), Some(DgmValue::Str(url)), Some(body @ DgmValue::Map(_)), None) if is_option_map(body) => {
                execute_request_future(method, url, None, Some(body.clone())).map(Some)
            }
            (Some(DgmValue::Str(method)), Some(DgmValue::Str(url)), body, options) => {
                execute_request_future(method, url, body.cloned(), options.cloned()).map(Some)
            }
            _ => Err(DgmError::RuntimeError { msg: "http.request(method, url, body?, options?) required".into() }),
        },
        _ => Ok(None),
    }
}

fn http_serve(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let port = match args.first() {
        Some(DgmValue::Int(port)) if *port > 0 => *port as u16,
        _ => return Err(DgmError::RuntimeError { msg: "http.serve(port, handler, options?) required".into() }),
    };
    let handler = args.get(1).ok_or_else(|| DgmError::RuntimeError { msg: "http.serve(port, handler, options?) required".into() })?;
    let options = parse_server_options(args.get(2).cloned())?;
    match handler {
        value if is_callable(value) => serve_loop(port, ServerHandler::PreparedCallback(prepare_isolated_callable(value.clone())?), options),
        DgmValue::Map(routes) => serve_loop(port, ServerHandler::StaticRoutes(routes.borrow().clone()), options),
        _ => Err(DgmError::RuntimeError { msg: "http.serve requires callable handler or routes map".into() }),
    }
}

fn http_new(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "http.new() does not take arguments".into() });
    }
    let routes = DgmValue::List(Rc::new(RefCell::new(vec![])));
    let routes_state = Box::new(routes.clone());
    let mut app = HashMap::new();
    app.insert("get".into(), DgmValue::NativeMethod { name: "http.app.get".into(), state: routes_state.clone(), func: router_get });
    app.insert("post".into(), DgmValue::NativeMethod { name: "http.app.post".into(), state: routes_state.clone(), func: router_post });
    app.insert("put".into(), DgmValue::NativeMethod { name: "http.app.put".into(), state: routes_state.clone(), func: router_put });
    app.insert("delete".into(), DgmValue::NativeMethod { name: "http.app.delete".into(), state: routes_state.clone(), func: router_delete });
    app.insert("listen".into(), DgmValue::NativeMethod { name: "http.app.listen".into(), state: routes_state, func: router_listen });
    let value = DgmValue::Map(Rc::new(RefCell::new(app)));
    runtime_reserve_value(&value)?;
    Ok(value)
}

fn next_prepared_route_handler_id() -> u64 {
    NEXT_PREPARED_ROUTE_HANDLER_ID
        .get_or_init(|| AtomicU64::new(1))
        .fetch_add(1, Ordering::Relaxed)
}

fn thread_pool_metrics_holder() -> &'static Mutex<ThreadPoolMetricsState> {
    THREAD_POOL_METRICS.get_or_init(|| Mutex::new(ThreadPoolMetricsState::default()))
}

fn lock_thread_pool_metrics() -> std::sync::MutexGuard<'static, ThreadPoolMetricsState> {
    match thread_pool_metrics_holder().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn merge_thread_pool_profile(profile: &RuntimeProfile) {
    let mut metrics = lock_thread_pool_metrics();
    runtime_merge_profile_into(&mut metrics.profile, profile);
}

fn cache_prepared_route_handler(handler: &DgmValue) -> Result<u64, DgmError> {
    let prepared = prepare_isolated_callable(handler.clone())?;
    let id = next_prepared_route_handler_id();
    PREPARED_ROUTE_HANDLERS.with(|handlers| {
        handlers.borrow_mut().insert(id, prepared);
    });
    Ok(id)
}

fn prepared_route_handler(id: u64) -> Option<PreparedCallable> {
    PREPARED_ROUTE_HANDLERS.with(|handlers| handlers.borrow().get(&id).cloned())
}

fn http_stats(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let metrics = lock_thread_pool_metrics().clone();
    let memory = memory_snapshot();
    let mut map = HashMap::new();
    map.insert("max_threads".into(), DgmValue::Int(metrics.max_threads as i64));
    map.insert("active_threads".into(), DgmValue::Int(metrics.running_jobs as i64));
    map.insert(
        "idle_threads".into(),
        DgmValue::Int(metrics.max_threads.saturating_sub(metrics.running_jobs) as i64),
    );
    map.insert("submitted_jobs".into(), DgmValue::Int(metrics.submitted_jobs.min(i64::MAX as u64) as i64));
    map.insert("completed_jobs".into(), DgmValue::Int(metrics.completed_jobs.min(i64::MAX as u64) as i64));
    map.insert("rejected_jobs".into(), DgmValue::Int(metrics.rejected_jobs.min(i64::MAX as u64) as i64));
    map.insert("pending_jobs".into(), DgmValue::Int(metrics.pending_jobs.min(i64::MAX as usize) as i64));
    map.insert("trim_supported".into(), DgmValue::Bool(memory.trim_supported));
    map.insert("trim_calls".into(), DgmValue::Int(memory.trim_calls.min(i64::MAX as u64) as i64));
    map.insert("trim_successes".into(), DgmValue::Int(memory.trim_successes.min(i64::MAX as u64) as i64));
    map.insert("trim_failures".into(), DgmValue::Int(memory.trim_failures.min(i64::MAX as u64) as i64));
    map.insert("idle_trim_calls".into(), DgmValue::Int(memory.idle_trim_calls.min(i64::MAX as u64) as i64));
    map.insert("periodic_trim_calls".into(), DgmValue::Int(memory.periodic_trim_calls.min(i64::MAX as u64) as i64));
    map.insert("manual_trim_calls".into(), DgmValue::Int(memory.manual_trim_calls.min(i64::MAX as u64) as i64));
    map.insert(
        "last_trim_elapsed_ms".into(),
        match memory.last_trim_elapsed_ms {
            Some(value) => DgmValue::Int(value.min(i64::MAX as u64) as i64),
            None => DgmValue::Null,
        },
    );
    map.insert("profile".into(), runtime_profile_to_value(&metrics.profile));
    Ok(DgmValue::Map(Rc::new(RefCell::new(map))))
}

fn http_reset_stats(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let max_threads = {
        let metrics = lock_thread_pool_metrics();
        metrics.max_threads
    };
    let mut metrics = lock_thread_pool_metrics();
    *metrics = ThreadPoolMetricsState {
        max_threads,
        ..ThreadPoolMetricsState::default()
    };
    reset_memory_snapshot();
    Ok(DgmValue::Null)
}

fn execute_request(method: &str, url: &str, body: Option<DgmValue>, options: Option<DgmValue>) -> Result<DgmValue, DgmError> {
    let opts = parse_options(body, options)?;
    let future = execute_request_future_with_options(method.to_string(), url.to_string(), opts)?;
    crate::concurrency::await_future(match &future {
        DgmValue::Future(state) => state,
        _ => return Err(DgmError::RuntimeError { msg: "internal async future expected".into() }),
    })
}

fn execute_request_future(method: &str, url: &str, body: Option<DgmValue>, options: Option<DgmValue>) -> Result<DgmValue, DgmError> {
    let opts = parse_options(body, options)?;
    execute_request_future_with_options(method.to_string(), url.to_string(), opts)
}

fn execute_request_future_with_options(method: String, url: String, opts: RequestOptions) -> Result<DgmValue, DgmError> {
    let label = format!("http.{} {}", method.to_ascii_lowercase(), url);
    schedule_named_future("http", label, move || execute_request_with_options(&method, &url, &opts))
}

fn execute_request_with_options(method: &str, url: &str, opts: &RequestOptions) -> Result<DgmValue, DgmError> {
    runtime_check_limits()?;
    runtime_begin_request()?;
    let started = Instant::now();
    let result = io_runtime::block_on(async_execute_request_with_options(method, url, opts));
    runtime_end_request();
    runtime_record_io(started.elapsed().as_nanos());
    result
}

fn serve_loop(port: u16, handler: ServerHandler, options: ServerOptions) -> Result<DgmValue, DgmError> {
    let started = Instant::now();
    let result = run_local_http_server(port, handler, options);
    runtime_record_io(started.elapsed().as_nanos());
    result
}

fn prepare_request_job(
    parsed: ParsedRequest,
    handler: &ServerHandler,
    max_body_size: usize,
) -> Result<RequestJob, DgmError> {
    match handler {
        ServerHandler::PreparedCallback(callback) => Ok(RequestJob::SpawnPrepared {
            handler: callback.clone(),
            request_payload: build_owned_request_payload(parsed, HashMap::new(), max_body_size)?,
        }),
        ServerHandler::Routes(routes) => {
            let method = parsed.method.clone();
            let path = parsed.path.clone();
            let (route_handler, params) = match find_route(routes, &method, &path)? {
                Some(matched) => matched,
                None => {
                    return Ok(RequestJob::Immediate(ServerResponse {
                        status: 404,
                        headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                        body: "not found".into(),
                    }));
                }
            };
            match route_handler {
                RouteHandler::Live(handler) => Ok(RequestJob::Spawn {
                    handler,
                    request_value: build_request_value(parsed, params, max_body_size)?,
                }),
                RouteHandler::Prepared(handler) => Ok(RequestJob::SpawnPrepared {
                    handler,
                    request_payload: build_owned_request_payload(parsed, params, max_body_size)?,
                }),
            }
        }
        ServerHandler::StaticRoutes(routes) => {
            let method = parsed.method.clone();
            let path = parsed.path.clone();
            let key = format!("{method} {path}");
            if let Some(value) = routes.get(&key).or_else(|| routes.get(&path)).cloned() {
                return normalize_server_response(value).map(RequestJob::Immediate);
            }
            return Ok(RequestJob::Immediate(ServerResponse {
                status: 404,
                headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                body: "not found".into(),
            }));
        }
    }
}

fn build_request_value(
    parsed: ParsedRequest,
    params: HashMap<String, String>,
    max_body_size: usize,
) -> Result<DgmValue, DgmError> {
    if parsed.body.len() > max_body_size {
        return Err(DgmError::RuntimeError { msg: format!("request body exceeds {} bytes", max_body_size) });
    }
    let headers = parsed
        .headers
        .into_iter()
        .map(|(key, value)| (normalize_header(&key), value))
        .collect::<HashMap<_, _>>();
    let value = request_value_from_parts(
        parsed.method,
        parsed.raw_url,
        parsed.path,
        headers,
        parse_query_string_map(&parsed.query),
        params,
        parsed.body,
    );
    runtime_reserve_value_labeled("http.request.value", &value)?;
    Ok(value)
}

fn build_owned_request_payload(
    parsed: ParsedRequest,
    params: HashMap<String, String>,
    max_body_size: usize,
) -> Result<OwnedPayload, DgmError> {
    if parsed.body.len() > max_body_size {
        return Err(DgmError::RuntimeError { msg: format!("request body exceeds {} bytes", max_body_size) });
    }
    let headers = parsed
        .headers
        .into_iter()
        .map(|(key, value)| (normalize_header(&key), value))
        .collect::<HashMap<_, _>>();
    Ok(owned_payload(OwnedValue::Request(RequestBacking::from_parts(
        parsed.method,
        parsed.raw_url,
        parsed.path,
        headers,
        parse_query_string_map(&parsed.query),
        params,
        parsed.body,
    ))))
}

fn run_local_http_server(port: u16, handler: ServerHandler, options: ServerOptions) -> Result<DgmValue, DgmError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|err| DgmError::RuntimeError { msg: format!("http.serve: {}", err) })?;
    let local = tokio::task::LocalSet::new();
    let handler = Rc::new(handler);
    let pool = Rc::new(ThreadPool::new(options.max_threads));
    runtime.block_on(local.run_until(async move { async_serve_loop(port, handler, pool, options).await }))
}

async fn async_serve_loop(
    port: u16,
    handler: Rc<ServerHandler>,
    pool: Rc<ThreadPool>,
    options: ServerOptions,
) -> Result<DgmValue, DgmError> {
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| DgmError::RuntimeError { msg: format!("http.serve: {}", e) })?;
    let accept_timeout = Duration::from_millis(options.timeout_ms.max(50));
    let connection_limit = Arc::new(Semaphore::new(options.max_connections.max(1)));
    let mut handled = 0usize;
    let track_tasks = options.max_requests.is_some();
    let mut tasks = Vec::new();

    loop {
        if let Some(max_requests) = options.max_requests {
            if handled >= max_requests {
                break;
            }
        }
        match tokio::time::timeout(accept_timeout, listener.accept()).await {
            Ok(Ok((stream, _))) => {
                handled += 1;
                if track_tasks && handled % 64 == 0 {
                    prune_finished_server_tasks(&mut tasks);
                }
                let permit = match Arc::clone(&connection_limit).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let timeout = options.write_timeout_ms.max(50);
                        let task = tokio::task::spawn_local(async move {
                            let response = ServerResponse {
                                status: 503,
                                headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                                body: "server busy".into(),
                            };
                            let _ = write_server_response(stream, response, timeout).await;
                        });
                        maybe_track_server_task(task, track_tasks, &mut tasks);
                        continue;
                    }
                };
                let task_handler = Rc::clone(&handler);
                let task_pool = Rc::clone(&pool);
                let task_options = options.clone();
                let task = tokio::task::spawn_local(async move {
                    let _ = handle_server_connection(stream, task_handler, task_pool, task_options, permit).await;
                });
                maybe_track_server_task(task, track_tasks, &mut tasks);
            }
            Ok(Err(err)) => return Err(DgmError::RuntimeError { msg: format!("http.serve: {}", err) }),
            Err(_) => continue,
        }
    }

    for task in tasks {
        let _ = task.await;
    }
    Ok(DgmValue::Null)
}

fn maybe_track_server_task(task: tokio::task::JoinHandle<()>, track: bool, tasks: &mut Vec<tokio::task::JoinHandle<()>>) {
    if track {
        tasks.push(task);
    }
}

fn prune_finished_server_tasks(tasks: &mut Vec<tokio::task::JoinHandle<()>>) {
    tasks.retain(|task| !task.is_finished());
}

async fn handle_server_connection(
    mut stream: TcpStream,
    handler: Rc<ServerHandler>,
    pool: Rc<ThreadPool>,
    options: ServerOptions,
    _permit: OwnedSemaphorePermit,
) -> Result<(), DgmError> {
    let response = match read_http_request(&mut stream, options.max_body_size, options.read_timeout_ms).await {
        Ok(parsed) => match prepare_request_job(parsed, &handler, options.max_body_size) {
            Ok(RequestJob::Immediate(response)) => response,
            Ok(RequestJob::Spawn { handler, request_value }) => {
                let prepared = prepare_isolated_call(handler, vec![request_value]);
                match prepared {
                    Ok(prepared) => {
                        let (tx, rx) = oneshot::channel();
                        pool.execute(move || {
                            let _request_scope = begin_request_scope();
                            let response = trap_dgm("http server request", || {
                                let result = run_prepared_call_for_response(prepared);
                                match result {
                                    Ok(value) => normalize_server_response(value),
                                    Err(err) => Err(err),
                                }
                            });
                            let response = match response {
                                Ok(response) => response,
                                Err(err) => {
                                    let (status, message) = classify_server_error(&err);
                                    ServerResponse {
                                        status,
                                        headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                                        body: message.into(),
                                    }
                                }
                            };
                            let _ = tx.send(response);
                        })?;
                        match tokio::time::timeout(Duration::from_millis(options.timeout_ms.max(1_000)), rx).await {
                            Ok(Ok(response)) => response,
                            Ok(Err(_)) => ServerResponse {
                                status: 500,
                                headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                                body: "internal server error".into(),
                            },
                            Err(_) => ServerResponse {
                                status: 504,
                                headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                                body: "request timed out".into(),
                            },
                        }
                    }
                    Err(err) => {
                        let (status, message) = classify_server_error(&err);
                        ServerResponse {
                            status,
                            headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                            body: message.into(),
                        }
                    }
                }
            }
            Ok(RequestJob::SpawnPrepared { handler, request_payload }) => {
                let (tx, rx) = oneshot::channel();
                pool.execute(move || {
                    let _request_scope = begin_request_scope();
                    let response = trap_dgm("http server request", || {
                        let result = run_prepared_callable(handler, vec![request_payload]);
                        match result {
                            Ok(value) => normalize_server_response(value),
                            Err(err) => Err(err),
                        }
                    });
                    let response = match response {
                        Ok(response) => response,
                        Err(err) => {
                            let (status, message) = classify_server_error(&err);
                            ServerResponse {
                                status,
                                headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                                body: message.into(),
                            }
                        }
                    };
                    let _ = tx.send(response);
                })?;
                match tokio::time::timeout(Duration::from_millis(options.timeout_ms.max(1_000)), rx).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(_)) => ServerResponse {
                        status: 500,
                        headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                        body: "internal server error".into(),
                    },
                    Err(_) => ServerResponse {
                        status: 504,
                        headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                        body: "request timed out".into(),
                    },
                }
            }
            Err(err) => {
                let (status, message) = classify_server_error(&err);
                ServerResponse {
                    status,
                    headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                    body: message.into(),
                }
            }
        },
        Err(err) => {
            let (status, message) = classify_server_error(&err);
            ServerResponse {
                status,
                headers: vec![("Content-Type".into(), "text/plain; charset=utf-8".into())],
                body: message.into(),
            }
        }
    };
    write_server_response(stream, response, options.write_timeout_ms).await
}

async fn write_server_response(
    mut stream: TcpStream,
    response: ServerResponse,
    timeout_ms: u64,
) -> Result<(), DgmError> {
    let (head, body) = serialize_server_response_parts(response)?;
    tokio::time::timeout(Duration::from_millis(timeout_ms.max(50)), stream.write_all(&head))
        .await
        .map_err(|_| DgmError::RuntimeError { msg: "http.serve: response write timed out".into() })?
        .map_err(|err| DgmError::RuntimeError { msg: format!("http.serve: {}", err) })?;
    match body {
        ServerBody::Owned(text) => {
            tokio::time::timeout(Duration::from_millis(timeout_ms.max(50)), stream.write_all(text.as_bytes()))
                .await
                .map_err(|_| DgmError::RuntimeError { msg: "http.serve: response write timed out".into() })?
                .map_err(|err| DgmError::RuntimeError { msg: format!("http.serve: {}", err) })?;
        }
        ServerBody::JsonBytes(bytes) => {
            tokio::time::timeout(Duration::from_millis(timeout_ms.max(50)), stream.write_all(bytes.as_slice()))
                .await
                .map_err(|_| DgmError::RuntimeError { msg: "http.serve: response write timed out".into() })?
                .map_err(|err| DgmError::RuntimeError { msg: format!("http.serve: {}", err) })?;
        }
        ServerBody::RawJson(raw) => {
            for segment in raw.segments() {
                tokio::time::timeout(Duration::from_millis(timeout_ms.max(50)), stream.write_all(segment.as_bytes()))
                    .await
                    .map_err(|_| DgmError::RuntimeError { msg: "http.serve: response write timed out".into() })?
                    .map_err(|err| DgmError::RuntimeError { msg: format!("http.serve: {}", err) })?;
            }
        }
    }
    let _ = stream.shutdown().await;
    Ok(())
}

async fn read_http_request(
    stream: &mut TcpStream,
    max_body_size: usize,
    timeout_ms: u64,
) -> Result<ParsedRequest, DgmError> {
    const MAX_HEADER_BYTES: usize = 64 * 1024;
    let timeout = Duration::from_millis(timeout_ms.max(50));
    let mut buffer = Vec::new();
    let mut header_end = None;
    while header_end.is_none() {
        if buffer.len() > MAX_HEADER_BYTES {
            return Err(DgmError::RuntimeError { msg: "invalid request header".into() });
        }
        let mut chunk = [0u8; 4096];
        let read = tokio::time::timeout(timeout, stream.read(&mut chunk))
            .await
            .map_err(|_| DgmError::RuntimeError { msg: "http.serve: request timed out".into() })?
            .map_err(|err| DgmError::RuntimeError { msg: format!("http.serve: {}", err) })?;
        if read == 0 {
            return Err(DgmError::RuntimeError { msg: "invalid request header".into() });
        }
        buffer.extend_from_slice(&chunk[..read]);
        header_end = find_header_end(&buffer);
    }
    let header_end = header_end.unwrap();
    let headers_text = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let mut lines = headers_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| DgmError::RuntimeError { msg: "invalid request header".into() })?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| DgmError::RuntimeError { msg: "invalid request header".into() })?
        .to_ascii_uppercase();
    let raw_url = request_parts
        .next()
        .ok_or_else(|| DgmError::RuntimeError { msg: "invalid request header".into() })?
        .to_string();
    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| DgmError::RuntimeError { msg: "invalid request header".into() })?;
        let value = value.trim();
        sanitize_header_pair(name.trim(), value).map_err(|_| DgmError::RuntimeError { msg: "invalid request header".into() })?;
        if name.trim().eq_ignore_ascii_case("content-length") {
            content_length = value
                .parse::<usize>()
                .map_err(|_| DgmError::RuntimeError { msg: "invalid request header".into() })?;
        }
        headers.insert(name.trim().to_string(), value.to_string());
    }
    if content_length > max_body_size {
        return Err(DgmError::RuntimeError { msg: format!("request body exceeds {} bytes", max_body_size) });
    }
    let mut body_bytes = buffer[header_end + 4..].to_vec();
    while body_bytes.len() < content_length {
        let remaining = content_length - body_bytes.len();
        let mut chunk = vec![0u8; remaining.min(4096)];
        let read = tokio::time::timeout(timeout, stream.read(&mut chunk))
            .await
            .map_err(|_| DgmError::RuntimeError { msg: "http.serve: request timed out".into() })?
            .map_err(|err| DgmError::RuntimeError { msg: format!("http.serve: {}", err) })?;
        if read == 0 {
            return Err(DgmError::RuntimeError { msg: "invalid request body".into() });
        }
        body_bytes.extend_from_slice(&chunk[..read]);
        if body_bytes.len() > max_body_size {
            return Err(DgmError::RuntimeError { msg: format!("request body exceeds {} bytes", max_body_size) });
        }
    }
    body_bytes.truncate(content_length);
    let (path, query) = split_url(&raw_url);
    Ok(ParsedRequest {
        method,
        raw_url,
        path,
        query,
        headers,
        body: String::from_utf8_lossy(&body_bytes).to_string(),
    })
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn serialize_server_response_parts(mut response: ServerResponse) -> Result<(Vec<u8>, ServerBody), DgmError> {
    let body_len = response.body.len();
    let has_content_length = response
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
    let has_connection = response
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("connection"));
    if !has_content_length {
        response.headers.push(("Content-Length".into(), body_len.to_string()));
    }
    if !has_connection {
        response.headers.push(("Connection".into(), "close".into()));
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        format!(
            "HTTP/1.1 {} {}\r\n",
            response.status,
            status_text(response.status)
        )
        .as_bytes(),
    );
    for (name, value) in response.headers {
        let (name, value) = sanitize_header_pair(&name, &value)?;
        bytes.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    bytes.extend_from_slice(b"\r\n");
    Ok((bytes, response.body))
}

fn status_text(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        500 => "Internal Server Error",
        504 => "Gateway Timeout",
        _ => "OK",
    }
}

async fn async_execute_request_with_options(method: &str, url: &str, opts: &RequestOptions) -> Result<DgmValue, DgmError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(opts.timeout_ms))
        .connect_timeout(Duration::from_millis(opts.connect_timeout_ms))
        .redirect(Policy::limited(opts.max_redirects as usize))
        .build()
        .map_err(|err| DgmError::RuntimeError { msg: format!("http.{method}: {}", err) })?;
    let attempts = opts.retries.max(1);
    for attempt in 0..attempts {
        match perform_async_request(&client, method, url, opts).await {
            Ok(response) => return make_client_response_async(method, url, response, opts.max_body_size).await,
            Err(err) => {
                let should_retry = is_retryable_request_error(&err) && attempt + 1 < attempts;
                if should_retry {
                    tokio::time::sleep(retry_delay(opts, attempt)).await;
                    continue;
                }
                return Err(DgmError::RuntimeError { msg: format_request_error(method, &err) });
            }
        }
    }
    Err(DgmError::RuntimeError { msg: format!("http.{method}: request failed") })
}

async fn perform_async_request(
    client: &reqwest::Client,
    method: &str,
    url: &str,
    opts: &RequestOptions,
) -> Result<reqwest::Response, reqwest::Error> {
    let method = reqwest::Method::from_bytes(method.as_bytes())
        .unwrap_or(reqwest::Method::GET);
    let mut request = client.request(method, url);
    for (key, value) in &opts.headers {
        request = request.header(key, value);
    }
    if !opts.query.is_empty() {
        request = request.query(&opts.query);
    }
    if let Some(body) = &opts.body {
        request = request.body(body.clone());
    }
    request.send().await
}

async fn make_client_response_async(
    method: &str,
    url: &str,
    response: reqwest::Response,
    max_body_size: usize,
) -> Result<DgmValue, DgmError> {
    let status = response.status().as_u16();
    let mut headers = HashMap::new();
    for (name, value) in response.headers() {
        if let Ok(value) = value.to_str() {
            headers.insert(normalize_header(name.as_str()), DgmValue::Str(value.to_string()));
        }
    }
    let content_type = headers
        .get("content_type")
        .and_then(|value| match value { DgmValue::Str(text) => Some(text.clone()), _ => None })
        .unwrap_or_default();
    let body = read_async_body_limited(response, max_body_size, method).await?;
    let json = if content_type.contains("application/json") || content_type.contains("+json") {
        parse_json_limited(&body, &format!("http.{method} json"))
            .ok()
            .map(|value| {
                let out = json_to_dgm(&value);
                let _ = runtime_reserve_value_labeled("http.response.json", &out);
                out
            })
            .unwrap_or(DgmValue::Null)
    } else {
        DgmValue::Null
    };
    let mut map = HashMap::new();
    map.insert("status".into(), DgmValue::Int(status as i64));
    map.insert("body".into(), DgmValue::Str(body));
    map.insert("ok".into(), DgmValue::Bool((200..300).contains(&status)));
    map.insert("headers".into(), DgmValue::Map(Rc::new(RefCell::new(headers))));
    map.insert("method".into(), DgmValue::Str(method.to_string()));
    map.insert("url".into(), DgmValue::Str(url.to_string()));
    map.insert("json".into(), json);
    let out = DgmValue::Map(Rc::new(RefCell::new(map)));
    runtime_reserve_value_labeled("http.response.value", &out)?;
    Ok(out)
}

async fn read_async_body_limited(
    mut response: reqwest::Response,
    max_body_size: usize,
    method: &str,
) -> Result<String, DgmError> {
    if let Some(length) = response.content_length() {
        if length as usize > max_body_size {
            return Err(DgmError::RuntimeError { msg: format!("http.{method}: response exceeds {} bytes", max_body_size) });
        }
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|err| DgmError::RuntimeError { msg: format!("http.{method}: {}", err) })?
    {
        bytes.extend_from_slice(&chunk);
        if bytes.len() > max_body_size {
            return Err(DgmError::RuntimeError { msg: format!("http.{method}: response exceeds {} bytes", max_body_size) });
        }
    }
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

pub(crate) fn request_json_method(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "req.json() does not take arguments".into() });
    }
    if let DgmValue::RequestJson(state) = state {
        if let Some(cached) = state.parsed.borrow().as_ref() {
            return cached
                .clone()
                .map_err(|error| DgmError::RuntimeError { msg: error });
        }
        let parsed = if state.body.trim().is_empty() {
            Ok(DgmValue::Null)
        } else if state.content_type.contains("application/json") || state.content_type.contains("+json") {
            match parse_json_limited(&state.body, "http.request json") {
                Ok(value) => {
                    let out = json_to_dgm(&value);
                    runtime_reserve_value_labeled("http.request.json", &out)?;
                    Ok(out)
                }
                Err(_) => Err("invalid json body".into()),
            }
        } else {
            Err("request content-type is not json".into())
        };
        *state.parsed.borrow_mut() = Some(parsed.clone());
        return parsed.map_err(|error| DgmError::RuntimeError { msg: error });
    }
    let DgmValue::Map(state) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid request json state".into() });
    };
    let state = state.borrow();
    if let Some(DgmValue::Str(error)) = state.get("error") {
        return Err(DgmError::RuntimeError { msg: error.clone() });
    }
    Ok(state.get("value").cloned().unwrap_or(DgmValue::Null))
}

fn normalize_server_response(value: DgmValue) -> Result<ServerResponse, DgmError> {
    match value {
        DgmValue::Map(map) => {
            let map = map.borrow();
            let status = match map.get("status") {
                Some(DgmValue::Int(status)) if *status > 0 && *status <= u16::MAX as i64 => *status as u16,
                Some(DgmValue::Int(_)) => return Err(DgmError::RuntimeError { msg: "response status must be between 1 and 65535".into() }),
                Some(other) => return Err(DgmError::RuntimeError { msg: format!("response status must be int, got {}", other) }),
                None => 200,
            };

            let mut headers = match map.get("headers") {
                Some(value) => map_to_string_pairs(value, "headers")?
                    .into_iter()
                    .map(|(name, value)| sanitize_header_pair(&name, &value))
                    .collect::<Result<Vec<_>, _>>()?,
                None => vec![],
            };

            let body_value = map.get("body").cloned().unwrap_or(DgmValue::Null);
            let body = match &body_value {
                DgmValue::Null => ServerBody::Owned(String::new()),
                DgmValue::Str(text) => ServerBody::Owned(text.clone()),
                DgmValue::RawJson(text) => {
                    if !headers.iter().any(|(key, _)| key.eq_ignore_ascii_case("content-type")) {
                        headers.push(("Content-Type".into(), "application/json".into()));
                    }
                    mark_response_owned_raw_json(text);
                    ServerBody::RawJson(Arc::clone(text))
                }
                DgmValue::Map(_) | DgmValue::List(_) | DgmValue::Bool(_) | DgmValue::Int(_) | DgmValue::Float(_) | DgmValue::Json(_) => {
                    if !headers.iter().any(|(key, _)| key.eq_ignore_ascii_case("content-type")) {
                        headers.push(("Content-Type".into(), "application/json".into()));
                    }
                    ServerBody::JsonBytes(PooledJsonBody::encode(&body_value)?)
                }
                other => ServerBody::Owned(format!("{}", other)),
            };

            Ok(ServerResponse { status, headers, body })
        }
        DgmValue::RawJson(text) => Ok(ServerResponse {
            status: 200,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: {
                mark_response_owned_raw_json(&text);
                ServerBody::RawJson(text)
            },
        }),
        DgmValue::Str(text) => Ok(ServerResponse {
            status: 200,
            headers: vec![],
            body: ServerBody::Owned(text),
        }),
        DgmValue::Null => Ok(ServerResponse {
            status: 204,
            headers: vec![],
            body: ServerBody::Owned(String::new()),
        }),
        other => Ok(ServerResponse {
            status: 200,
            headers: vec![],
            body: ServerBody::Owned(format!("{}", other)),
        }),
    }
}

fn classify_server_error(err: &DgmError) -> (u16, String) {
    match err {
        DgmError::RuntimeError { msg } if msg == "invalid json body" => (400, "invalid json body".into()),
        DgmError::RuntimeError { msg } if msg == "request content-type is not json" => (415, "request content-type is not json".into()),
        DgmError::RuntimeError { msg } if msg == "invalid request header" => (400, "invalid request header".into()),
        DgmError::RuntimeError { msg } if msg == "invalid request body" => (400, "invalid request body".into()),
        DgmError::RuntimeError { msg } if msg.contains("timed out") => (504, msg.clone()),
        DgmError::RuntimeError { msg } if msg.starts_with("request body exceeds ") => (413, msg.clone()),
        _ => (500, "internal server error".into()),
    }
}

fn router_get(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    router_add(state, "GET", args)
}

fn router_post(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    router_add(state, "POST", args)
}

fn router_put(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    router_add(state, "PUT", args)
}

fn router_delete(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    router_add(state, "DELETE", args)
}

fn router_add(state: DgmValue, method: &str, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let routes = extract_routes(state)?;
    let pattern = match args.first() {
        Some(DgmValue::Str(pattern)) => pattern.clone(),
        _ => return Err(DgmError::RuntimeError { msg: "route pattern must be string".into() }),
    };
    let handler = args.get(1).cloned().ok_or_else(|| DgmError::RuntimeError { msg: "route handler required".into() })?;
    if !is_callable(&handler) {
        return Err(DgmError::RuntimeError { msg: "route handler must be callable".into() });
    }
    let prepared_handler_id = cache_prepared_route_handler(&handler)?;
    let mut route = HashMap::new();
    route.insert("method".into(), DgmValue::Str(method.into()));
    route.insert("pattern".into(), DgmValue::Str(pattern));
    route.insert("handler".into(), handler);
    route.insert("prepared_handler_id".into(), DgmValue::Int(prepared_handler_id as i64));
    let value = DgmValue::Map(Rc::new(RefCell::new(route)));
    runtime_reserve_value(&value)?;
    routes.borrow_mut().push(value);
    Ok(DgmValue::Null)
}

fn router_listen(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let routes = extract_routes(state)?;
    let port = match args.first() {
        Some(DgmValue::Int(port)) if *port > 0 => *port as u16,
        _ => return Err(DgmError::RuntimeError { msg: "app.listen(port, options?) requires positive int port".into() }),
    };
    let options = parse_server_options(args.get(1).cloned())?;
    serve_loop(port, ServerHandler::Routes(routes), options)
}

fn extract_routes(state: DgmValue) -> Result<Rc<RefCell<Vec<DgmValue>>>, DgmError> {
    match state {
        DgmValue::List(routes) => Ok(routes),
        _ => Err(DgmError::RuntimeError { msg: "invalid router state".into() }),
    }
}

fn find_route(routes: &Rc<RefCell<Vec<DgmValue>>>, method: &str, path: &str) -> Result<Option<(RouteHandler, HashMap<String, String>)>, DgmError> {
    let routes = routes.borrow();
    for route in routes.iter() {
        let DgmValue::Map(map) = route else {
            continue;
        };
        let map = map.borrow();
        let route_method = match map.get("method") {
            Some(DgmValue::Str(text)) => text.as_str(),
            _ => continue,
        };
        if route_method != method {
            continue;
        }
        let pattern = match map.get("pattern") {
            Some(DgmValue::Str(text)) => text.as_str(),
            _ => continue,
        };
        if let Some(params) = match_route(pattern, path) {
            let handler = if let Some(DgmValue::Int(id)) = map.get("prepared_handler_id") {
                if *id > 0 {
                    if let Some(prepared) = prepared_route_handler(*id as u64) {
                        RouteHandler::Prepared(prepared)
                    } else {
                        let live = map.get("handler").cloned()
                            .ok_or_else(|| DgmError::RuntimeError { msg: "route missing handler".into() })?;
                        RouteHandler::Live(live)
                    }
                } else {
                    let live = map.get("handler").cloned()
                        .ok_or_else(|| DgmError::RuntimeError { msg: "route missing handler".into() })?;
                    RouteHandler::Live(live)
                }
            } else {
                let live = map.get("handler").cloned()
                    .ok_or_else(|| DgmError::RuntimeError { msg: "route missing handler".into() })?;
                RouteHandler::Live(live)
            };
            return Ok(Some((handler, params)));
        }
    }
    Ok(None)
}

fn match_route(pattern: &str, path: &str) -> Option<HashMap<String, String>> {
    if pattern == path {
        return Some(HashMap::new());
    }

    if let Some(prefix) = pattern.strip_suffix('*') {
        let base = prefix.trim_end_matches('/');
        let path = path.trim_end_matches('/');
        if path == base {
            return Some(HashMap::from([("wildcard".into(), String::new())]));
        }
        let prefix = format!("{base}/");
        if let Some(rest) = path.strip_prefix(&prefix) {
            return Some(HashMap::from([("wildcard".into(), percent_decode(rest))]));
        }
        return None;
    }

    let pattern_parts = route_segments(pattern);
    let path_parts = route_segments(path);
    if pattern_parts.len() != path_parts.len() {
        return None;
    }

    let mut params = HashMap::new();
    for (pattern_part, path_part) in pattern_parts.iter().zip(path_parts.iter()) {
        if let Some(name) = pattern_part.strip_prefix(':') {
            params.insert(name.to_string(), percent_decode(path_part));
            continue;
        }
        if pattern_part != path_part {
            return None;
        }
    }
    Some(params)
}

fn route_segments(path: &str) -> Vec<&str> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        vec![]
    } else {
        trimmed.split('/').collect()
    }
}

fn split_url(url: &str) -> (String, String) {
    match url.split_once('?') {
        Some((path, query)) => (path.to_string(), query.to_string()),
        None => (url.to_string(), String::new()),
    }
}

fn parse_query_string_map(query: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if query.is_empty() {
        return out;
    }
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((key, value)) => (percent_decode(key), percent_decode(value)),
            None => (percent_decode(pair), String::new()),
        };
        out.insert(key, value);
    }
    out
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_value(bytes[i + 1]);
                let lo = hex_value(bytes[i + 2]);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as char);
                    i += 3;
                } else {
                    out.push('%');
                    i += 1;
                }
            }
            b => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    out
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_options(body: Option<DgmValue>, options: Option<DgmValue>) -> Result<RequestOptions, DgmError> {
    let mut headers = vec![];
    let mut query = vec![];
    let mut timeout_ms = DEFAULT_TIMEOUT_MS;
    let mut connect_timeout_ms = DEFAULT_TIMEOUT_MS;
    let mut max_body_size = DEFAULT_MAX_BODY_SIZE;
    let mut max_redirects = DEFAULT_MAX_REDIRECTS;
    let mut retries = DEFAULT_RETRIES;
    let mut retry_backoff_ms = DEFAULT_RETRY_BACKOFF_MS;
    let mut retry_backoff_max_ms = DEFAULT_RETRY_BACKOFF_MAX_MS;
    let mut payload = body;

    if let Some(DgmValue::Map(map)) = options {
        let map = map.borrow();
        if let Some(value) = map.get("headers") {
            headers = map_to_string_pairs(value, "headers")?
                .into_iter()
                .map(|(name, value)| sanitize_header_pair(&name, &value))
                .collect::<Result<Vec<_>, _>>()?;
        }
        if let Some(value) = map.get("query") {
            query = map_to_string_pairs(value, "query")?;
        }
        if let Some(value) = map.get("timeout_ms") {
            timeout_ms = parse_positive_u64(value, "timeout_ms")?;
        }
        if let Some(value) = map.get("connect_timeout_ms") {
            connect_timeout_ms = parse_positive_u64(value, "connect_timeout_ms")?;
        }
        if let Some(value) = map.get("max_body_size") {
            max_body_size = parse_positive_usize(value, "max_body_size")?;
        }
        if let Some(value) = map.get("max_redirects") {
            max_redirects = parse_non_negative_u32(value, "max_redirects")?;
        }
        if let Some(value) = map.get("retries") {
            retries = parse_positive_u32(value, "retries")?;
        }
        if let Some(value) = map.get("retry_backoff_ms") {
            retry_backoff_ms = parse_positive_u64(value, "retry_backoff_ms")?;
        }
        if let Some(value) = map.get("retry_backoff_max_ms") {
            retry_backoff_max_ms = parse_positive_u64(value, "retry_backoff_max_ms")?;
        }
        if let Some(value) = map.get("json") {
            payload = Some(value.clone());
            if !headers.iter().any(|(key, _)| key.eq_ignore_ascii_case("content-type")) {
                headers.push(("Content-Type".into(), "application/json".into()));
            }
        }
        if let Some(value) = map.get("body") {
            payload = Some(value.clone());
        }
    } else if let Some(value) = options {
        return Err(DgmError::RuntimeError { msg: format!("http options must be map, got {}", value) });
    }

    let body = match payload {
        None => None,
        Some(DgmValue::Str(text)) => Some(text),
        Some(value @ DgmValue::Map(_))
        | Some(value @ DgmValue::List(_))
        | Some(value @ DgmValue::Bool(_))
        | Some(value @ DgmValue::Null)
        | Some(value @ DgmValue::Int(_))
        | Some(value @ DgmValue::Float(_)) => {
            if !headers.iter().any(|(key, _)| key.eq_ignore_ascii_case("content-type")) {
                headers.push(("Content-Type".into(), "application/json".into()));
            }
            Some(dgm_to_json_string(&value)?)
        }
        Some(other) => Some(format!("{}", other)),
    };

    Ok(RequestOptions {
        headers,
        query,
        body,
        timeout_ms,
        connect_timeout_ms,
        max_body_size,
        max_redirects,
        retries,
        retry_backoff_ms,
        retry_backoff_max_ms: retry_backoff_max_ms.max(retry_backoff_ms),
    })
}

fn parse_server_options(options: Option<DgmValue>) -> Result<ServerOptions, DgmError> {
    let mut timeout_ms = DEFAULT_TIMEOUT_MS;
    let mut read_timeout_ms = DEFAULT_TIMEOUT_MS;
    let mut write_timeout_ms = DEFAULT_TIMEOUT_MS;
    let mut max_body_size = DEFAULT_MAX_BODY_SIZE;
    let mut max_requests = None;
    let mut max_threads = default_max_threads();
    let mut max_connections = default_max_connections(max_threads);
    if let Some(DgmValue::Map(map)) = options {
        let map = map.borrow();
        if let Some(value) = map.get("timeout_ms") {
            timeout_ms = parse_positive_u64(value, "timeout_ms")?;
            read_timeout_ms = timeout_ms;
            write_timeout_ms = timeout_ms;
        }
        if let Some(value) = map.get("read_timeout_ms") {
            read_timeout_ms = parse_positive_u64(value, "read_timeout_ms")?;
        }
        if let Some(value) = map.get("write_timeout_ms") {
            write_timeout_ms = parse_positive_u64(value, "write_timeout_ms")?;
        }
        if let Some(value) = map.get("max_body_size") {
            max_body_size = parse_positive_usize(value, "max_body_size")?;
        }
        if let Some(value) = map.get("max_requests") {
            max_requests = Some(parse_positive_usize(value, "max_requests")?);
        }
        if let Some(value) = map.get("max_threads") {
            max_threads = parse_positive_usize(value, "max_threads")?;
            max_connections = default_max_connections(max_threads);
        }
        if let Some(value) = map.get("max_connections") {
            max_connections = parse_positive_usize(value, "max_connections")?;
        }
    } else if let Some(value) = options {
        return Err(DgmError::RuntimeError { msg: format!("http server options must be map, got {}", value) });
    }
    Ok(ServerOptions {
        timeout_ms,
        read_timeout_ms,
        write_timeout_ms,
        max_body_size,
        max_requests,
        max_threads,
        max_connections: max_connections.max(1),
    })
}

fn parse_positive_u64(value: &DgmValue, field: &str) -> Result<u64, DgmError> {
    match value {
        DgmValue::Int(n) if *n > 0 => Ok(*n as u64),
        DgmValue::Int(_) => Err(DgmError::RuntimeError { msg: format!("http option '{}' must be > 0", field) }),
        other => Err(DgmError::RuntimeError { msg: format!("http option '{}' must be int, got {}", field, other) }),
    }
}

fn parse_positive_usize(value: &DgmValue, field: &str) -> Result<usize, DgmError> {
    match value {
        DgmValue::Int(n) if *n > 0 => Ok(*n as usize),
        DgmValue::Int(_) => Err(DgmError::RuntimeError { msg: format!("http option '{}' must be > 0", field) }),
        other => Err(DgmError::RuntimeError { msg: format!("http option '{}' must be int, got {}", field, other) }),
    }
}

fn parse_non_negative_u32(value: &DgmValue, field: &str) -> Result<u32, DgmError> {
    match value {
        DgmValue::Int(n) if *n >= 0 => Ok(*n as u32),
        DgmValue::Int(_) => Err(DgmError::RuntimeError { msg: format!("http option '{}' must be >= 0", field) }),
        other => Err(DgmError::RuntimeError { msg: format!("http option '{}' must be int, got {}", field, other) }),
    }
}

fn parse_positive_u32(value: &DgmValue, field: &str) -> Result<u32, DgmError> {
    match value {
        DgmValue::Int(n) if *n > 0 => Ok(*n as u32),
        DgmValue::Int(_) => Err(DgmError::RuntimeError { msg: format!("http option '{}' must be > 0", field) }),
        other => Err(DgmError::RuntimeError { msg: format!("http option '{}' must be int, got {}", field, other) }),
    }
}

fn map_to_string_pairs(value: &DgmValue, name: &str) -> Result<Vec<(String, String)>, DgmError> {
    match value {
        DgmValue::Map(map) => {
            let mut pairs = vec![];
            for (key, value) in map.borrow().iter() {
                pairs.push((key.clone(), match value {
                    DgmValue::Str(s) => s.clone(),
                    other => format!("{}", other),
                }));
            }
            Ok(pairs)
        }
        _ => Err(DgmError::RuntimeError { msg: format!("http options '{}' must be map", name) }),
    }
}

fn sanitize_header_pair(name: &str, value: &str) -> Result<(String, String), DgmError> {
    if name.trim().is_empty() || name.contains(':') || contains_header_injection(name) || contains_header_injection(value) {
        return Err(DgmError::RuntimeError { msg: format!("http: invalid header '{}'", name) });
    }
    Ok((name.to_string(), value.to_string()))
}

fn contains_header_injection(value: &str) -> bool {
    value.contains('\n') || value.contains('\r')
}

fn normalize_header(name: &str) -> String {
    name.to_ascii_lowercase().replace('-', "_")
}

fn is_option_map(value: &DgmValue) -> bool {
    match value {
        DgmValue::Map(map) => map.borrow().keys().any(|key| matches!(
            key.as_str(),
            "headers" | "query" | "timeout_ms" | "connect_timeout_ms" | "json" | "body" | "max_body_size"
                | "max_redirects" | "retries" | "retry_backoff_ms" | "retry_backoff_max_ms"
        )),
        _ => false,
    }
}

fn default_max_connections(max_threads: usize) -> usize {
    max_threads
        .max(1)
        .saturating_mul(DEFAULT_MAX_CONNECTIONS_MULTIPLIER)
        .max(1)
}

fn retry_delay(opts: &RequestOptions, attempt: u32) -> Duration {
    let multiplier = 1u64.checked_shl(attempt.min(20)).unwrap_or(u64::MAX);
    let millis = opts
        .retry_backoff_ms
        .saturating_mul(multiplier)
        .min(opts.retry_backoff_max_ms)
        .max(1);
    Duration::from_millis(millis)
}

fn is_callable(value: &DgmValue) -> bool {
    matches!(value, DgmValue::Function { .. } | DgmValue::NativeFunction { .. } | DgmValue::NativeMethod { .. })
}

fn is_retryable_request_error(err: &reqwest::Error) -> bool {
    err.is_timeout() || err.is_connect() || err.is_request() || err.is_body()
}

fn format_request_error(method: &str, err: &reqwest::Error) -> String {
    if err.is_timeout() {
        format!("http.{method}: timeout")
    } else if err.is_connect() {
        format!("http.{method}: Connection Failed: {}", err)
    } else {
        format!("http.{method}: {}", err)
    }
}

fn default_max_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_mul(DEFAULT_MAX_THREADS_MULTIPLIER))
        .unwrap_or(4)
        .min(DEFAULT_MAX_THREADS_CAP)
        .max(1)
}

impl ThreadPool {
    fn new(size: usize) -> Self {
        let size = size.max(1);
        let (sender, receiver) = mpsc::channel::<PoolMessage>();
        let shared = Arc::new(Mutex::new(receiver));
        {
            let mut metrics = lock_thread_pool_metrics();
            *metrics = ThreadPoolMetricsState {
                max_threads: size,
                ..ThreadPoolMetricsState::default()
            };
        }
        let mut workers = Vec::with_capacity(size);
        for index in 0..size {
            let receiver = Arc::clone(&shared);
            let name = format!("dgm-http-worker-{}", index + 1);
            workers.push(spawn_http_worker(name, receiver));
        }
        Self { sender: Some(sender), workers }
    }

    fn execute<F>(&self, job: F) -> Result<(), DgmError>
    where
        F: FnOnce() + Send + 'static,
    {
        let sender = self.sender.as_ref()
            .ok_or_else(|| DgmError::RuntimeError { msg: "server worker pool is closed".into() })?;
        {
            let mut metrics = lock_thread_pool_metrics();
            metrics.submitted_jobs = metrics.submitted_jobs.saturating_add(1);
            metrics.pending_jobs = metrics.pending_jobs.saturating_add(1);
        }
        sender.send(PoolMessage::Run(Box::new(job)))
            .map_err(|_| {
                let mut metrics = lock_thread_pool_metrics();
                metrics.pending_jobs = metrics.pending_jobs.saturating_sub(1);
                metrics.rejected_jobs = metrics.rejected_jobs.saturating_add(1);
                DgmError::RuntimeError { msg: "server worker pool is closed".into() }
            })
    }
}

fn spawn_http_worker(name: String, receiver: Arc<Mutex<mpsc::Receiver<PoolMessage>>>) -> JoinHandle<()> {
    let fallback_receiver = Arc::clone(&receiver);
    thread::Builder::new()
        .name(name)
        .stack_size(SERVER_WORKER_STACK_SIZE)
        .spawn(move || http_worker_loop(receiver))
        .unwrap_or_else(|_| thread::spawn(move || http_worker_loop(fallback_receiver)))
}

fn http_worker_loop(receiver: Arc<Mutex<mpsc::Receiver<PoolMessage>>>) {
    loop {
        let message = receiver.lock().unwrap().recv();
        match message {
            Ok(PoolMessage::Run(job)) => {
                {
                    let mut metrics = lock_thread_pool_metrics();
                    metrics.pending_jobs = metrics.pending_jobs.saturating_sub(1);
                    metrics.running_jobs = metrics.running_jobs.saturating_add(1);
                }
                let _ = catch_unwind(AssertUnwindSafe(job));
                let profile = runtime_profile_snapshot();
                merge_thread_pool_profile(&profile);
                runtime_reset_usage();
                let mut metrics = lock_thread_pool_metrics();
                metrics.running_jobs = metrics.running_jobs.saturating_sub(1);
                metrics.completed_jobs = metrics.completed_jobs.saturating_add(1);
                let should_trim_idle = metrics.pending_jobs == 0 && metrics.running_jobs == 0;
                let should_trim_periodic = metrics.completed_jobs % 64 == 0;
                drop(metrics);
                if should_trim_idle {
                    let _ = maybe_trim_allocator(TrimTrigger::Idle);
                } else if should_trim_periodic {
                    let _ = maybe_trim_allocator(TrimTrigger::Periodic);
                }
            }
            Ok(PoolMessage::Stop) | Err(_) => break,
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            for _ in 0..self.workers.len() {
                let _ = sender.send(PoolMessage::Stop);
            }
        }
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
        *lock_thread_pool_metrics() = ThreadPoolMetricsState::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_scope::{begin_request_scope, request_scope_snapshot, reset_request_scope_stats, track_raw_json_ref};

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        match LOCK.get_or_init(|| Mutex::new(())).lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn dgm_map(entries: &[(&str, DgmValue)]) -> DgmValue {
        let map = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect::<HashMap<_, _>>();
        DgmValue::Map(Rc::new(RefCell::new(map)))
    }

    #[test]
    fn unbounded_server_mode_does_not_retain_request_join_handles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let mut tasks = Vec::new();
            let task = tokio::task::spawn_local(async {});
            maybe_track_server_task(task, false, &mut tasks);
            tokio::task::yield_now().await;
            assert!(tasks.is_empty());
        }));
    }

    #[test]
    fn bounded_server_mode_prunes_completed_request_join_handles() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async {
            let mut tasks = Vec::new();
            let task = tokio::task::spawn_local(async {});
            maybe_track_server_task(task, true, &mut tasks);
            tokio::task::yield_now().await;
            prune_finished_server_tasks(&mut tasks);
            assert!(tasks.is_empty());
        }));
    }

    #[test]
    fn parse_options_apply_secure_defaults() {
        let opts = parse_options(None, None).unwrap();
        assert_eq!(opts.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(opts.connect_timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(opts.max_body_size, DEFAULT_MAX_BODY_SIZE);
        assert_eq!(opts.max_redirects, DEFAULT_MAX_REDIRECTS);
        assert_eq!(opts.retries, DEFAULT_RETRIES);
        assert_eq!(opts.retry_backoff_ms, DEFAULT_RETRY_BACKOFF_MS);
        assert_eq!(opts.retry_backoff_max_ms, DEFAULT_RETRY_BACKOFF_MAX_MS);
    }

    #[test]
    fn parse_server_options_apply_connection_defaults() {
        let opts = parse_server_options(None).unwrap();
        assert_eq!(opts.timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(opts.read_timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(opts.write_timeout_ms, DEFAULT_TIMEOUT_MS);
        assert_eq!(opts.max_connections, default_max_connections(opts.max_threads));
    }

    #[test]
    fn parse_options_reject_header_injection() {
        let options = dgm_map(&[
            ("headers", dgm_map(&[("Authorization", DgmValue::Str("Bearer abc\nhack".into()))])),
        ]);
        let err = parse_options(None, Some(options)).unwrap_err();
        assert!(format!("{}", err).contains("invalid header"));
    }

    #[test]
    fn route_matching_supports_exact_param_and_wildcard() {
        assert_eq!(match_route("/api", "/api").unwrap().len(), 0);
        assert_eq!(match_route("/user/:id", "/user/42").unwrap().get("id").unwrap(), "42");
        assert_eq!(match_route("/static/*", "/static/css/app.css").unwrap().get("wildcard").unwrap(), "css/app.css");
        assert!(match_route("/user/:id", "/user").is_none());
    }

    #[test]
    fn raw_json_response_transfer_does_not_count_as_request_scope_survivor() {
        let _guard = test_lock();
        reset_request_scope_stats();

        let response;
        {
            let _scope = begin_request_scope();
            let raw = Arc::new(crate::interpreter::RawJsonValue::from_text("{\"ok\":true}".into()));
            track_raw_json_ref(&raw);
            response = normalize_server_response(DgmValue::RawJson(raw)).unwrap();
        }

        let scope = request_scope_snapshot();
        assert_eq!(scope.scopes_with_survivors, 0);
        assert_eq!(scope.scopes_with_assertable_survivors, 0);
        assert_eq!(scope.scopes_with_report_only_survivors, 0);
        assert_eq!(scope.last_survivors.raw_json, 0);
        drop(response);
    }

    #[test]
    fn normalize_server_response_encodes_json_body_into_pooled_bytes() {
        let response = normalize_server_response(dgm_map(&[
            ("status", DgmValue::Int(200)),
            ("body", dgm_map(&[("ok", DgmValue::Bool(true))])),
        ]))
        .unwrap();

        match response.body {
            ServerBody::JsonBytes(bytes) => {
                assert_eq!(std::str::from_utf8(bytes.as_slice()).unwrap(), "{\"ok\":true}");
            }
            other => panic!("expected pooled json body, got len={}", other.len()),
        }
    }

}
