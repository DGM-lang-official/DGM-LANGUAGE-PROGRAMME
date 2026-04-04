use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::mem;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;
use crate::ast::{Argument, BindingPattern, Expr, MatchPattern, Param, Span, Stmt, StmtKind};
use crate::bundle::{self, BundleImage, BundleRuntimeStats};
use crate::concurrency::{await_future, await_task_handle, ChannelState, FutureState, TaskHandleState};
use crate::environment::{
    environment_live_snapshot, new_child_environment_ref, new_environment_ref, Environment,
};
use crate::error::{register_source, trap_dgm, DgmError, StackFrame, ThrownValue};
use crate::memory::{allocator_snapshot, process_memory_snapshot};
use crate::request_scope::{
    request_scope_snapshot, track_json_view_ref, track_request_json_ref,
    track_request_shell_ref,
};
use crate::vm::{self, CompiledFunctionBlob};

macro_rules! numeric_op {
    ($left:expr, $right:expr, $int_op:ident, $float_op:tt, $name:expr) => {
        match ($left, $right) {
            (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Int(a.$int_op(b))),
            (DgmValue::Float(a), DgmValue::Float(b)) => Ok(DgmValue::Float(a $float_op b)),
            (DgmValue::Int(a), DgmValue::Float(b)) => Ok(DgmValue::Float(a as f64 $float_op b)),
            (DgmValue::Float(a), DgmValue::Int(b)) => Ok(DgmValue::Float(a $float_op b as f64)),
            _ => Err(DgmError::RuntimeError { msg: format!("'{}' type mismatch", $name) }),
        }
    };
}

macro_rules! cmp_op {
    ($left:expr, $right:expr, $op:tt, $name:expr) => {
        match ($left, $right) {
            (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Bool(a $op b)),
            (DgmValue::Float(a), DgmValue::Float(b)) => Ok(DgmValue::Bool(a $op b)),
            (DgmValue::Int(a), DgmValue::Float(b)) => Ok(DgmValue::Bool((a as f64) $op b)),
            (DgmValue::Float(a), DgmValue::Int(b)) => Ok(DgmValue::Bool(a $op (b as f64))),
            (DgmValue::Str(a), DgmValue::Str(b)) => Ok(DgmValue::Bool(a $op b)),
            _ => Err(DgmError::RuntimeError { msg: format!("'{}' type mismatch", $name) }),
        }
    };
}

const DEFAULT_MAX_STEPS: u64 = 10_000_000;
const DEFAULT_MAX_CALL_DEPTH: usize = 1_000;
const DEFAULT_MAX_HEAP_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_THREADS_MULTIPLIER: usize = 2;
const DEFAULT_MAX_THREADS_CAP: usize = 16;
const DEFAULT_MAX_WALL_TIME_MS: u64 = 30_000;
const DEFAULT_MAX_OPEN_SOCKETS: usize = 64;
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 16;
const REQUEST_METRIC_HISTORY_LIMIT: usize = 4096;
const REQUEST_ROUTE_HISTORY_LIMIT: usize = 512;

#[derive(Debug, Clone)]
struct RuntimeState {
    max_steps: u64,
    steps: u64,
    max_call_depth: usize,
    call_depth: usize,
    max_heap_bytes: usize,
    used_heap_bytes: usize,
    peak_heap_bytes: usize,
    max_threads: usize,
    max_wall_time_ms: u64,
    started_at: Instant,
    max_open_sockets: usize,
    open_sockets: usize,
    max_concurrent_requests: usize,
    concurrent_requests: usize,
    peak_concurrent_requests: usize,
    profile: RuntimeProfile,
    active_functions: Vec<ActiveProfileScope>,
    active_modules: Vec<ActiveProfileScope>,
    active_call_names: Vec<Option<String>>,
    active_alloc_labels: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RuntimeConfig {
    pub max_steps: u64,
    pub max_call_depth: usize,
    pub max_heap_bytes: usize,
    pub max_threads: usize,
    pub max_wall_time_ms: u64,
    pub max_open_sockets: usize,
    pub max_concurrent_requests: usize,
}

#[derive(Debug, Clone)]
pub struct RuntimeUsageSnapshot {
    pub max_steps: u64,
    pub steps: u64,
    pub max_call_depth: usize,
    pub call_depth: usize,
    pub max_heap_bytes: usize,
    pub used_heap_bytes: usize,
    pub peak_heap_bytes: usize,
    pub max_threads: usize,
    pub max_wall_time_ms: u64,
    pub elapsed_wall_time_ms: u64,
    pub max_open_sockets: usize,
    pub open_sockets: usize,
    pub max_concurrent_requests: usize,
    pub concurrent_requests: usize,
    pub peak_concurrent_requests: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LiveObjectTypeSnapshot {
    pub live: u64,
    pub created: u64,
    pub dropped: u64,
    pub live_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct RuntimeLiveObjectSnapshot {
    pub requests: LiveObjectTypeSnapshot,
    pub request_maps: LiveObjectTypeSnapshot,
    pub request_json: LiveObjectTypeSnapshot,
    pub raw_json: LiveObjectTypeSnapshot,
    pub json_views: LiveObjectTypeSnapshot,
}

#[derive(Debug, Clone)]
struct ActiveProfileScope {
    name: String,
    started_at: Instant,
}

#[derive(Debug, Clone)]
pub struct RuntimeProfile {
    pub start_time: Instant,
    pub total_time: u128,
    pub function_time: HashMap<String, u128>,
    pub function_calls: HashMap<String, u64>,
    pub module_time: HashMap<String, u128>,
    pub alloc_count: u64,
    pub alloc_bytes: u64,
    pub alloc_by_label: HashMap<String, AllocationProfileEntry>,
    pub peak_heap_bytes: u64,
    pub vm_instruction_count: u64,
    pub vm_frame_count: u64,
    pub vm_time_ns: u128,
    pub vm_alloc_count: u64,
    pub vm_alloc_bytes: u64,
    pub io_call_count: u64,
    pub io_time_ns: u128,
}

#[derive(Debug, Clone, Default)]
pub struct AllocationProfileEntry {
    pub count: u64,
    pub bytes: u64,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            max_steps: DEFAULT_MAX_STEPS,
            steps: 0,
            max_call_depth: DEFAULT_MAX_CALL_DEPTH,
            call_depth: 0,
            max_heap_bytes: DEFAULT_MAX_HEAP_BYTES,
            used_heap_bytes: 0,
            peak_heap_bytes: 0,
            max_threads: default_max_threads(),
            max_wall_time_ms: DEFAULT_MAX_WALL_TIME_MS,
            started_at: Instant::now(),
            max_open_sockets: DEFAULT_MAX_OPEN_SOCKETS,
            open_sockets: 0,
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            concurrent_requests: 0,
            peak_concurrent_requests: 0,
            profile: RuntimeProfile::default(),
            active_functions: vec![],
            active_modules: vec![],
            active_call_names: vec![],
            active_alloc_labels: vec![],
        }
    }
}

impl Default for RuntimeProfile {
    fn default() -> Self {
        Self {
            start_time: Instant::now(),
            total_time: 0,
            function_time: HashMap::new(),
            function_calls: HashMap::new(),
            module_time: HashMap::new(),
            alloc_count: 0,
            alloc_bytes: 0,
            alloc_by_label: HashMap::new(),
            peak_heap_bytes: 0,
            vm_instruction_count: 0,
            vm_frame_count: 0,
            vm_time_ns: 0,
            vm_alloc_count: 0,
            vm_alloc_bytes: 0,
            io_call_count: 0,
            io_time_ns: 0,
        }
    }
}

thread_local! {
    static RUNTIME_STATE: RefCell<RuntimeState> = RefCell::new(RuntimeState::default());
}

#[derive(Debug, Clone, Default)]
struct RequestMetricRoute {
    requests: u64,
    errors: u64,
    auth_failures: u64,
    last_status: i64,
    latencies_ms: VecDeque<u64>,
}

#[derive(Debug, Clone, Default)]
struct RequestMetricsState {
    requests: u64,
    errors: u64,
    auth_failures: u64,
    latencies_ms: VecDeque<u64>,
    routes: HashMap<String, RequestMetricRoute>,
}

static REQUEST_METRICS: OnceLock<Mutex<RequestMetricsState>> = OnceLock::new();

static LIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static CREATED_REQUESTS: AtomicU64 = AtomicU64::new(0);
static DROPPED_REQUESTS: AtomicU64 = AtomicU64::new(0);
static LIVE_REQUEST_BYTES: AtomicU64 = AtomicU64::new(0);

static LIVE_REQUEST_MAPS: AtomicU64 = AtomicU64::new(0);
static CREATED_REQUEST_MAPS: AtomicU64 = AtomicU64::new(0);
static DROPPED_REQUEST_MAPS: AtomicU64 = AtomicU64::new(0);
static LIVE_REQUEST_MAP_BYTES: AtomicU64 = AtomicU64::new(0);

static LIVE_REQUEST_JSON: AtomicU64 = AtomicU64::new(0);
static CREATED_REQUEST_JSON: AtomicU64 = AtomicU64::new(0);
static DROPPED_REQUEST_JSON: AtomicU64 = AtomicU64::new(0);
static LIVE_REQUEST_JSON_BYTES: AtomicU64 = AtomicU64::new(0);

static LIVE_RAW_JSON: AtomicU64 = AtomicU64::new(0);
static CREATED_RAW_JSON: AtomicU64 = AtomicU64::new(0);
static DROPPED_RAW_JSON: AtomicU64 = AtomicU64::new(0);
static LIVE_RAW_JSON_BYTES: AtomicU64 = AtomicU64::new(0);

static LIVE_JSON_VIEWS: AtomicU64 = AtomicU64::new(0);
static CREATED_JSON_VIEWS: AtomicU64 = AtomicU64::new(0);
static DROPPED_JSON_VIEWS: AtomicU64 = AtomicU64::new(0);
static LIVE_JSON_VIEW_BYTES: AtomicU64 = AtomicU64::new(0);

struct CallDepthGuard;
struct FunctionProfileGuard;
struct ModuleProfileGuard;
struct AllocationProfileGuard;

fn request_metrics_state() -> &'static Mutex<RequestMetricsState> {
    REQUEST_METRICS.get_or_init(|| Mutex::new(RequestMetricsState::default()))
}

fn lock_request_metrics() -> std::sync::MutexGuard<'static, RequestMetricsState> {
    match request_metrics_state().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn track_live_object_create(
    live: &AtomicU64,
    created: &AtomicU64,
    live_bytes: &AtomicU64,
    bytes: usize,
) {
    live.fetch_add(1, Ordering::Relaxed);
    created.fetch_add(1, Ordering::Relaxed);
    live_bytes.fetch_add(bytes.min(u64::MAX as usize) as u64, Ordering::Relaxed);
}

fn track_live_object_drop(live: &AtomicU64, dropped: &AtomicU64, live_bytes: &AtomicU64, bytes: usize) {
    live.fetch_sub(1, Ordering::Relaxed);
    dropped.fetch_add(1, Ordering::Relaxed);
    live_bytes.fetch_sub(bytes.min(u64::MAX as usize) as u64, Ordering::Relaxed);
}

fn snapshot_live_object(
    live: &AtomicU64,
    created: &AtomicU64,
    dropped: &AtomicU64,
    live_bytes: &AtomicU64,
) -> LiveObjectTypeSnapshot {
    LiveObjectTypeSnapshot {
        live: live.load(Ordering::Relaxed),
        created: created.load(Ordering::Relaxed),
        dropped: dropped.load(Ordering::Relaxed),
        live_bytes: live_bytes.load(Ordering::Relaxed),
    }
}

pub(crate) fn runtime_live_object_snapshot() -> RuntimeLiveObjectSnapshot {
    RuntimeLiveObjectSnapshot {
        requests: snapshot_live_object(&LIVE_REQUESTS, &CREATED_REQUESTS, &DROPPED_REQUESTS, &LIVE_REQUEST_BYTES),
        request_maps: snapshot_live_object(
            &LIVE_REQUEST_MAPS,
            &CREATED_REQUEST_MAPS,
            &DROPPED_REQUEST_MAPS,
            &LIVE_REQUEST_MAP_BYTES,
        ),
        request_json: snapshot_live_object(
            &LIVE_REQUEST_JSON,
            &CREATED_REQUEST_JSON,
            &DROPPED_REQUEST_JSON,
            &LIVE_REQUEST_JSON_BYTES,
        ),
        raw_json: snapshot_live_object(&LIVE_RAW_JSON, &CREATED_RAW_JSON, &DROPPED_RAW_JSON, &LIVE_RAW_JSON_BYTES),
        json_views: snapshot_live_object(
            &LIVE_JSON_VIEWS,
            &CREATED_JSON_VIEWS,
            &DROPPED_JSON_VIEWS,
            &LIVE_JSON_VIEW_BYTES,
        ),
    }
}

fn push_metric_sample(samples: &mut VecDeque<u64>, value: u64, limit: usize) {
    if samples.len() >= limit {
        samples.pop_front();
    }
    samples.push_back(value);
}

fn percentile_u64(samples: &VecDeque<u64>, ratio: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut ordered = samples.iter().copied().collect::<Vec<_>>();
    ordered.sort_unstable();
    let index = ((ordered.len().saturating_sub(1)) as f64 * ratio).floor() as usize;
    ordered[index.min(ordered.len().saturating_sub(1))]
}

fn latency_summary_value(samples: &VecDeque<u64>) -> DgmValue {
    let count = samples.len() as u64;
    let sum = samples.iter().copied().sum::<u64>();
    let avg = if count == 0 { 0.0 } else { sum as f64 / count as f64 };
    let max = samples.iter().copied().max().unwrap_or(0);
    DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
        ("count".into(), DgmValue::Int(clamp_u64_to_i64(count))),
        ("avg".into(), DgmValue::Float(avg)),
        ("p50".into(), DgmValue::Int(clamp_u64_to_i64(percentile_u64(samples, 0.50)))),
        ("p95".into(), DgmValue::Int(clamp_u64_to_i64(percentile_u64(samples, 0.95)))),
        ("p99".into(), DgmValue::Int(clamp_u64_to_i64(percentile_u64(samples, 0.99)))),
        ("max".into(), DgmValue::Int(clamp_u64_to_i64(max))),
    ]))))
}

impl Drop for CallDepthGuard {
    fn drop(&mut self) {
        RUNTIME_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if state.call_depth > 0 {
                state.call_depth -= 1;
            }
            state.active_call_names.pop();
        });
    }
}

impl Drop for FunctionProfileGuard {
    fn drop(&mut self) {
        RUNTIME_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if let Some(scope) = state.active_functions.pop() {
                let elapsed = scope.started_at.elapsed().as_nanos();
                let entry = state.profile.function_time.entry(scope.name).or_insert(0);
                *entry = entry.saturating_add(elapsed);
            }
        });
    }
}

impl Drop for ModuleProfileGuard {
    fn drop(&mut self) {
        RUNTIME_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if let Some(scope) = state.active_modules.pop() {
                let elapsed = scope.started_at.elapsed().as_nanos();
                let entry = state.profile.module_time.entry(scope.name).or_insert(0);
                *entry = entry.saturating_add(elapsed);
            }
        });
    }
}

impl Drop for AllocationProfileGuard {
    fn drop(&mut self) {
        RUNTIME_STATE.with(|state| {
            state.borrow_mut().active_alloc_labels.pop();
        });
    }
}

#[derive(Debug, Clone)]
pub enum DgmValue {
    Int(i64), Float(f64), Str(String), Bool(bool), Null,
    List(Rc<RefCell<Vec<DgmValue>>>),
    Map(Rc<RefCell<HashMap<String, DgmValue>>>),
    Request(Rc<RequestValue>),
    RequestShell(Rc<RequestShellValue>),
    RequestMap(Rc<RequestMapValue>),
    RequestMapView(Arc<HashMap<String, String>>),
    RequestJson(Rc<RequestJsonValue>),
    Json(Arc<JsonValueState>),
    RawJson(Arc<RawJsonValue>),
    Function {
        name: Option<String>,
        params: Vec<Param>,
        body: Vec<Stmt>,
        closure: Rc<RefCell<Environment>>,
        compiled: Option<Arc<CompiledFunctionBlob>>,
    },
    NativeFunction { name: String, func: fn(Vec<DgmValue>) -> Result<DgmValue, DgmError> },
    NativeMethod { name: String, state: Box<DgmValue>, func: fn(DgmValue, Vec<DgmValue>) -> Result<DgmValue, DgmError> },
    Channel(std::sync::Arc<ChannelState>),
    TaskHandle(std::sync::Arc<TaskHandleState>),
    Future(std::sync::Arc<FutureState>),
    Class { name: String, parent: Option<String>, methods: Vec<Stmt>, closure: Rc<RefCell<Environment>> },
    Instance { class_name: String, fields: Rc<RefCell<HashMap<String, DgmValue>>> },
}

#[derive(Debug, Clone)]
pub struct RawJsonValue {
    segments: Arc<Vec<Arc<str>>>,
    len: usize,
}

impl RawJsonValue {
    pub fn from_text(text: String) -> Self {
        Self::from_arc(Arc::<str>::from(text))
    }

    pub fn from_arc(text: Arc<str>) -> Self {
        let value = Self {
            len: text.len(),
            segments: Arc::new(vec![text]),
        };
        track_live_object_create(&LIVE_RAW_JSON, &CREATED_RAW_JSON, &LIVE_RAW_JSON_BYTES, raw_json_live_size(&value));
        value
    }

    pub fn from_parts(parts: Vec<Arc<str>>) -> Self {
        let filtered = parts.into_iter().filter(|part| !part.is_empty()).collect::<Vec<_>>();
        let len = filtered.iter().map(|part| part.len()).sum();
        let value = Self {
            len,
            segments: Arc::new(filtered),
        };
        track_live_object_create(&LIVE_RAW_JSON, &CREATED_RAW_JSON, &LIVE_RAW_JSON_BYTES, raw_json_live_size(&value));
        value
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn segments(&self) -> &[Arc<str>] {
        self.segments.as_ref()
    }

    pub fn to_string_lossy(&self) -> String {
        if self.segments.len() == 1 {
            return self.segments[0].to_string();
        }
        let mut out = String::with_capacity(self.len);
        for segment in self.segments.iter() {
            out.push_str(segment);
        }
        out
    }

    pub fn contains(&self, needle: &str) -> bool {
        self.to_string_lossy().contains(needle)
    }

    pub fn starts_with(&self, needle: &str) -> bool {
        self.to_string_lossy().starts_with(needle)
    }

    pub fn ends_with(&self, needle: &str) -> bool {
        self.to_string_lossy().ends_with(needle)
    }

    pub fn char_len(&self) -> usize {
        self.segments.iter().map(|segment| segment.chars().count()).sum()
    }
}

impl Drop for RawJsonValue {
    fn drop(&mut self) {
        track_live_object_drop(&LIVE_RAW_JSON, &DROPPED_RAW_JSON, &LIVE_RAW_JSON_BYTES, raw_json_live_size(self));
    }
}

#[derive(Debug)]
pub struct RequestMapValue {
    pub(crate) values: Arc<HashMap<String, String>>,
}

impl RequestMapValue {
    pub(crate) fn new(values: Arc<HashMap<String, String>>) -> Self {
        let value = Self { values };
        track_live_object_create(
            &LIVE_REQUEST_MAPS,
            &CREATED_REQUEST_MAPS,
            &LIVE_REQUEST_MAP_BYTES,
            request_map_live_size(value.values.as_ref()),
        );
        value
    }
}

impl Drop for RequestMapValue {
    fn drop(&mut self) {
        track_live_object_drop(
            &LIVE_REQUEST_MAPS,
            &DROPPED_REQUEST_MAPS,
            &LIVE_REQUEST_MAP_BYTES,
            request_map_live_size(self.values.as_ref()),
        );
    }
}

#[derive(Debug)]
pub struct RequestJsonValue {
    pub(crate) content_type: Arc<str>,
    pub(crate) body: Arc<str>,
    pub(crate) parsed: RefCell<Option<Result<DgmValue, String>>>,
}

impl RequestJsonValue {
    pub(crate) fn new(content_type: Arc<str>, body: Arc<str>) -> Self {
        let value = Self {
            content_type,
            body,
            parsed: RefCell::new(None),
        };
        track_live_object_create(
            &LIVE_REQUEST_JSON,
            &CREATED_REQUEST_JSON,
            &LIVE_REQUEST_JSON_BYTES,
            request_json_live_size(value.content_type.as_ref(), value.body.as_ref()),
        );
        value
    }
}

impl Drop for RequestJsonValue {
    fn drop(&mut self) {
        track_live_object_drop(
            &LIVE_REQUEST_JSON,
            &DROPPED_REQUEST_JSON,
            &LIVE_REQUEST_JSON_BYTES,
            request_json_live_size(self.content_type.as_ref(), self.body.as_ref()),
        );
    }
}

#[derive(Debug)]
pub struct RequestValue {
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) path: String,
    headers_values: Arc<HashMap<String, String>>,
    query_values: Arc<HashMap<String, String>>,
    params_values: Arc<HashMap<String, String>>,
    pub(crate) body: Arc<str>,
    content_type: Arc<str>,
    json: RefCell<Option<Rc<RequestJsonValue>>>,
}

#[derive(Debug)]
pub struct RequestBacking {
    pub(crate) method: String,
    pub(crate) url: String,
    pub(crate) path: String,
    pub(crate) headers_values: Arc<HashMap<String, String>>,
    pub(crate) query_values: Arc<HashMap<String, String>>,
    pub(crate) params_values: Arc<HashMap<String, String>>,
    pub(crate) body: Arc<str>,
    pub(crate) content_type: Arc<str>,
}

impl RequestBacking {
    pub(crate) fn from_parts(
        method: String,
        url: String,
        path: String,
        headers: HashMap<String, String>,
        query: HashMap<String, String>,
        params: HashMap<String, String>,
        body: String,
    ) -> Arc<Self> {
        let content_type = Arc::<str>::from(
            headers
                .get("content_type")
                .map(|text| text.to_ascii_lowercase())
                .unwrap_or_default(),
        );
        Arc::new(Self {
            method,
            url,
            path,
            headers_values: Arc::new(headers),
            query_values: Arc::new(query),
            params_values: Arc::new(params),
            body: Arc::<str>::from(body),
            content_type,
        })
    }
}

#[derive(Debug)]
pub struct RequestShellValue {
    backing: Arc<RequestBacking>,
    json: RefCell<Option<Rc<RequestJsonValue>>>,
}

impl RequestShellValue {
    fn new(backing: Arc<RequestBacking>) -> Self {
        let value = Self {
            backing,
            json: RefCell::new(None),
        };
        track_live_object_create(
            &LIVE_REQUESTS,
            &CREATED_REQUESTS,
            &LIVE_REQUEST_BYTES,
            request_shell_live_size(&value),
        );
        value
    }

    pub(crate) fn json_ref(&self) -> Rc<RequestJsonValue> {
        request_json_ref_for(&self.backing.content_type, &self.backing.body, &self.json)
    }

    pub(crate) fn backing(&self) -> &Arc<RequestBacking> {
        &self.backing
    }

    pub(crate) fn headers_values(&self) -> &HashMap<String, String> {
        self.backing.headers_values.as_ref()
    }

    pub(crate) fn query_values(&self) -> &HashMap<String, String> {
        self.backing.query_values.as_ref()
    }

    pub(crate) fn params_values(&self) -> &HashMap<String, String> {
        self.backing.params_values.as_ref()
    }

    pub(crate) fn body_text(&self) -> &str {
        self.backing.body.as_ref()
    }

    pub(crate) fn content_type_text(&self) -> &str {
        self.backing.content_type.as_ref()
    }
}

impl Drop for RequestShellValue {
    fn drop(&mut self) {
        track_live_object_drop(
            &LIVE_REQUESTS,
            &DROPPED_REQUESTS,
            &LIVE_REQUEST_BYTES,
            request_shell_live_size(self),
        );
    }
}

impl RequestValue {
    #[allow(dead_code)]
    fn new(
        method: String,
        url: String,
        path: String,
        headers_values: Arc<HashMap<String, String>>,
        query_values: Arc<HashMap<String, String>>,
        params_values: Arc<HashMap<String, String>>,
        body: Arc<str>,
        content_type: Arc<str>,
    ) -> Self {
        let value = Self {
            method,
            url,
            path,
            headers_values,
            query_values,
            params_values,
            body,
            content_type,
            json: RefCell::new(None),
        };
        track_live_object_create(
            &LIVE_REQUESTS,
            &CREATED_REQUESTS,
            &LIVE_REQUEST_BYTES,
            request_live_size(&value.method, &value.url, &value.path, value.body.as_ref()),
        );
        value
    }

    pub(crate) fn json_ref(&self) -> Rc<RequestJsonValue> {
        request_json_ref_for(&self.content_type, &self.body, &self.json)
    }

    pub(crate) fn headers_values(&self) -> &HashMap<String, String> {
        self.headers_values.as_ref()
    }

    pub(crate) fn query_values(&self) -> &HashMap<String, String> {
        self.query_values.as_ref()
    }

    pub(crate) fn params_values(&self) -> &HashMap<String, String> {
        self.params_values.as_ref()
    }

    pub(crate) fn body_text(&self) -> &str {
        self.body.as_ref()
    }

    pub(crate) fn content_type_text(&self) -> &str {
        self.content_type.as_ref()
    }
}

impl Drop for RequestValue {
    fn drop(&mut self) {
        track_live_object_drop(
            &LIVE_REQUESTS,
            &DROPPED_REQUESTS,
            &LIVE_REQUEST_BYTES,
            request_live_size(&self.method, &self.url, &self.path, self.body.as_ref()),
        );
    }
}

#[derive(Debug, Clone)]
pub(crate) enum JsonPathSegment {
    Key(String),
    Index(usize),
}

#[derive(Debug)]
pub struct JsonValueState {
    pub(crate) root: Arc<serde_json::Value>,
    pub(crate) path: Vec<JsonPathSegment>,
}

impl JsonValueState {
    fn new(root: Arc<serde_json::Value>, path: Vec<JsonPathSegment>) -> Self {
        let value = Self { root, path };
        track_live_object_create(
            &LIVE_JSON_VIEWS,
            &CREATED_JSON_VIEWS,
            &LIVE_JSON_VIEW_BYTES,
            json_view_live_size(&value.path),
        );
        value
    }
}

impl Drop for JsonValueState {
    fn drop(&mut self) {
        track_live_object_drop(
            &LIVE_JSON_VIEWS,
            &DROPPED_JSON_VIEWS,
            &LIVE_JSON_VIEW_BYTES,
            json_view_live_size(&self.path),
        );
    }
}

pub(crate) fn request_value_from_parts(
    method: String,
    url: String,
    path: String,
    headers: HashMap<String, String>,
    query: HashMap<String, String>,
    params: HashMap<String, String>,
    body: String,
) -> DgmValue {
    request_shell_from_backing(RequestBacking::from_parts(method, url, path, headers, query, params, body))
}

pub(crate) fn request_shell_from_backing(backing: Arc<RequestBacking>) -> DgmValue {
    let request = Rc::new(RequestShellValue::new(backing));
    track_request_shell_ref(&request);
    DgmValue::RequestShell(request)
}

pub(crate) fn request_backing_live_size(backing: &RequestBacking) -> usize {
    mem::size_of::<RequestBacking>() + backing.method.len() + backing.url.len() + backing.path.len() + backing.body.len()
}

pub(crate) fn request_shell_live_size(value: &RequestShellValue) -> usize {
    mem::size_of::<RequestShellValue>() + request_backing_live_size(value.backing.as_ref())
}

fn request_json_ref_for(
    content_type: &Arc<str>,
    body: &Arc<str>,
    cache: &RefCell<Option<Rc<RequestJsonValue>>>,
) -> Rc<RequestJsonValue> {
    if let Some(existing) = cache.borrow().as_ref().cloned() {
        return existing;
    }
    let json = Rc::new(RequestJsonValue::new(Arc::clone(content_type), Arc::clone(body)));
    track_request_json_ref(&json);
    *cache.borrow_mut() = Some(Rc::clone(&json));
    json
}

fn json_resolve_with<'a>(root: &'a serde_json::Value, path: &[JsonPathSegment]) -> &'a serde_json::Value {
    let mut value = root;
    for segment in path {
        value = match segment {
            JsonPathSegment::Key(key) => value.get(key).unwrap_or(&serde_json::Value::Null),
            JsonPathSegment::Index(index) => value.get(*index).unwrap_or(&serde_json::Value::Null),
        };
    }
    value
}

pub(crate) fn json_resolved_value(state: &JsonValueState) -> &serde_json::Value {
    json_resolve_with(&state.root, &state.path)
}

fn json_value_to_dgm(root: Arc<serde_json::Value>, path: Vec<JsonPathSegment>) -> DgmValue {
    match json_resolve_with(&root, &path) {
        serde_json::Value::Null => DgmValue::Null,
        serde_json::Value::Bool(value) => DgmValue::Bool(*value),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                DgmValue::Int(value)
            } else {
                DgmValue::Float(number.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(value) => DgmValue::Str(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let view = Arc::new(JsonValueState::new(root, path));
            track_json_view_ref(&view);
            DgmValue::Json(view)
        }
    }
}

pub(crate) fn json_value_from_root(root: Arc<serde_json::Value>) -> DgmValue {
    json_value_to_dgm(root, vec![])
}

fn request_string_map_heap_size(values: &HashMap<String, String>) -> usize {
    mem::size_of::<HashMap<String, String>>()
        + values
            .iter()
            .map(|(key, value)| mem::size_of::<String>() + key.len() + mem::size_of::<String>() + value.len())
            .sum::<usize>()
}

fn request_map_live_size(values: &HashMap<String, String>) -> usize {
    mem::size_of::<RequestMapValue>() + request_string_map_heap_size(values)
}

fn request_json_live_size(content_type: &str, body: &str) -> usize {
    mem::size_of::<RequestJsonValue>() + content_type.len() + body.len()
}

fn request_live_size(method: &str, url: &str, path: &str, body: &str) -> usize {
    mem::size_of::<RequestValue>() + method.len() + url.len() + path.len() + body.len()
}

fn raw_json_live_size(value: &RawJsonValue) -> usize {
    mem::size_of::<RawJsonValue>()
        + mem::size_of::<Vec<Arc<str>>>()
        + value
            .segments()
            .iter()
            .map(|segment| mem::size_of::<Arc<str>>() + segment.len())
            .sum::<usize>()
}

fn json_view_live_size(path: &[JsonPathSegment]) -> usize {
    mem::size_of::<JsonValueState>()
        + mem::size_of::<Vec<JsonPathSegment>>()
        + path
            .iter()
            .map(|segment| match segment {
                JsonPathSegment::Key(key) => mem::size_of::<String>() + key.len(),
                JsonPathSegment::Index(_) => mem::size_of::<usize>(),
            })
            .sum::<usize>()
}

pub(crate) fn runtime_reset_all() {
    RUNTIME_STATE.with(|state| *state.borrow_mut() = RuntimeState::default());
}

pub(crate) fn runtime_reset_usage() {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.steps = 0;
        state.call_depth = 0;
        state.used_heap_bytes = 0;
        state.peak_heap_bytes = 0;
        state.started_at = Instant::now();
        state.open_sockets = 0;
        state.concurrent_requests = 0;
        state.peak_concurrent_requests = 0;
        state.profile = RuntimeProfile::default();
        state.active_functions.clear();
        state.active_modules.clear();
        state.active_call_names.clear();
        state.active_alloc_labels.clear();
    });
}

pub(crate) fn runtime_config() -> RuntimeConfig {
    RUNTIME_STATE.with(|state| {
        let state = state.borrow();
        RuntimeConfig {
            max_steps: state.max_steps,
            max_call_depth: state.max_call_depth,
            max_heap_bytes: state.max_heap_bytes,
            max_threads: state.max_threads,
            max_wall_time_ms: state.max_wall_time_ms,
            max_open_sockets: state.max_open_sockets,
            max_concurrent_requests: state.max_concurrent_requests,
        }
    })
}

pub(crate) fn runtime_apply_config(config: &RuntimeConfig) -> Result<(), DgmError> {
    runtime_set_max_steps(config.max_steps)?;
    runtime_set_max_call_depth(config.max_call_depth)?;
    runtime_set_max_heap_bytes(config.max_heap_bytes)?;
    runtime_set_max_threads(config.max_threads)?;
    runtime_set_max_wall_time_ms(config.max_wall_time_ms)?;
    runtime_set_max_open_sockets(config.max_open_sockets)?;
    runtime_set_max_concurrent_requests(config.max_concurrent_requests)?;
    Ok(())
}

pub(crate) fn runtime_tick() -> Result<(), DgmError> {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        runtime_check_wall_clock_locked(&state)?;
        state.steps += 1;
        if state.steps > state.max_steps {
            Err(DgmError::RuntimeError { msg: "execution limit exceeded".into() })
        } else {
            Ok(())
        }
    })
}

fn runtime_enter_call(name: Option<&str>) -> Result<CallDepthGuard, DgmError> {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        runtime_check_wall_clock_locked(&state)?;
        state.call_depth += 1;
        if state.call_depth > state.max_call_depth {
            state.call_depth -= 1;
            Err(DgmError::RuntimeError { msg: "max call depth exceeded".into() })
        } else {
            state.active_call_names.push(name.map(|name| name.to_string()));
            Ok(CallDepthGuard)
        }
    })
}

fn runtime_current_function() -> Option<String> {
    RUNTIME_STATE.with(|state| {
        state
            .borrow()
            .active_call_names
            .iter()
            .rev()
            .find_map(|name| name.clone())
    })
}

fn runtime_enter_function_profile(name: &str) -> FunctionProfileGuard {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        let entry = state.profile.function_calls.entry(name.to_string()).or_insert(0);
        *entry = entry.saturating_add(1);
        state.active_functions.push(ActiveProfileScope {
            name: name.to_string(),
            started_at: Instant::now(),
        });
    });
    FunctionProfileGuard
}

fn runtime_enter_module_profile(name: &str) -> ModuleProfileGuard {
    RUNTIME_STATE.with(|state| {
        state.borrow_mut().active_modules.push(ActiveProfileScope {
            name: name.to_string(),
            started_at: Instant::now(),
        });
    });
    ModuleProfileGuard
}

fn runtime_enter_alloc_profile(label: &str) -> AllocationProfileGuard {
    RUNTIME_STATE.with(|state| {
        state.borrow_mut().active_alloc_labels.push(label.to_string());
    });
    AllocationProfileGuard
}

pub fn runtime_profile_snapshot() -> RuntimeProfile {
    RUNTIME_STATE.with(|state| {
        let state = state.borrow();
        let mut profile = state.profile.clone();
        profile.total_time = profile.start_time.elapsed().as_nanos();
        profile.peak_heap_bytes = state.peak_heap_bytes.min(u64::MAX as usize) as u64;
        for scope in &state.active_functions {
            let elapsed = scope.started_at.elapsed().as_nanos();
            let entry = profile.function_time.entry(scope.name.clone()).or_insert(0);
            *entry = entry.saturating_add(elapsed);
        }
        for scope in &state.active_modules {
            let elapsed = scope.started_at.elapsed().as_nanos();
            let entry = profile.module_time.entry(scope.name.clone()).or_insert(0);
            *entry = entry.saturating_add(elapsed);
        }
        profile
    })
}

pub(crate) fn runtime_reserve(bytes: usize) -> Result<(), DgmError> {
    if bytes == 0 {
        return Ok(());
    }
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        runtime_check_wall_clock_locked(&state)?;
        state.profile.alloc_count = state.profile.alloc_count.saturating_add(1);
        state.profile.alloc_bytes = state.profile.alloc_bytes.saturating_add(bytes as u64);
        if let Some(label) = state.active_alloc_labels.last().cloned() {
            let entry = state.profile.alloc_by_label.entry(label).or_default();
            entry.count = entry.count.saturating_add(1);
            entry.bytes = entry.bytes.saturating_add(bytes as u64);
        }
        state.used_heap_bytes = state.used_heap_bytes.checked_add(bytes)
            .ok_or_else(|| DgmError::RuntimeError { msg: "memory limit exceeded".into() })?;
        state.peak_heap_bytes = state.peak_heap_bytes.max(state.used_heap_bytes);
        state.profile.peak_heap_bytes = state.peak_heap_bytes.min(u64::MAX as usize) as u64;
        if state.used_heap_bytes > state.max_heap_bytes {
            Err(DgmError::RuntimeError { msg: "memory limit exceeded".into() })
        } else {
            Ok(())
        }
    })
}

pub(crate) fn runtime_allocation_counters() -> (u64, u64) {
    RUNTIME_STATE.with(|state| {
        let state = state.borrow();
        (state.profile.alloc_count, state.profile.alloc_bytes)
    })
}

pub(crate) fn runtime_record_vm_instruction() {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.profile.vm_instruction_count = state.profile.vm_instruction_count.saturating_add(1);
    });
}

pub(crate) fn runtime_record_vm_frame(elapsed_ns: u128, instructions: u64, alloc_count: u64, alloc_bytes: u64) {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.profile.vm_frame_count = state.profile.vm_frame_count.saturating_add(1);
        let _ = instructions;
        state.profile.vm_time_ns = state.profile.vm_time_ns.saturating_add(elapsed_ns);
        state.profile.vm_alloc_count = state.profile.vm_alloc_count.saturating_add(alloc_count);
        state.profile.vm_alloc_bytes = state.profile.vm_alloc_bytes.saturating_add(alloc_bytes);
    });
}

pub(crate) fn runtime_check_limits() -> Result<(), DgmError> {
    RUNTIME_STATE.with(|state| {
        let state = state.borrow();
        runtime_check_wall_clock_locked(&state)
    })
}

pub(crate) fn runtime_record_io(elapsed_ns: u128) {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.profile.io_call_count = state.profile.io_call_count.saturating_add(1);
        state.profile.io_time_ns = state.profile.io_time_ns.saturating_add(elapsed_ns);
    });
}

pub(crate) fn runtime_merge_profile(profile: &RuntimeProfile) {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        runtime_merge_profile_into(&mut state.profile, profile);
    });
}

pub(crate) fn runtime_merge_profile_into(target: &mut RuntimeProfile, profile: &RuntimeProfile) {
    target.total_time = target.total_time.saturating_add(profile.total_time);
    target.alloc_count = target.alloc_count.saturating_add(profile.alloc_count);
    target.alloc_bytes = target.alloc_bytes.saturating_add(profile.alloc_bytes);
    for (label, entry) in &profile.alloc_by_label {
        let slot = target.alloc_by_label.entry(label.clone()).or_default();
        slot.count = slot.count.saturating_add(entry.count);
        slot.bytes = slot.bytes.saturating_add(entry.bytes);
    }
    target.peak_heap_bytes = target.peak_heap_bytes.max(profile.peak_heap_bytes);
    target.vm_instruction_count = target.vm_instruction_count.saturating_add(profile.vm_instruction_count);
    target.vm_frame_count = target.vm_frame_count.saturating_add(profile.vm_frame_count);
    target.vm_time_ns = target.vm_time_ns.saturating_add(profile.vm_time_ns);
    target.vm_alloc_count = target.vm_alloc_count.saturating_add(profile.vm_alloc_count);
    target.vm_alloc_bytes = target.vm_alloc_bytes.saturating_add(profile.vm_alloc_bytes);
    target.io_call_count = target.io_call_count.saturating_add(profile.io_call_count);
    target.io_time_ns = target.io_time_ns.saturating_add(profile.io_time_ns);
    for (name, value) in &profile.function_time {
        let entry = target.function_time.entry(name.clone()).or_insert(0);
        *entry = entry.saturating_add(*value);
    }
    for (name, value) in &profile.function_calls {
        let entry = target.function_calls.entry(name.clone()).or_insert(0);
        *entry = entry.saturating_add(*value);
    }
    for (name, value) in &profile.module_time {
        let entry = target.module_time.entry(name.clone()).or_insert(0);
        *entry = entry.saturating_add(*value);
    }
}

pub(crate) fn runtime_set_max_steps(limit: u64) -> Result<(), DgmError> {
    if limit == 0 {
        return Err(DgmError::RuntimeError { msg: "runtime.set_max_steps requires limit > 0".into() });
    }
    RUNTIME_STATE.with(|state| state.borrow_mut().max_steps = limit);
    Ok(())
}

pub(crate) fn runtime_set_max_call_depth(limit: usize) -> Result<(), DgmError> {
    if limit == 0 {
        return Err(DgmError::RuntimeError { msg: "runtime.set_max_call_depth requires limit > 0".into() });
    }
    RUNTIME_STATE.with(|state| state.borrow_mut().max_call_depth = limit);
    Ok(())
}

pub(crate) fn runtime_set_max_heap_bytes(limit: usize) -> Result<(), DgmError> {
    if limit == 0 {
        return Err(DgmError::RuntimeError { msg: "runtime.set_max_heap_bytes requires limit > 0".into() });
    }
    RUNTIME_STATE.with(|state| state.borrow_mut().max_heap_bytes = limit);
    Ok(())
}

pub(crate) fn runtime_set_max_threads(limit: usize) -> Result<(), DgmError> {
    if limit == 0 {
        return Err(DgmError::RuntimeError { msg: "runtime.set_max_threads requires limit > 0".into() });
    }
    RUNTIME_STATE.with(|state| state.borrow_mut().max_threads = limit);
    crate::concurrency::configure_async_runtime(limit);
    Ok(())
}

pub(crate) fn runtime_set_max_wall_time_ms(limit: u64) -> Result<(), DgmError> {
    if limit == 0 {
        return Err(DgmError::RuntimeError { msg: "runtime.set_max_wall_time_ms requires limit > 0".into() });
    }
    RUNTIME_STATE.with(|state| state.borrow_mut().max_wall_time_ms = limit);
    Ok(())
}

pub(crate) fn runtime_set_max_open_sockets(limit: usize) -> Result<(), DgmError> {
    if limit == 0 {
        return Err(DgmError::RuntimeError { msg: "runtime.set_max_open_sockets requires limit > 0".into() });
    }
    RUNTIME_STATE.with(|state| state.borrow_mut().max_open_sockets = limit);
    Ok(())
}

pub(crate) fn runtime_set_max_concurrent_requests(limit: usize) -> Result<(), DgmError> {
    if limit == 0 {
        return Err(DgmError::RuntimeError { msg: "runtime.set_max_concurrent_requests requires limit > 0".into() });
    }
    RUNTIME_STATE.with(|state| state.borrow_mut().max_concurrent_requests = limit);
    Ok(())
}

pub(crate) fn runtime_open_socket() -> Result<(), DgmError> {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        runtime_check_wall_clock_locked(&state)?;
        if state.open_sockets >= state.max_open_sockets {
            return Err(DgmError::RuntimeError { msg: "open socket limit exceeded".into() });
        }
        state.open_sockets = state.open_sockets.saturating_add(1);
        Ok(())
    })
}

pub(crate) fn runtime_close_socket() {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.open_sockets = state.open_sockets.saturating_sub(1);
    });
}

pub(crate) fn runtime_begin_request() -> Result<(), DgmError> {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        runtime_check_wall_clock_locked(&state)?;
        if state.concurrent_requests >= state.max_concurrent_requests {
            return Err(DgmError::RuntimeError { msg: "concurrent request limit exceeded".into() });
        }
        state.concurrent_requests = state.concurrent_requests.saturating_add(1);
        state.peak_concurrent_requests = state.peak_concurrent_requests.max(state.concurrent_requests);
        Ok(())
    })
}

pub(crate) fn runtime_end_request() {
    RUNTIME_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.concurrent_requests = state.concurrent_requests.saturating_sub(1);
    });
}

pub(crate) fn runtime_usage_snapshot() -> RuntimeUsageSnapshot {
    RUNTIME_STATE.with(|state| {
        let state = state.borrow();
        RuntimeUsageSnapshot {
            max_steps: state.max_steps,
            steps: state.steps,
            max_call_depth: state.max_call_depth,
            call_depth: state.call_depth,
            max_heap_bytes: state.max_heap_bytes,
            used_heap_bytes: state.used_heap_bytes,
            peak_heap_bytes: state.peak_heap_bytes,
            max_threads: state.max_threads,
            max_wall_time_ms: state.max_wall_time_ms,
            elapsed_wall_time_ms: state.started_at.elapsed().as_millis().min(u64::MAX as u128) as u64,
            max_open_sockets: state.max_open_sockets,
            open_sockets: state.open_sockets,
            max_concurrent_requests: state.max_concurrent_requests,
            concurrent_requests: state.concurrent_requests,
            peak_concurrent_requests: state.peak_concurrent_requests,
        }
    })
}

pub(crate) fn runtime_stats() -> HashMap<String, DgmValue> {
    let snapshot = runtime_usage_snapshot();
    let process = process_memory_snapshot();
    let allocator = allocator_snapshot();
    let live_objects = runtime_live_object_snapshot();
    let environments = environment_live_snapshot();
    let request_scope = request_scope_snapshot();
    HashMap::from([
        ("max_steps".into(), DgmValue::Int(snapshot.max_steps as i64)),
        ("steps".into(), DgmValue::Int(snapshot.steps as i64)),
        ("max_call_depth".into(), DgmValue::Int(snapshot.max_call_depth as i64)),
        ("call_depth".into(), DgmValue::Int(snapshot.call_depth as i64)),
        ("max_heap_bytes".into(), DgmValue::Int(snapshot.max_heap_bytes as i64)),
        ("used_heap_bytes".into(), DgmValue::Int(snapshot.used_heap_bytes as i64)),
        ("peak_heap_bytes".into(), DgmValue::Int(snapshot.peak_heap_bytes as i64)),
        ("max_threads".into(), DgmValue::Int(snapshot.max_threads as i64)),
        ("max_wall_time_ms".into(), DgmValue::Int(snapshot.max_wall_time_ms as i64)),
        ("elapsed_wall_time_ms".into(), DgmValue::Int(snapshot.elapsed_wall_time_ms as i64)),
        ("max_open_sockets".into(), DgmValue::Int(snapshot.max_open_sockets as i64)),
        ("open_sockets".into(), DgmValue::Int(snapshot.open_sockets as i64)),
        ("max_concurrent_requests".into(), DgmValue::Int(snapshot.max_concurrent_requests as i64)),
        ("concurrent_requests".into(), DgmValue::Int(snapshot.concurrent_requests as i64)),
        ("peak_concurrent_requests".into(), DgmValue::Int(snapshot.peak_concurrent_requests as i64)),
        (
            "process_memory".into(),
            DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
                ("rss_bytes".into(), DgmValue::Int(clamp_u64_to_i64(process.rss_bytes))),
                ("virtual_bytes".into(), DgmValue::Int(clamp_u64_to_i64(process.virtual_bytes))),
            ])))),
        ),
        (
            "allocator".into(),
            DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
                ("supported".into(), DgmValue::Bool(allocator.supported)),
                ("arena_bytes".into(), DgmValue::Int(clamp_u64_to_i64(allocator.arena_bytes))),
                ("mmap_bytes".into(), DgmValue::Int(clamp_u64_to_i64(allocator.mmap_bytes))),
                ("in_use_bytes".into(), DgmValue::Int(clamp_u64_to_i64(allocator.in_use_bytes))),
                ("free_bytes".into(), DgmValue::Int(clamp_u64_to_i64(allocator.free_bytes))),
                (
                    "releasable_bytes".into(),
                    DgmValue::Int(clamp_u64_to_i64(allocator.releasable_bytes)),
                ),
                ("free_chunks".into(), DgmValue::Int(clamp_u64_to_i64(allocator.free_chunks))),
            ])))),
        ),
        (
            "environments".into(),
            DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
                ("live".into(), DgmValue::Int(clamp_u64_to_i64(environments.live))),
                ("created".into(), DgmValue::Int(clamp_u64_to_i64(environments.created))),
                ("dropped".into(), DgmValue::Int(clamp_u64_to_i64(environments.dropped))),
                ("pooled_maps".into(), DgmValue::Int(clamp_u64_to_i64(environments.pooled_maps))),
                ("pooled_capacity".into(), DgmValue::Int(clamp_u64_to_i64(environments.pooled_capacity))),
            ])))),
        ),
        (
            "live_objects".into(),
            DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
                ("requests".into(), live_object_stats_value(live_objects.requests)),
                ("request_maps".into(), live_object_stats_value(live_objects.request_maps)),
                ("request_json".into(), live_object_stats_value(live_objects.request_json)),
                ("raw_json".into(), live_object_stats_value(live_objects.raw_json)),
                ("json_views".into(), live_object_stats_value(live_objects.json_views)),
            ])))),
        ),
        (
            "request_scope".into(),
            DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
                ("started".into(), DgmValue::Int(clamp_u64_to_i64(request_scope.started))),
                ("completed".into(), DgmValue::Int(clamp_u64_to_i64(request_scope.completed))),
                (
                    "scopes_with_survivors".into(),
                    DgmValue::Int(clamp_u64_to_i64(request_scope.scopes_with_survivors)),
                ),
                (
                    "scopes_with_assertable_survivors".into(),
                    DgmValue::Int(clamp_u64_to_i64(request_scope.scopes_with_assertable_survivors)),
                ),
                (
                    "scopes_with_report_only_survivors".into(),
                    DgmValue::Int(clamp_u64_to_i64(request_scope.scopes_with_report_only_survivors)),
                ),
                (
                    "total_survivors".into(),
                    request_scope_survivor_value(request_scope.total_survivors),
                ),
                (
                    "last_survivors".into(),
                    request_scope_survivor_value(request_scope.last_survivors),
                ),
            ])))),
        ),
    ])
}

fn live_object_stats_value(snapshot: LiveObjectTypeSnapshot) -> DgmValue {
    DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
        ("live".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.live))),
        ("created".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.created))),
        ("dropped".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.dropped))),
        ("live_bytes".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.live_bytes))),
    ]))))
}

fn request_scope_survivor_value(snapshot: crate::request_scope::RequestScopeSurvivorSnapshot) -> DgmValue {
    DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
        ("environments".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.environments))),
        ("requests".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.requests))),
        ("request_maps".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.request_maps))),
        ("request_json".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.request_json))),
        ("json_views".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.json_views))),
        ("raw_json".into(), DgmValue::Int(clamp_u64_to_i64(snapshot.raw_json))),
    ]))))
}

pub(crate) fn runtime_reset_request_metrics() {
    *lock_request_metrics() = RequestMetricsState::default();
}

pub(crate) fn runtime_record_request(label: &str, status: i64, elapsed_ms: u64, auth_failed: bool) {
    let mut state = lock_request_metrics();
    state.requests = state.requests.saturating_add(1);
    if status >= 400 {
        state.errors = state.errors.saturating_add(1);
    }
    if auth_failed {
        state.auth_failures = state.auth_failures.saturating_add(1);
    }
    push_metric_sample(&mut state.latencies_ms, elapsed_ms, REQUEST_METRIC_HISTORY_LIMIT);
    let route = state.routes.entry(label.to_string()).or_default();
    route.requests = route.requests.saturating_add(1);
    if status >= 400 {
        route.errors = route.errors.saturating_add(1);
    }
    if auth_failed {
        route.auth_failures = route.auth_failures.saturating_add(1);
    }
    route.last_status = status;
    push_metric_sample(&mut route.latencies_ms, elapsed_ms, REQUEST_ROUTE_HISTORY_LIMIT);
}

pub(crate) fn runtime_request_metrics_value() -> DgmValue {
    let state = lock_request_metrics().clone();
    let mut routes = HashMap::new();
    let mut labels = state.routes.keys().cloned().collect::<Vec<_>>();
    labels.sort();
    for label in labels {
        if let Some(route) = state.routes.get(&label) {
            routes.insert(label, DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
                ("requests".into(), DgmValue::Int(clamp_u64_to_i64(route.requests))),
                ("errors".into(), DgmValue::Int(clamp_u64_to_i64(route.errors))),
                ("auth_failures".into(), DgmValue::Int(clamp_u64_to_i64(route.auth_failures))),
                ("last_status".into(), DgmValue::Int(route.last_status)),
                ("latency_ms".into(), latency_summary_value(&route.latencies_ms)),
            ])))));
        }
    }
    DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
        ("requests".into(), DgmValue::Int(clamp_u64_to_i64(state.requests))),
        ("errors".into(), DgmValue::Int(clamp_u64_to_i64(state.errors))),
        ("auth_failures".into(), DgmValue::Int(clamp_u64_to_i64(state.auth_failures))),
        ("latency_ms".into(), latency_summary_value(&state.latencies_ms)),
        ("routes".into(), DgmValue::Map(Rc::new(RefCell::new(routes)))),
    ]))))
}

fn default_max_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_mul(DEFAULT_MAX_THREADS_MULTIPLIER))
        .unwrap_or(4)
        .min(DEFAULT_MAX_THREADS_CAP)
        .max(1)
}

pub(crate) fn runtime_profile_value() -> DgmValue {
    let profile = runtime_profile_snapshot();
    runtime_profile_to_value(&profile)
}

pub(crate) fn runtime_profile_to_value(profile: &RuntimeProfile) -> DgmValue {
    let mut map = HashMap::new();
    map.insert("total_time".into(), DgmValue::Int(clamp_u128_to_i64(profile.total_time)));
    map.insert("total_time_ms".into(), DgmValue::Float(profile.total_time as f64 / 1_000_000.0));
    map.insert("function_time".into(), DgmValue::Map(Rc::new(RefCell::new(time_map_to_values(&profile.function_time)))));
    map.insert("function_calls".into(), DgmValue::Map(Rc::new(RefCell::new(count_map_to_values(&profile.function_calls)))));
    map.insert("module_time".into(), DgmValue::Map(Rc::new(RefCell::new(time_map_to_values(&profile.module_time)))));
    map.insert("alloc_count".into(), DgmValue::Int(clamp_u64_to_i64(profile.alloc_count)));
    map.insert("alloc_bytes".into(), DgmValue::Int(clamp_u64_to_i64(profile.alloc_bytes)));
    map.insert("alloc_by_label".into(), DgmValue::Map(Rc::new(RefCell::new(alloc_label_map_to_values(&profile.alloc_by_label)))));
    map.insert("peak_heap_bytes".into(), DgmValue::Int(clamp_u64_to_i64(profile.peak_heap_bytes)));
    map.insert("vm_instruction_count".into(), DgmValue::Int(clamp_u64_to_i64(profile.vm_instruction_count)));
    map.insert("vm_frame_count".into(), DgmValue::Int(clamp_u64_to_i64(profile.vm_frame_count)));
    map.insert("vm_time".into(), DgmValue::Int(clamp_u128_to_i64(profile.vm_time_ns)));
    map.insert("vm_time_ms".into(), DgmValue::Float(profile.vm_time_ns as f64 / 1_000_000.0));
    map.insert("vm_alloc_count".into(), DgmValue::Int(clamp_u64_to_i64(profile.vm_alloc_count)));
    map.insert("vm_alloc_bytes".into(), DgmValue::Int(clamp_u64_to_i64(profile.vm_alloc_bytes)));
    let avg_alloc = if profile.vm_frame_count == 0 {
        0.0
    } else {
        profile.vm_alloc_bytes as f64 / profile.vm_frame_count as f64
    };
    map.insert("vm_avg_alloc_bytes_per_frame".into(), DgmValue::Float(avg_alloc));
    map.insert("io_call_count".into(), DgmValue::Int(clamp_u64_to_i64(profile.io_call_count)));
    map.insert("io_time".into(), DgmValue::Int(clamp_u128_to_i64(profile.io_time_ns)));
    map.insert("io_time_ms".into(), DgmValue::Float(profile.io_time_ns as f64 / 1_000_000.0));
    DgmValue::Map(Rc::new(RefCell::new(map)))
}

fn runtime_check_wall_clock_locked(state: &RuntimeState) -> Result<(), DgmError> {
    let elapsed_ms = state.started_at.elapsed().as_millis();
    if elapsed_ms > state.max_wall_time_ms as u128 {
        Err(DgmError::RuntimeError { msg: "wall-clock limit exceeded".into() })
    } else {
        Ok(())
    }
}

fn clamp_u128_to_i64(value: u128) -> i64 {
    value.min(i64::MAX as u128) as i64
}

fn clamp_u64_to_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn time_map_to_values(source: &HashMap<String, u128>) -> HashMap<String, DgmValue> {
    source
        .iter()
        .map(|(key, value)| (key.clone(), DgmValue::Int(clamp_u128_to_i64(*value))))
        .collect()
}

fn count_map_to_values(source: &HashMap<String, u64>) -> HashMap<String, DgmValue> {
    source
        .iter()
        .map(|(key, value)| (key.clone(), DgmValue::Int(clamp_u64_to_i64(*value))))
        .collect()
}

fn alloc_label_map_to_values(source: &HashMap<String, AllocationProfileEntry>) -> HashMap<String, DgmValue> {
    source
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
                    ("count".into(), DgmValue::Int(clamp_u64_to_i64(value.count))),
                    ("bytes".into(), DgmValue::Int(clamp_u64_to_i64(value.bytes))),
                ])))),
            )
        })
        .collect()
}

fn callable_profile_name(value: &DgmValue) -> String {
    match value {
        DgmValue::Function { name: Some(name), .. } => name.clone(),
        DgmValue::Function { .. } => "<lambda>".into(),
        DgmValue::NativeFunction { name, .. } => name.clone(),
        DgmValue::NativeMethod { name, .. } => name.clone(),
        DgmValue::Class { name, .. } => format!("new {}", name),
        _ => "<call>".into(),
    }
}

fn build_function_value(
    name: Option<String>,
    params: Vec<Param>,
    body: Vec<Stmt>,
    closure: Rc<RefCell<Environment>>,
) -> DgmValue {
    let compiled = Some(vm::compile_function_blob(name.clone(), &params, &body));
    DgmValue::Function {
        name,
        params,
        body,
        closure,
        compiled,
    }
}

fn teardown_environment_graph(
    env: Rc<RefCell<Environment>>,
    seen_envs: &mut HashSet<usize>,
    seen_maps: &mut HashSet<usize>,
    seen_lists: &mut HashSet<usize>,
) {
    let ptr = Rc::as_ptr(&env) as usize;
    if !seen_envs.insert(ptr) {
        return;
    }
    let (parent, values) = {
        let borrowed = env.borrow();
        (borrowed.parent(), borrowed.entries())
    };
    for (_, value) in values {
        teardown_value_graph(&value, seen_envs, seen_maps, seen_lists);
    }
    if let Some(parent) = parent {
        teardown_environment_graph(parent, seen_envs, seen_maps, seen_lists);
    }
    env.borrow_mut().clear_values();
}

fn teardown_map_graph(
    map: &Rc<RefCell<HashMap<String, DgmValue>>>,
    seen_envs: &mut HashSet<usize>,
    seen_maps: &mut HashSet<usize>,
    seen_lists: &mut HashSet<usize>,
) {
    let ptr = Rc::as_ptr(map) as usize;
    if !seen_maps.insert(ptr) {
        return;
    }
    let values = map.borrow().values().cloned().collect::<Vec<_>>();
    for value in values {
        teardown_value_graph(&value, seen_envs, seen_maps, seen_lists);
    }
    map.borrow_mut().clear();
}

fn teardown_list_graph(
    list: &Rc<RefCell<Vec<DgmValue>>>,
    seen_envs: &mut HashSet<usize>,
    seen_maps: &mut HashSet<usize>,
    seen_lists: &mut HashSet<usize>,
) {
    let ptr = Rc::as_ptr(list) as usize;
    if !seen_lists.insert(ptr) {
        return;
    }
    let values = list.borrow().clone();
    for value in values {
        teardown_value_graph(&value, seen_envs, seen_maps, seen_lists);
    }
    list.borrow_mut().clear();
}

fn teardown_value_graph(
    value: &DgmValue,
    seen_envs: &mut HashSet<usize>,
    seen_maps: &mut HashSet<usize>,
    seen_lists: &mut HashSet<usize>,
) {
    match value {
        DgmValue::List(list) => teardown_list_graph(list, seen_envs, seen_maps, seen_lists),
        DgmValue::Map(map) => teardown_map_graph(map, seen_envs, seen_maps, seen_lists),
        DgmValue::Function { closure, .. } | DgmValue::Class { closure, .. } => {
            teardown_environment_graph(Rc::clone(closure), seen_envs, seen_maps, seen_lists);
        }
        DgmValue::NativeMethod { state, .. } => teardown_value_graph(state, seen_envs, seen_maps, seen_lists),
        DgmValue::Instance { fields, .. } => teardown_map_graph(fields, seen_envs, seen_maps, seen_lists),
        DgmValue::Int(_)
        | DgmValue::Float(_)
        | DgmValue::Str(_)
        | DgmValue::Bool(_)
        | DgmValue::Null
        | DgmValue::Request(_)
        | DgmValue::RequestShell(_)
        | DgmValue::RequestMap(_)
        | DgmValue::RequestMapView(_)
        | DgmValue::RequestJson(_)
        | DgmValue::Json(_)
        | DgmValue::RawJson(_)
        | DgmValue::NativeFunction { .. }
        | DgmValue::Channel(_)
        | DgmValue::TaskHandle(_)
        | DgmValue::Future(_) => {}
    }
}

fn sorted_map_entries(map: &HashMap<String, DgmValue>) -> Vec<(String, DgmValue)> {
    let mut entries = map
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

fn sorted_string_map_entries(map: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut entries = map
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

fn request_field_names() -> [&'static str; 8] {
    ["body", "headers", "json", "method", "params", "path", "query", "url"]
}

trait RequestAccess {
    fn method_text(&self) -> &str;
    fn url_text(&self) -> &str;
    fn path_text(&self) -> &str;
    fn headers_arc(&self) -> &Arc<HashMap<String, String>>;
    fn query_arc(&self) -> &Arc<HashMap<String, String>>;
    fn params_arc(&self) -> &Arc<HashMap<String, String>>;
    fn body_text(&self) -> &str;
    fn json_ref(&self) -> Rc<RequestJsonValue>;
}

impl RequestAccess for RequestValue {
    fn method_text(&self) -> &str {
        &self.method
    }

    fn url_text(&self) -> &str {
        &self.url
    }

    fn path_text(&self) -> &str {
        &self.path
    }

    fn headers_arc(&self) -> &Arc<HashMap<String, String>> {
        &self.headers_values
    }

    fn query_arc(&self) -> &Arc<HashMap<String, String>> {
        &self.query_values
    }

    fn params_arc(&self) -> &Arc<HashMap<String, String>> {
        &self.params_values
    }

    fn body_text(&self) -> &str {
        self.body.as_ref()
    }

    fn json_ref(&self) -> Rc<RequestJsonValue> {
        RequestValue::json_ref(self)
    }
}

impl RequestAccess for RequestShellValue {
    fn method_text(&self) -> &str {
        &self.backing.method
    }

    fn url_text(&self) -> &str {
        &self.backing.url
    }

    fn path_text(&self) -> &str {
        &self.backing.path
    }

    fn headers_arc(&self) -> &Arc<HashMap<String, String>> {
        &self.backing.headers_values
    }

    fn query_arc(&self) -> &Arc<HashMap<String, String>> {
        &self.backing.query_values
    }

    fn params_arc(&self) -> &Arc<HashMap<String, String>> {
        &self.backing.params_values
    }

    fn body_text(&self) -> &str {
        self.backing.body.as_ref()
    }

    fn json_ref(&self) -> Rc<RequestJsonValue> {
        RequestShellValue::json_ref(self)
    }
}

fn request_json_method_value(state: &Rc<RequestJsonValue>) -> DgmValue {
    DgmValue::NativeMethod {
        name: "http.request.json".into(),
        state: Box::new(DgmValue::RequestJson(Rc::clone(state))),
        func: crate::stdlib::http_mod::request_json_method,
    }
}

fn string_map_field_value(values: &HashMap<String, String>, field: &str) -> Result<DgmValue, DgmError> {
    values
        .get(field)
        .cloned()
        .map(DgmValue::Str)
        .ok_or_else(|| DgmError::RuntimeError { msg: format!("key '{}' not found", field) })
}

fn string_map_entries(values: &HashMap<String, String>) -> Vec<(String, DgmValue)> {
    sorted_string_map_entries(values)
        .into_iter()
        .map(|(key, value)| (key, DgmValue::Str(value)))
        .collect()
}

fn request_map_field_value(map: &RequestMapValue, field: &str) -> Result<DgmValue, DgmError> {
    string_map_field_value(map.values.as_ref(), field)
}

fn request_map_view_field_value(values: &Arc<HashMap<String, String>>, field: &str) -> Result<DgmValue, DgmError> {
    string_map_field_value(values.as_ref(), field)
}

fn request_map_entries(map: &RequestMapValue) -> Vec<(String, DgmValue)> {
    string_map_entries(map.values.as_ref())
}

fn request_map_view_entries(values: &Arc<HashMap<String, String>>) -> Vec<(String, DgmValue)> {
    string_map_entries(values.as_ref())
}

fn request_field_value<T: RequestAccess>(request: &T, field: &str) -> Result<DgmValue, DgmError> {
    match field {
        "method" => Ok(DgmValue::Str(request.method_text().to_owned())),
        "url" => Ok(DgmValue::Str(request.url_text().to_owned())),
        "path" => Ok(DgmValue::Str(request.path_text().to_owned())),
        "headers" => Ok(DgmValue::RequestMapView(Arc::clone(request.headers_arc()))),
        "query" => Ok(DgmValue::RequestMapView(Arc::clone(request.query_arc()))),
        "params" => Ok(DgmValue::RequestMapView(Arc::clone(request.params_arc()))),
        "body" => Ok(DgmValue::Str(request.body_text().to_owned())),
        "json" => Ok(request_json_method_value(&request.json_ref())),
        _ => Err(DgmError::RuntimeError { msg: format!("key '{}' not found", field) }),
    }
}

fn request_entries<T: RequestAccess>(request: &T) -> Vec<(String, DgmValue)> {
    request_field_names()
        .into_iter()
        .filter_map(|field| request_field_value(request, field).ok().map(|value| (field.to_string(), value)))
        .collect()
}

fn json_child_value(state: &Arc<JsonValueState>, segment: JsonPathSegment) -> DgmValue {
    let mut path = state.path.clone();
    path.push(segment);
    json_value_to_dgm(Arc::clone(&state.root), path)
}

fn json_entries(state: &Arc<JsonValueState>) -> Result<Vec<(String, DgmValue)>, DgmError> {
    match json_resolved_value(state) {
        serde_json::Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            Ok(keys
                .into_iter()
                .map(|key| {
                    let value = json_child_value(state, JsonPathSegment::Key(key.clone()));
                    (key, value)
                })
                .collect())
        }
        _ => Err(DgmError::RuntimeError { msg: "json value is not an object".into() }),
    }
}

fn json_array_values(state: &Arc<JsonValueState>) -> Result<Vec<DgmValue>, DgmError> {
    match json_resolved_value(state) {
        serde_json::Value::Array(items) => Ok((0..items.len())
            .map(|index| json_child_value(state, JsonPathSegment::Index(index)))
            .collect()),
        _ => Err(DgmError::RuntimeError { msg: "json value is not an array".into() }),
    }
}

fn json_field_value(state: &Arc<JsonValueState>, field: &str) -> Result<DgmValue, DgmError> {
    match json_resolved_value(state) {
        serde_json::Value::Object(map) => {
            if map.contains_key(field) {
                Ok(json_child_value(state, JsonPathSegment::Key(field.to_string())))
            } else {
                Err(DgmError::RuntimeError { msg: format!("key '{}' not found", field) })
            }
        }
        serde_json::Value::Array(items) if field == "length" => Ok(DgmValue::Int(items.len() as i64)),
        _ => Err(DgmError::RuntimeError { msg: format!("cannot access field '{}' on json value", field) }),
    }
}

fn json_index_value(state: &Arc<JsonValueState>, index: &DgmValue) -> Result<DgmValue, DgmError> {
    match (json_resolved_value(state), index) {
        (serde_json::Value::Object(map), DgmValue::Str(key)) => {
            if map.contains_key(key) {
                Ok(json_child_value(state, JsonPathSegment::Key(key.clone())))
            } else {
                Err(DgmError::RuntimeError { msg: format!("key '{}' not found", key) })
            }
        }
        (serde_json::Value::Array(items), DgmValue::Int(i)) => {
            let idx = if *i < 0 { items.len() as i64 + i } else { *i };
            if idx < 0 {
                return Err(DgmError::RuntimeError { msg: "json index out of range".into() });
            }
            let idx = idx as usize;
            if idx >= items.len() {
                return Err(DgmError::RuntimeError { msg: "json index out of range".into() });
            }
            Ok(json_child_value(state, JsonPathSegment::Index(idx)))
        }
        (serde_json::Value::Array(_), _) => Err(DgmError::RuntimeError { msg: "json array index requires int".into() }),
        (serde_json::Value::Object(_), _) => Err(DgmError::RuntimeError { msg: "json object index requires string".into() }),
        _ => Err(DgmError::RuntimeError { msg: "json value is not indexable".into() }),
    }
}

fn map_like_contains(value: &DgmValue, key: &str) -> bool {
    match value {
        DgmValue::Map(map) => map.borrow().contains_key(key),
        DgmValue::Request(_) => request_field_names().contains(&key),
        DgmValue::RequestShell(_) => request_field_names().contains(&key),
        DgmValue::RequestMap(map) => map.values.contains_key(key),
        DgmValue::RequestMapView(values) => values.contains_key(key),
        DgmValue::Json(state) => matches!(json_resolved_value(state), serde_json::Value::Object(map) if map.contains_key(key)),
        _ => false,
    }
}

fn map_like_entries(value: &DgmValue) -> Option<Vec<(String, DgmValue)>> {
    match value {
        DgmValue::Map(map) => Some(sorted_map_entries(&map.borrow())),
        DgmValue::Request(request) => Some(request_entries(request.as_ref())),
        DgmValue::RequestShell(request) => Some(request_entries(request.as_ref())),
        DgmValue::RequestMap(map) => Some(request_map_entries(map)),
        DgmValue::RequestMapView(values) => Some(request_map_view_entries(values)),
        DgmValue::Json(state) => json_entries(state).ok(),
        _ => None,
    }
}

fn field_access_value(value: &DgmValue, field: &str) -> Result<DgmValue, DgmError> {
    match value {
        DgmValue::Instance { fields, .. } => fields
            .borrow()
            .get(field)
            .cloned()
            .ok_or_else(|| DgmError::RuntimeError { msg: format!("no field '{}'", field) }),
        DgmValue::Map(map) => map
            .borrow()
            .get(field)
            .cloned()
            .ok_or_else(|| DgmError::RuntimeError { msg: format!("key '{}' not found", field) }),
        DgmValue::Request(request) => request_field_value(request.as_ref(), field),
        DgmValue::RequestShell(request) => request_field_value(request.as_ref(), field),
        DgmValue::RequestMap(map) => request_map_field_value(map, field),
        DgmValue::RequestMapView(values) => request_map_view_field_value(values, field),
        DgmValue::Json(state) => json_field_value(state, field),
        DgmValue::Str(text) => match field {
            "length" => Ok(DgmValue::Int(text.chars().count() as i64)),
            _ => Err(DgmError::RuntimeError { msg: format!("no property '{}' on string", field) }),
        },
        DgmValue::List(list) => match field {
            "length" => Ok(DgmValue::Int(list.borrow().len() as i64)),
            _ => Err(DgmError::RuntimeError { msg: format!("no property '{}' on list", field) }),
        },
        _ => Err(DgmError::RuntimeError { msg: format!("cannot access field '{}' on {}", field, value) }),
    }
}

fn index_access_value(value: &DgmValue, index: &DgmValue) -> Result<DgmValue, DgmError> {
    match (value, index) {
        (DgmValue::List(list), DgmValue::Int(i)) => {
            let list = list.borrow();
            let idx = if *i < 0 { list.len() as i64 + i } else { *i };
            if idx < 0 {
                return Err(DgmError::RuntimeError { msg: "list index out of range".into() });
            }
            let idx = idx as usize;
            list.get(idx).cloned().ok_or_else(|| DgmError::RuntimeError { msg: "list index out of range".into() })
        }
        (DgmValue::Map(map), DgmValue::Str(key)) => map
            .borrow()
            .get(key)
            .cloned()
            .ok_or_else(|| DgmError::RuntimeError { msg: format!("key '{}' not found", key) }),
        (DgmValue::Request(request), DgmValue::Str(key)) => request_field_value(request.as_ref(), key),
        (DgmValue::RequestShell(request), DgmValue::Str(key)) => request_field_value(request.as_ref(), key),
        (DgmValue::RequestMap(map), DgmValue::Str(key)) => request_map_field_value(map, key),
        (DgmValue::RequestMapView(values), DgmValue::Str(key)) => request_map_view_field_value(values, key),
        (DgmValue::Json(state), idx) => json_index_value(state, idx),
        (DgmValue::Str(text), DgmValue::Int(i)) => {
            let len = text.chars().count() as i64;
            let idx = if *i < 0 { len + i } else { *i };
            if idx < 0 {
                return Err(DgmError::RuntimeError { msg: "string index out of range".into() });
            }
            let idx = idx as usize;
            text.chars()
                .nth(idx)
                .map(|c| DgmValue::Str(c.to_string()))
                .ok_or_else(|| DgmError::RuntimeError { msg: "string index out of range".into() })
        }
        _ => Err(DgmError::RuntimeError { msg: "invalid index operation".into() }),
    }
}

fn module_path_from_env(env: &Rc<RefCell<Environment>>) -> Option<PathBuf> {
    match env.borrow().get("__module_path__") {
        Some(DgmValue::Str(path)) if !path.is_empty() => Some(PathBuf::from(path)),
        _ => None,
    }
}

pub(crate) fn reserve_string_bytes(len: usize) -> Result<(), DgmError> {
    runtime_reserve(mem::size_of::<String>() + len)
}

pub(crate) fn reserve_list_growth(additional_items: usize) -> Result<(), DgmError> {
    runtime_reserve(mem::size_of::<Vec<DgmValue>>() + additional_items.saturating_mul(mem::size_of::<DgmValue>()))
}

pub(crate) fn reserve_map_growth(additional_entries: usize, key_bytes: usize) -> Result<(), DgmError> {
    runtime_reserve(
        mem::size_of::<HashMap<String, DgmValue>>()
            + additional_entries.saturating_mul(mem::size_of::<(String, DgmValue)>() + 16)
            + key_bytes
    )
}

fn estimate_value_size_inner(value: &DgmValue, seen_lists: &mut HashSet<usize>, seen_maps: &mut HashSet<usize>) -> usize {
    match value {
        DgmValue::Null => 0,
        DgmValue::Bool(_) => mem::size_of::<bool>(),
        DgmValue::Int(_) => mem::size_of::<i64>(),
        DgmValue::Float(_) => mem::size_of::<f64>(),
        DgmValue::Str(s) => mem::size_of::<String>() + s.len(),
        DgmValue::List(items) => {
            let ptr = Rc::as_ptr(items) as usize;
            if !seen_lists.insert(ptr) {
                return 0;
            }
            mem::size_of::<Vec<DgmValue>>()
                + items.borrow().iter().map(|item| estimate_value_size_inner(item, seen_lists, seen_maps)).sum::<usize>()
        }
        DgmValue::Map(map) => {
            let ptr = Rc::as_ptr(map) as usize;
            if !seen_maps.insert(ptr) {
                return 0;
            }
            mem::size_of::<HashMap<String, DgmValue>>()
                + map.borrow().iter().map(|(key, value)| key.len() + estimate_value_size_inner(value, seen_lists, seen_maps)).sum::<usize>()
        }
        DgmValue::Request(request) => {
            request.method.len()
                + request.url.len()
                + request.path.len()
                + request.body.len()
                + request.headers_values().iter().map(|(key, value)| key.len() + value.len()).sum::<usize>()
                + request.query_values().iter().map(|(key, value)| key.len() + value.len()).sum::<usize>()
                + request.params_values().iter().map(|(key, value)| key.len() + value.len()).sum::<usize>()
                + request.content_type_text().len()
        }
        DgmValue::RequestShell(request) => {
            request.backing.method.len()
                + request.backing.url.len()
                + request.backing.path.len()
                + request.backing.body.len()
                + request.headers_values().iter().map(|(key, value)| key.len() + value.len()).sum::<usize>()
                + request.query_values().iter().map(|(key, value)| key.len() + value.len()).sum::<usize>()
                + request.params_values().iter().map(|(key, value)| key.len() + value.len()).sum::<usize>()
                + request.content_type_text().len()
        }
        DgmValue::RequestMap(map) => {
            map.values.iter().map(|(key, value)| key.len() + value.len()).sum::<usize>()
        }
        DgmValue::RequestMapView(values) => {
            values.iter().map(|(key, value)| key.len() + value.len()).sum::<usize>()
        }
        DgmValue::RequestJson(state) => {
            let cached = state
                .parsed
                .borrow()
                .as_ref()
                .map(|result| match result {
                    Ok(value) => estimate_value_size_inner(value, seen_lists, seen_maps),
                    Err(error) => error.len(),
                })
                .unwrap_or(0);
            state.content_type.len() + state.body.len() + cached
        }
        DgmValue::Json(state) => match json_resolved_value(state) {
            serde_json::Value::Null => 0,
            serde_json::Value::Bool(_) => mem::size_of::<bool>(),
            serde_json::Value::Number(_) => mem::size_of::<serde_json::Number>(),
            serde_json::Value::String(text) => mem::size_of::<String>() + text.len(),
            serde_json::Value::Array(items) => {
                mem::size_of::<Vec<serde_json::Value>>()
                    + items.len().saturating_mul(mem::size_of::<serde_json::Value>())
            }
            serde_json::Value::Object(map) => {
                mem::size_of::<serde_json::Map<String, serde_json::Value>>()
                    + map
                        .iter()
                        .map(|(key, _)| mem::size_of::<String>() + key.len() + mem::size_of::<serde_json::Value>())
                        .sum::<usize>()
            }
        },
        DgmValue::RawJson(text) => {
            mem::size_of::<RawJsonValue>()
                + text.segments().iter().map(|segment| mem::size_of::<Arc<str>>() + segment.len()).sum::<usize>()
        }
        DgmValue::Function { name, params, .. } => {
            name.as_ref().map(|name| name.len()).unwrap_or(0)
                + params.iter().map(|p| p.name.len() + p.default.as_ref().map(|_| 8).unwrap_or(0)).sum::<usize>()
                + 64
        }
        DgmValue::NativeFunction { name, .. } => name.len() + 32,
        DgmValue::NativeMethod { name, state, .. } => name.len() + estimate_value_size_inner(state, seen_lists, seen_maps) + 48,
        DgmValue::Channel(_) | DgmValue::TaskHandle(_) | DgmValue::Future(_) => 48,
        DgmValue::Class { name, parent, methods, .. } => {
            name.len() + parent.as_ref().map(|p| p.len()).unwrap_or(0) + methods.len() * 32 + 64
        }
        DgmValue::Instance { class_name, fields } => {
            class_name.len()
                + mem::size_of::<HashMap<String, DgmValue>>()
                + {
                    let ptr = Rc::as_ptr(fields) as usize;
                    if seen_maps.insert(ptr) {
                        fields.borrow().iter().map(|(key, value)| key.len() + estimate_value_size_inner(value, seen_lists, seen_maps)).sum::<usize>()
                    } else {
                        0
                    }
                }
        }
    }
}

pub(crate) fn estimate_value_size(value: &DgmValue) -> usize {
    estimate_value_size_inner(value, &mut HashSet::new(), &mut HashSet::new())
}

pub(crate) fn runtime_reserve_value(value: &DgmValue) -> Result<(), DgmError> {
    runtime_reserve(estimate_value_size(value))
}

pub(crate) fn runtime_reserve_labeled(label: &str, bytes: usize) -> Result<(), DgmError> {
    let _guard = runtime_enter_alloc_profile(label);
    runtime_reserve(bytes)
}

pub(crate) fn runtime_reserve_value_labeled(label: &str, value: &DgmValue) -> Result<(), DgmError> {
    let _guard = runtime_enter_alloc_profile(label);
    runtime_reserve_value(value)
}

pub(crate) fn runtime_reserve_restored_payload_labeled(label: &str, value: &DgmValue) -> Result<(), DgmError> {
    match value {
        DgmValue::RequestShell(_) => runtime_reserve_labeled(label, mem::size_of::<RequestShellValue>()),
        _ => runtime_reserve_value_labeled(label, value),
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let normalized: String = key.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect();
    [
        "authorization",
        "apikey",
        "secret",
        "token",
        "password",
        "cookie",
        "setcookie",
    ].iter().any(|needle| normalized.contains(needle))
}

fn masked_secret_value(value: &DgmValue) -> String {
    match value {
        DgmValue::Str(raw) => {
            let trimmed = raw.trim();
            let (prefix, secret) = if let Some(rest) = trimmed.strip_prefix("Bearer ") {
                ("Bearer ", rest)
            } else if let Some(rest) = trimmed.strip_prefix("bearer ") {
                ("bearer ", rest)
            } else {
                ("", trimmed)
            };
            let suffix: String = secret.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect();
            if suffix.is_empty() {
                format!("{prefix}****")
            } else {
                format!("{prefix}****{}", suffix)
            }
        }
        _ => "****".into(),
    }
}

impl fmt::Display for DgmValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DgmValue::Int(n) => write!(f, "{}", n),
            DgmValue::Float(n) => write!(f, "{}", n),
            DgmValue::Str(s) => write!(f, "{}", s),
            DgmValue::Bool(b) => write!(f, "{}", if *b { "tru" } else { "fals" }),
            DgmValue::Null => write!(f, "nul"),
            DgmValue::List(l) => { let items: Vec<String> = l.borrow().iter().map(|v| format!("{}", v)).collect(); write!(f, "[{}]", items.join(", ")) }
            DgmValue::Map(m) => {
                let pairs: Vec<String> = sorted_map_entries(&m.borrow()).into_iter().map(|(k, v)| {
                    if is_sensitive_key(&k) {
                        format!("{}: {}", k, masked_secret_value(&v))
                    } else {
                        format!("{}: {}", k, v)
                    }
                }).collect();
                write!(f, "{{{}}}", pairs.join(", "))
            }
            DgmValue::Request(request) => {
                let pairs: Vec<String> = request_entries(request.as_ref()).into_iter().map(|(k, v)| {
                    if is_sensitive_key(&k) {
                        format!("{}: {}", k, masked_secret_value(&v))
                    } else {
                        format!("{}: {}", k, v)
                    }
                }).collect();
                write!(f, "{{{}}}", pairs.join(", "))
            }
            DgmValue::RequestShell(request) => {
                let pairs: Vec<String> = request_entries(request.as_ref()).into_iter().map(|(k, v)| {
                    if is_sensitive_key(&k) {
                        format!("{}: {}", k, masked_secret_value(&v))
                    } else {
                        format!("{}: {}", k, v)
                    }
                }).collect();
                write!(f, "{{{}}}", pairs.join(", "))
            }
            DgmValue::RequestMap(map) => {
                let pairs: Vec<String> = request_map_entries(map).into_iter().map(|(k, v)| {
                    if is_sensitive_key(&k) {
                        format!("{}: {}", k, masked_secret_value(&v))
                    } else {
                        format!("{}: {}", k, v)
                    }
                }).collect();
                write!(f, "{{{}}}", pairs.join(", "))
            }
            DgmValue::RequestMapView(values) => {
                let pairs: Vec<String> = request_map_view_entries(values).into_iter().map(|(k, v)| {
                    if is_sensitive_key(&k) {
                        format!("{}: {}", k, masked_secret_value(&v))
                    } else {
                        format!("{}: {}", k, v)
                    }
                }).collect();
                write!(f, "{{{}}}", pairs.join(", "))
            }
            DgmValue::RequestJson(_) => write!(f, "<request_json>"),
            DgmValue::Json(state) => match json_resolved_value(state) {
                serde_json::Value::Null => write!(f, "nul"),
                serde_json::Value::Bool(value) => write!(f, "{}", if *value { "tru" } else { "fals" }),
                serde_json::Value::Number(number) => write!(f, "{}", number),
                serde_json::Value::String(text) => write!(f, "{}", text),
                serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                    write!(f, "{}", serde_json::to_string(json_resolved_value(state)).unwrap_or_else(|_| "<json>".into()))
                }
            },
            DgmValue::RawJson(text) => write!(f, "{}", text.to_string_lossy()),
            DgmValue::Function { name, params, .. } => {
                let params = params.iter().map(|param| param.name.clone()).collect::<Vec<_>>().join(", ");
                match name {
                    Some(name) => write!(f, "<fn {}({})>", name, params),
                    None => write!(f, "<fn({})>", params),
                }
            },
            DgmValue::NativeFunction { name, .. } => write!(f, "<native {}>", name),
            DgmValue::NativeMethod { name, .. } => write!(f, "<native {}>", name),
            DgmValue::Channel(_) => write!(f, "<channel>"),
            DgmValue::TaskHandle(_) => write!(f, "<task>"),
            DgmValue::Future(_) => write!(f, "<future>"),
            DgmValue::Class { name, .. } => write!(f, "<class {}>", name),
            DgmValue::Instance { class_name, .. } => write!(f, "<{} instance>", class_name),
        }
    }
}

pub enum ControlFlow { None, Return(DgmValue), Break, Continue }

#[derive(Clone)]
struct EvaluatedArg {
    name: Option<String>,
    value: DgmValue,
}

pub struct Interpreter {
    pub globals: Rc<RefCell<Environment>>,
    module_cache: HashMap<String, DgmValue>,
    loading_modules: HashSet<String>,
    bundle: Option<Arc<BundleImage>>,
    current_paths: Vec<PathBuf>,
    current_module_keys: Vec<String>,
}

impl Interpreter {
    pub fn new() -> Self {
        runtime_reset_all();
        let globals = new_environment_ref();
        let natives: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
            ("len", native_len), ("type", native_type), ("str", native_str),
            ("int", native_int), ("float", native_float), ("push", native_push),
            ("pop", native_pop), ("range", native_range), ("input", native_input),
            ("abs", native_abs), ("min", native_min), ("max", native_max),
            ("sort", native_sort), ("reverse", native_reverse), ("keys", native_keys),
            ("values", native_values), ("has_key", native_has_key),
            ("slice", native_slice), ("join", native_join), ("split", native_split),
            ("replace", native_replace), ("upper", native_upper), ("lower", native_lower),
            ("trim", native_trim), ("contains", native_contains),
            ("starts_with", native_starts_with), ("ends_with", native_ends_with),
            ("chars", native_chars), ("format", native_format),
            ("map", native_map_fn), ("filter", native_filter),
            ("reduce", native_reduce), ("each", native_each),
            ("find", native_find), ("index_of", native_index_of),
            ("flat", native_flat), ("zip", native_zip),
            ("sum", native_sum), ("any", native_any), ("all", native_all),
            ("print", native_print), ("println", native_println),
            ("repr", native_repr), ("dbg", native_dbg),
            ("assert", native_assert), ("assert_eq", native_assert_eq),
            ("panic", native_panic),
            ("spawn", crate::concurrency::native_spawn),
            ("channel", crate::concurrency::native_channel),
            ("chr", native_chr), ("ord", native_ord),
            ("hex", native_hex), ("bin", native_bin),
            ("exit", native_exit),
        ];
        for (name, func) in natives {
            globals.borrow_mut().set(name, DgmValue::NativeFunction { name: name.to_string(), func: *func });
        }
        for (alias, target) in [
            ("prnt", "print"),
            ("prln", "println"),
            ("asrt", "assert"),
            ("asrt_eq", "assert_eq"),
            ("ext", "exit"),
        ] {
            let value = globals.borrow().get(target);
            if let Some(value) = value {
                globals.borrow_mut().set(alias, value);
            }
        }
        let env_map: HashMap<String, DgmValue> = std::env::vars().map(|(k, v)| (k, DgmValue::Str(v))).collect();
        globals.borrow_mut().set("env", DgmValue::Map(Rc::new(RefCell::new(env_map))));
        let args_list: Vec<DgmValue> = std::env::args().map(DgmValue::Str).collect();
        globals.borrow_mut().set("args", DgmValue::List(Rc::new(RefCell::new(args_list))));
        let argv_value = globals.borrow().get("args");
        if let Some(value) = argv_value {
            globals.borrow_mut().set("argv", value);
        }
        Self {
            globals,
            module_cache: HashMap::new(),
            loading_modules: HashSet::new(),
            bundle: None,
            current_paths: vec![],
            current_module_keys: vec![],
        }
    }

    pub fn set_entry_path(&mut self, path: Option<PathBuf>) {
        self.current_paths.clear();
        self.current_module_keys.clear();
        if let Some(path) = path {
            self.current_paths.push(path);
        }
    }

    pub fn run_bundle(&mut self, bundle: BundleImage) -> Result<(), DgmError> {
        runtime_reset_all();
        let runtime = bundle::runtime_config_from_bundle(&bundle.manifest().runtime);
        runtime_apply_config(&runtime)?;
        for source in bundle.manifest().debug_sources.values() {
            if !source.source.is_empty() {
                register_source(source.path.clone(), source.source.clone());
            }
        }
        let bundle = Arc::new(bundle);
        let entry = bundle.entry_module()?;
        self.bundle = Some(Arc::clone(&bundle));
        self.current_paths.clear();
        self.current_module_keys.clear();
        self.current_paths.push(PathBuf::from(&entry.logical_path));
        self.current_module_keys.push(entry.key.clone());
        let result = self.run_loaded_module(&entry);
        self.current_module_keys.pop();
        self.current_paths.pop();
        result
    }

    pub fn bundle_stats(&self) -> Option<BundleRuntimeStats> {
        self.bundle.as_ref().map(|bundle| bundle.runtime_stats())
    }

    pub fn run(&mut self, stmts: Vec<Stmt>) -> Result<(), DgmError> {
        runtime_reset_usage();
        let module_name = self
            .current_paths
            .last()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<main>".into());
        let _profile_guard = runtime_enter_module_profile(&module_name);
        let env = Rc::clone(&self.globals);
        env.borrow_mut().set("__module_path__", DgmValue::Str(module_name.clone()));
        let module = vm::compile_module(&stmts, Some(module_name));
        match vm::execute_module(self, &module, env)? {
            ControlFlow::None | ControlFlow::Return(_) | ControlFlow::Break | ControlFlow::Continue => {}
        }
        Ok(())
    }

    fn run_loaded_module(&mut self, module: &bundle::BundleModule) -> Result<(), DgmError> {
        runtime_reset_usage();
        let module_name = self
            .current_paths
            .last()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<main>".into());
        let _profile_guard = runtime_enter_module_profile(&module_name);
        let env = Rc::clone(&self.globals);
        env.borrow_mut().set("__module_path__", DgmValue::Str(module_name));
        if let Some(bytecode) = &module.bytecode {
            let _ = vm::execute_module(self, bytecode, env)?;
        } else {
            for stmt in &module.stmts {
                match self.exec_stmt(stmt, Rc::clone(&env))? {
                    ControlFlow::None => {}
                    ControlFlow::Return(_) | ControlFlow::Break | ControlFlow::Continue => break,
                }
            }
        }
        Ok(())
    }

    fn execute_module_as_map(
        &mut self,
        cache_key: &str,
        logical_path: PathBuf,
        module_key: Option<String>,
        stmts: &[Stmt],
        bytecode: Option<&vm::BytecodeModule>,
    ) -> Result<DgmValue, DgmError> {
        if !self.loading_modules.insert(cache_key.to_string()) {
            let label = module_key.as_deref().unwrap_or_else(|| logical_path.to_str().unwrap_or("module"));
            return Err(DgmError::ImportError { msg: format!("circular import detected for '{}'", label) });
        }
        let module_name = logical_path.display().to_string();
        let _profile_guard = runtime_enter_module_profile(&module_name);
        let module_env = new_child_environment_ref(Rc::clone(&self.globals));
        module_env.borrow_mut().set("__module_path__", DgmValue::Str(module_name.clone()));
        self.current_paths.push(logical_path);
        if let Some(module_key) = &module_key {
            self.current_module_keys.push(module_key.clone());
        }
        let result = (|| -> Result<DgmValue, DgmError> {
            if let Some(bytecode) = bytecode {
                let _ = vm::execute_module(self, bytecode, Rc::clone(&module_env))?;
            } else {
                let compiled = vm::compile_module(stmts, Some(module_name.clone()));
                let _ = vm::execute_module(self, &compiled, Rc::clone(&module_env))?;
            }
            let exports = module_env
                .borrow()
                .entries()
                .into_iter()
                .filter(|(name, _)| !name.starts_with("__"))
                .collect::<HashMap<_, _>>();
            Ok(DgmValue::Map(Rc::new(RefCell::new(exports))))
        })();
        if module_key.is_some() {
            self.current_module_keys.pop();
        }
        self.current_paths.pop();
        self.loading_modules.remove(cache_key);
        let module = result?;
        self.module_cache.insert(cache_key.to_string(), module.clone());
        Ok(module)
    }

    fn load_bundled_module(&mut self, name: &str) -> Result<Option<(String, DgmValue)>, DgmError> {
        let Some(bundle) = self.bundle.clone() else {
            return Ok(None);
        };
        let Some(module_key) = bundle.resolve_import_key(self.current_module_keys.last().map(String::as_str), name) else {
            if !self.current_module_keys.is_empty() {
                if bundle.manifest().build_mode == "debug" {
                    return Ok(None);
                }
                return Err(DgmError::ImportError { msg: format!("bundle does not contain import '{}'", name) });
            }
            return Ok(None);
        };
        let module = bundle
            .load_module(&module_key)?
            .ok_or_else(|| DgmError::ImportError { msg: format!("bundle is missing module '{}'", module_key) })?;
        let binding_name = module_binding_name(name, Some(Path::new(&module.logical_path)));
        let cache_key = format!("@bundle:{}", module_key);
        if let Some(value) = self.module_cache.get(&cache_key).cloned() {
            return Ok(Some((binding_name, value)));
        }
        let value = self.execute_module_as_map(
            &cache_key,
            PathBuf::from(&module.logical_path),
            Some(module.key.clone()),
            &module.stmts,
            module.bytecode.as_ref(),
        )?;
        Ok(Some((binding_name, value)))
    }

    pub(crate) fn exec_stmt(&mut self, stmt: &Stmt, env: Rc<RefCell<Environment>>) -> Result<ControlFlow, DgmError> {
        runtime_tick()?;
        let result = trap_dgm("statement execution", || {
            match &stmt.kind {
                StmtKind::Expr(expr) => { self.eval_expr(expr, env)?; Ok(ControlFlow::None) }
                StmtKind::Let { pattern, value } => {
                    let v = self.eval_expr(value, Rc::clone(&env))?;
                    self.bind_pattern(pattern, Some(v), Rc::clone(&env))?;
                    Ok(ControlFlow::None)
                }
                StmtKind::Writ(expr) => { let v = self.eval_expr(expr, env)?; println!("{}", v); Ok(ControlFlow::None) }
                StmtKind::If { condition, then_block, elseif_branches, else_block } => {
                    let cond_val = self.eval_expr(condition, Rc::clone(&env))?;
                    if self.is_truthy(&cond_val) {
                        return self.exec_block(then_block, Rc::clone(&env));
                    }
                    for (cond_expr, block) in elseif_branches {
                        let branch_val = self.eval_expr(cond_expr, Rc::clone(&env))?;
                        if self.is_truthy(&branch_val) {
                            return self.exec_block(block, Rc::clone(&env));
                        }
                    }
                    if let Some(block) = else_block { return self.exec_block(block, Rc::clone(&env)); }
                    Ok(ControlFlow::None)
                }
                StmtKind::While { condition, body } => {
                    loop {
                        let cond_val = self.eval_expr(condition, Rc::clone(&env))?;
                        if !self.is_truthy(&cond_val) { break; }
                        match self.exec_block(body, Rc::clone(&env))? {
                            ControlFlow::Break => break,
                            ControlFlow::Return(v) => return Ok(ControlFlow::Return(v)),
                            _ => {}
                        }
                    }
                    Ok(ControlFlow::None)
                }
                StmtKind::For { var, iterable, body } => {
                    let iter_val = self.eval_expr(iterable, Rc::clone(&env))?;
                    let items = match iter_val {
                        DgmValue::List(l) => l.borrow().clone(),
                        DgmValue::Str(s) => s.chars().map(|c| DgmValue::Str(c.to_string())).collect(),
                        DgmValue::Map(_) | DgmValue::Request(_) | DgmValue::RequestShell(_) | DgmValue::RequestMap(_) | DgmValue::RequestMapView(_) => map_like_entries(&iter_val)
                            .unwrap_or_default()
                            .into_iter()
                            .map(|(key, _)| DgmValue::Str(key))
                            .collect(),
                        DgmValue::Json(state) => match json_resolved_value(&state) {
                            serde_json::Value::Array(_) => json_array_values(&state)?,
                            serde_json::Value::Object(_) => json_entries(&state)?
                                .into_iter()
                                .map(|(key, _)| DgmValue::Str(key))
                                .collect(),
                            _ => return Err(DgmError::RuntimeError { msg: "for loop requires iterable".into() }),
                        },
                        _ => return Err(DgmError::RuntimeError { msg: "for loop requires iterable".into() }),
                    };
                    for item in items {
                        let loop_env = new_child_environment_ref(Rc::clone(&env));
                        loop_env.borrow_mut().set(var, item);
                        match self.exec_block(body, loop_env)? {
                            ControlFlow::Break => break,
                            ControlFlow::Return(v) => return Ok(ControlFlow::Return(v)),
                            _ => {}
                        }
                    }
                    Ok(ControlFlow::None)
                }
                StmtKind::FuncDef { name, params, body } => {
                    let func = build_function_value(
                        Some(name.clone()),
                        params.clone(),
                        body.clone(),
                        Rc::clone(&env),
                    );
                    env.borrow_mut().set(name, func);
                    Ok(ControlFlow::None)
                }
                StmtKind::Return(expr) => {
                    let v = if let Some(e) = expr { self.eval_expr(e, env)? } else { DgmValue::Null };
                    Ok(ControlFlow::Return(v))
                }
                StmtKind::Break => Ok(ControlFlow::Break),
                StmtKind::Continue => Ok(ControlFlow::Continue),
                StmtKind::ClassDef { name, parent, methods } => {
                    let class = DgmValue::Class {
                        name: name.clone(),
                        parent: parent.clone(),
                        methods: methods.clone(),
                        closure: Rc::clone(&env),
                    };
                    env.borrow_mut().set(name, class);
                    Ok(ControlFlow::None)
                }
                StmtKind::TryCatch { try_block, catch_var, catch_block, finally_block } => {
                    let result = self.exec_block(try_block, Rc::clone(&env));
                    let cf = match result {
                        Ok(cf) => cf,
                        Err(e) => {
                            let catch_env = new_child_environment_ref(Rc::clone(&env));
                            if let Some(var) = catch_var {
                                let caught = e
                                    .thrown_value()
                                    .map(|value| dgm_from_thrown_value(&value))
                                    .unwrap_or_else(|| DgmValue::Str(format!("{}", e)));
                                catch_env.borrow_mut().set(var, caught);
                            }
                            self.exec_block(catch_block, catch_env)?
                        }
                    };
                    if let Some(fb) = finally_block { self.exec_block(fb, Rc::clone(&env))?; }
                    Ok(cf)
                }
                StmtKind::Throw(expr) => {
                    let v = self.eval_expr(expr, env)?;
                    Err(DgmError::ThrownError { value: thrown_value_from_dgm(&v) })
                }
                StmtKind::Match { expr, arms } => {
                    let val = self.eval_expr(expr, Rc::clone(&env))?;
                    for arm in arms {
                        let arm_env = new_child_environment_ref(Rc::clone(&env));
                        if self.match_pattern(&arm.pattern, &val, Rc::clone(&arm_env))? {
                            if let Some(guard) = &arm.guard {
                                let guard_value = self.eval_expr(guard, Rc::clone(&arm_env))?;
                                if !self.is_truthy(&guard_value) {
                                    continue;
                                }
                            }
                            return self.exec_block(&arm.body, arm_env);
                        }
                    }
                    Ok(ControlFlow::None)
                }
                StmtKind::Imprt(name) => { self.do_import(name, env)?; Ok(ControlFlow::None) }
            }
        });
        result.map_err(|err| self.attach_stmt_frame(err, stmt.span))
    }

    fn exec_block(&mut self, stmts: &[Stmt], parent_env: Rc<RefCell<Environment>>) -> Result<ControlFlow, DgmError> {
        let env = new_child_environment_ref(parent_env);
        for stmt in stmts {
            match self.exec_stmt(stmt, Rc::clone(&env))? {
                ControlFlow::None => {}
                cf => return Ok(cf),
            }
        }
        Ok(ControlFlow::None)
    }

    pub(crate) fn eval_expr(&mut self, expr: &Expr, env: Rc<RefCell<Environment>>) -> Result<DgmValue, DgmError> {
        runtime_tick()?;
        match expr {
            Expr::IntLit(n) => Ok(DgmValue::Int(*n)),
            Expr::FloatLit(f) => Ok(DgmValue::Float(*f)),
            Expr::StringLit(s) => {
                reserve_string_bytes(s.len())?;
                Ok(DgmValue::Str(s.clone()))
            }
            Expr::BoolLit(b) => Ok(DgmValue::Bool(*b)),
            Expr::NullLit => Ok(DgmValue::Null),
            Expr::This => env.borrow().get("__self__").ok_or_else(|| DgmError::RuntimeError { msg: "'ths' used outside class".into() }),
            Expr::Ident(name) => env.borrow().get(name).ok_or_else(|| DgmError::RuntimeError { msg: format!("undefined variable '{}'", name) }),
            Expr::BinOp { op, left, right } => {
                // Short-circuit for and/or
                if op == "and" {
                    let l = self.eval_expr(left, Rc::clone(&env))?;
                    if !self.is_truthy(&l) { return Ok(DgmValue::Bool(false)); }
                    let r = self.eval_expr(right, env)?;
                    return Ok(DgmValue::Bool(self.is_truthy(&r)));
                }
                if op == "or" {
                    let l = self.eval_expr(left, Rc::clone(&env))?;
                    if self.is_truthy(&l) { return Ok(DgmValue::Bool(true)); }
                    let r = self.eval_expr(right, env)?;
                    return Ok(DgmValue::Bool(self.is_truthy(&r)));
                }
                let l = self.eval_expr(left, Rc::clone(&env))?;
                let r = self.eval_expr(right, Rc::clone(&env))?;
                self.apply_binop(op, l, r)
            }
            Expr::NullCoalesce { left, right } => {
                let value = self.eval_expr(left, Rc::clone(&env))?;
                if matches!(value, DgmValue::Null) {
                    self.eval_expr(right, env)
                } else {
                    Ok(value)
                }
            }
            Expr::UnaryOp { op, operand } => {
                let v = self.eval_expr(operand, env)?;
                match op.as_str() {
                    "not" => Ok(DgmValue::Bool(!self.is_truthy(&v))),
                    "-" => match v {
                        DgmValue::Int(n) => Ok(DgmValue::Int(-n)),
                        DgmValue::Float(f) => Ok(DgmValue::Float(-f)),
                        _ => Err(DgmError::RuntimeError { msg: "unary '-' on non-number".into() }),
                    },
                    "~" => match v {
                        DgmValue::Int(n) => Ok(DgmValue::Int(!n)),
                        _ => Err(DgmError::RuntimeError { msg: "bitwise '~' requires int".into() }),
                    },
                    _ => Err(DgmError::RuntimeError { msg: format!("unknown unary op '{}'", op) }),
                }
            }
            Expr::Await(inner) => self.eval_await(inner, env),
            Expr::Assign { target, op, value } => {
                let val = self.eval_expr(value, Rc::clone(&env))?;
                self.do_assign(target, op, val, env)
            }
            Expr::Call { callee, args } => self.eval_call_expr(callee, args, env, false),
            Expr::OptionalCall { callee, args } => self.eval_call_expr(callee, args, env, true),
            Expr::FieldAccess { object, field } => {
                let obj = self.eval_expr(object, env)?;
                field_access_value(&obj, field)
            }
            Expr::OptionalFieldAccess { object, field } => {
                let obj = self.eval_expr(object, env)?;
                if matches!(obj, DgmValue::Null) {
                    return Ok(DgmValue::Null);
                }
                field_access_value(&obj, field)
            }
            Expr::Index { object, index } => {
                let obj = self.eval_expr(object, Rc::clone(&env))?;
                let idx = self.eval_expr(index, env)?;
                index_access_value(&obj, &idx)
            }
            Expr::List(items) => {
                let vals: Vec<DgmValue> = items.iter().map(|e| self.eval_expr(e, Rc::clone(&env))).collect::<Result<_, _>>()?;
                reserve_list_growth(vals.len())?;
                Ok(DgmValue::List(Rc::new(RefCell::new(vals))))
            }
            Expr::Map(pairs) => {
                let mut map = HashMap::new();
                let mut key_bytes = 0;
                for (k, v) in pairs {
                    let key = match self.eval_expr(k, Rc::clone(&env))? { DgmValue::Str(s) => s, other => value_key(&other) };
                    key_bytes += key.len();
                    let val = self.eval_expr(v, Rc::clone(&env))?;
                    map.insert(key, val);
                }
                reserve_map_growth(map.len(), key_bytes)?;
                Ok(DgmValue::Map(Rc::new(RefCell::new(map))))
            }
            Expr::New { callee, args } => {
                let class_value = self.eval_expr(callee, Rc::clone(&env))?;
                let arg_vals = self.eval_call_args(args, Rc::clone(&env))?;
                self.instantiate_class_value(class_value, arg_vals)
            }
            Expr::Lambda { params, body } => Ok(build_function_value(None, params.clone(), body.clone(), Rc::clone(&env))),
            Expr::Ternary { condition, then_expr, else_expr } => {
                let cond = self.eval_expr(condition, Rc::clone(&env))?;
                if self.is_truthy(&cond) { self.eval_expr(then_expr, env) } else { self.eval_expr(else_expr, env) }
            }
            Expr::StringInterp(parts) => {
                let mut result = String::new();
                for part in parts { result.push_str(&format!("{}", self.eval_expr(part, Rc::clone(&env))?)); }
                reserve_string_bytes(result.len())?;
                Ok(DgmValue::Str(result))
            }
            Expr::Range { start, end } => {
                let s = match self.eval_expr(start, Rc::clone(&env))? { DgmValue::Int(n) => n, _ => return Err(DgmError::RuntimeError { msg: "range requires int".into() }) };
                let e = match self.eval_expr(end, env)? { DgmValue::Int(n) => n, _ => return Err(DgmError::RuntimeError { msg: "range requires int".into() }) };
                let list: Vec<DgmValue> = (s..e).map(DgmValue::Int).collect();
                reserve_list_growth(list.len())?;
                Ok(DgmValue::List(Rc::new(RefCell::new(list))))
            }
        }
    }

    fn eval_await(&mut self, expr: &Expr, env: Rc<RefCell<Environment>>) -> Result<DgmValue, DgmError> {
        if let Expr::Call { callee, args } | Expr::OptionalCall { callee, args } = expr {
            let callee_val = self.eval_expr(callee, Rc::clone(&env))?;
            if matches!(callee_val, DgmValue::Null) {
                return Ok(DgmValue::Null);
            }
            let arg_vals = self.eval_call_args(args, Rc::clone(&env))?;
            let positional = self.ensure_positional_args(&callable_profile_name(&callee_val), arg_vals.clone())?;
            if let Some(future) = crate::stdlib::http_mod::try_schedule_async_call(&callee_val, positional.clone())? {
                return self.await_value(future);
            }
            if let Some(future) = crate::stdlib::net_mod::try_schedule_async_call(&callee_val, positional.clone())? {
                return self.await_value(future);
            }
            let value = self.call_function(callee_val, arg_vals, env)?;
            return self.await_value(value);
        }
        let value = self.eval_expr(expr, env)?;
        self.await_value(value)
    }

    fn eval_call_expr(
        &mut self,
        callee: &Expr,
        args: &[Argument],
        env: Rc<RefCell<Environment>>,
        optional: bool,
    ) -> Result<DgmValue, DgmError> {
        if let Expr::FieldAccess { object, field } | Expr::OptionalFieldAccess { object, field } = callee {
            let obj = self.eval_expr(object, Rc::clone(&env))?;
            if optional && matches!(obj, DgmValue::Null) {
                return Ok(DgmValue::Null);
            }
            if let DgmValue::Instance { ref fields, .. } = obj {
                let method = fields.borrow().get(field).cloned()
                    .ok_or_else(|| DgmError::RuntimeError { msg: format!("no method '{}'", field) })?;
                let arg_vals = self.eval_call_args(args, Rc::clone(&env))?;
                if let DgmValue::Function { name, params, body, closure, compiled } = method {
                    return self.invoke_user_function(
                        name.as_deref(),
                        &params,
                        &body,
                        closure,
                        arg_vals,
                        Some(obj.clone()),
                        compiled,
                    );
                }
                return Err(DgmError::RuntimeError { msg: format!("'{}' is not callable", field) });
            }
            if matches!(obj, DgmValue::Map(_) | DgmValue::Request(_) | DgmValue::RequestShell(_)) {
                if let Ok(func) = field_access_value(&obj, field) {
                    let arg_vals = self.eval_call_args(args, Rc::clone(&env))?;
                    return self.call_function(func, arg_vals, Rc::clone(&env));
                }
            }
            return Err(DgmError::RuntimeError { msg: format!("cannot call '{}' on {:?}", field, obj) });
        }
        let callee_val = self.eval_expr(callee, Rc::clone(&env))?;
        if optional && matches!(callee_val, DgmValue::Null) {
            return Ok(DgmValue::Null);
        }
        let arg_vals = self.eval_call_args(args, Rc::clone(&env))?;
        self.call_function(callee_val, arg_vals, env)
    }

    fn eval_call_args(&mut self, args: &[Argument], env: Rc<RefCell<Environment>>) -> Result<Vec<EvaluatedArg>, DgmError> {
        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            values.push(EvaluatedArg {
                name: arg.name.clone(),
                value: self.eval_expr(&arg.value, Rc::clone(&env))?,
            });
        }
        Ok(values)
    }

    fn ensure_positional_args(&self, label: &str, args: Vec<EvaluatedArg>) -> Result<Vec<DgmValue>, DgmError> {
        let mut positional = Vec::with_capacity(args.len());
        for arg in args {
            if let Some(name) = arg.name {
                return Err(DgmError::RuntimeError {
                    msg: format!("named argument '{}' is not supported for {}", name, label),
                });
            }
            positional.push(arg.value);
        }
        Ok(positional)
    }

    fn bind_function_args(
        &mut self,
        call_name: &str,
        params: &[Param],
        args: Vec<EvaluatedArg>,
        call_env: Rc<RefCell<Environment>>,
    ) -> Result<(), DgmError> {
        let mut assigned = vec![None; params.len()];
        let mut next_positional = 0usize;
        let mut named_seen = false;

        for arg in args {
            if let Some(name) = arg.name {
                named_seen = true;
                let index = params.iter().position(|param| param.name == name).ok_or_else(|| {
                    DgmError::RuntimeError {
                        msg: format!("unknown argument '{}' for {}", name, call_name),
                    }
                })?;
                if assigned[index].is_some() {
                    return Err(DgmError::RuntimeError {
                        msg: format!("argument '{}' passed more than once to {}", name, call_name),
                    });
                }
                assigned[index] = Some(arg.value);
            } else {
                if named_seen {
                    return Err(DgmError::RuntimeError {
                        msg: format!("positional argument cannot follow named arguments in {}", call_name),
                    });
                }
                if next_positional >= params.len() {
                    return Err(DgmError::RuntimeError {
                        msg: format!("expected at most {} args, got more in {}", params.len(), call_name),
                    });
                }
                assigned[next_positional] = Some(arg.value);
                next_positional += 1;
            }
        }

        for (index, param) in params.iter().enumerate() {
            let value = if let Some(value) = assigned[index].take() {
                value
            } else if let Some(default) = &param.default {
                self.eval_expr(default, Rc::clone(&call_env))?
            } else {
                return Err(DgmError::RuntimeError {
                    msg: format!("missing required argument '{}' for {}", param.name, call_name),
                });
            };
            call_env.borrow_mut().set(&param.name, value);
        }

        Ok(())
    }

    fn bind_pattern(
        &mut self,
        pattern: &BindingPattern,
        value: Option<DgmValue>,
        env: Rc<RefCell<Environment>>,
    ) -> Result<(), DgmError> {
        match pattern {
            BindingPattern::Ignore => Ok(()),
            BindingPattern::Name { name, default } => {
                let value = if let Some(value) = value {
                    value
                } else if let Some(default) = default {
                    self.eval_expr(default, Rc::clone(&env))?
                } else {
                    return Err(DgmError::RuntimeError {
                        msg: format!("missing value for binding '{}'", name),
                    });
                };
                env.borrow_mut().set(name, value);
                Ok(())
            }
            BindingPattern::List(items) => {
                let value = value.ok_or_else(|| DgmError::RuntimeError {
                    msg: "cannot destructure list from missing value".into(),
                })?;
                let values = match value {
                    DgmValue::List(list) => list.borrow().clone(),
                    DgmValue::Json(state) => json_array_values(&state)?,
                    _ => {
                        return Err(DgmError::RuntimeError {
                            msg: "list destructuring requires list value".into(),
                        });
                    }
                };
                for (index, item) in items.iter().enumerate() {
                    self.bind_pattern(item, values.get(index).cloned(), Rc::clone(&env))?;
                }
                Ok(())
            }
            BindingPattern::Map(entries) => {
                let value = value.ok_or_else(|| DgmError::RuntimeError {
                    msg: "cannot destructure map from missing value".into(),
                })?;
                let Some(values) = map_like_entries(&value) else {
                    return Err(DgmError::RuntimeError {
                        msg: "map destructuring requires map value".into(),
                    });
                };
                let values = values.into_iter().collect::<HashMap<_, _>>();
                for entry in entries {
                    self.bind_pattern(&entry.pattern, values.get(&entry.key).cloned(), Rc::clone(&env))?;
                }
                Ok(())
            }
        }
    }

    fn match_pattern(
        &mut self,
        pattern: &MatchPattern,
        value: &DgmValue,
        env: Rc<RefCell<Environment>>,
    ) -> Result<bool, DgmError> {
        match pattern {
            MatchPattern::Wildcard => Ok(true),
            MatchPattern::Binding(name) => {
                env.borrow_mut().set(name, value.clone());
                Ok(true)
            }
            MatchPattern::Expr(expr) => {
                let pattern_value = self.eval_expr(expr, env)?;
                Ok(dgm_eq(value, &pattern_value))
            }
        }
    }

    fn await_value(&self, value: DgmValue) -> Result<DgmValue, DgmError> {
        match value {
            DgmValue::Future(state) => await_future(&state),
            DgmValue::TaskHandle(handle) => await_task_handle(&handle),
            DgmValue::Map(map) => {
                let awaitable = map.borrow().get("__await__").cloned();
                if let Some(awaitable) = awaitable {
                    self.await_value(awaitable)
                } else {
                    Ok(DgmValue::Map(map))
                }
            }
            DgmValue::Json(state) => Ok(DgmValue::Json(state)),
            other => Ok(other),
        }
    }

    fn instantiate_class_value(&mut self, class: DgmValue, args: Vec<EvaluatedArg>) -> Result<DgmValue, DgmError> {
        let (class_name, methods, parent, closure) = match class {
            DgmValue::Class { name, methods, parent, closure } => (name, methods, parent, closure),
            other => return Err(DgmError::RuntimeError { msg: format!("cannot instantiate {}", other) }),
        };
        reserve_map_growth(0, 0)?;
        let fields: Rc<RefCell<HashMap<String, DgmValue>>> = Rc::new(RefCell::new(HashMap::new()));
        let instance = DgmValue::Instance { class_name: class_name.clone(), fields: Rc::clone(&fields) };
        self.bind_class_methods(&DgmValue::Class {
            name: class_name.clone(),
            methods,
            parent,
            closure: Rc::clone(&closure),
        }, &fields)?;
        let init_fn = fields.borrow().get("init").cloned();
        if let Some(init_fn) = init_fn {
            let call_env = new_child_environment_ref(Rc::clone(&closure));
            call_env.borrow_mut().set("__self__", instance.clone());
            if let DgmValue::Function { name, params, body, compiled, .. } = init_fn {
                let profile_name = name.unwrap_or_else(|| format!("{}.init", class_name));
                let _profile_guard = runtime_enter_function_profile(&profile_name);
                let _call_guard = runtime_enter_call(Some(&profile_name))?;
                self.bind_function_args(&profile_name, &params, args, Rc::clone(&call_env))?;
                if let Some(compiled) = compiled {
                    match vm::execute_function(self, &compiled, Rc::clone(&call_env))? {
                        ControlFlow::Return(_) | ControlFlow::None => {}
                        ControlFlow::Break | ControlFlow::Continue => {}
                    }
                } else {
                    for stmt in &body { self.exec_stmt(stmt, Rc::clone(&call_env))?; }
                }
            }
        }
        Ok(instance)
    }

    fn bind_class_methods(&self, class: &DgmValue, fields: &Rc<RefCell<HashMap<String, DgmValue>>>) -> Result<(), DgmError> {
        let (class_name, methods, parent, closure) = match class {
            DgmValue::Class { name, methods, parent, closure } => (name, methods, parent, closure),
            _ => return Err(DgmError::RuntimeError { msg: "invalid class value".into() }),
        };
        if let Some(parent_name) = parent {
            let parent_class = closure.borrow().get(parent_name)
                .ok_or_else(|| DgmError::RuntimeError { msg: format!("undefined parent class '{}'", parent_name) })?;
            self.bind_class_methods(&parent_class, fields)?;
        }
        for method in methods {
            if let StmtKind::FuncDef { name, params, body } = &method.kind {
                fields.borrow_mut().insert(
                    name.clone(),
                    build_function_value(
                        Some(format!("{}.{}", class_name, name)),
                        params.clone(),
                        body.clone(),
                        Rc::clone(closure),
                    ),
                );
            }
        }
        Ok(())
    }

    fn do_assign(&mut self, target: &Expr, op: &str, val: DgmValue, env: Rc<RefCell<Environment>>) -> Result<DgmValue, DgmError> {
        match target {
            Expr::Ident(name) => {
                let final_val = if op == "=" { val } else {
                    let current = env.borrow().get(name).ok_or_else(|| DgmError::RuntimeError { msg: format!("undefined '{}'", name) })?;
                    self.apply_binop(&op[..op.len()-1], current, val)?
                };
                if env.borrow().get(name).is_some() { env.borrow_mut().assign(name, final_val.clone())?; }
                else { env.borrow_mut().set(name, final_val.clone()); }
                Ok(final_val)
            }
            Expr::FieldAccess { object, field } => {
                let obj = self.eval_expr(object, Rc::clone(&env))?;
                match obj {
                    DgmValue::Instance { fields, .. } => {
                        let final_val = if op == "=" { val } else {
                            let current = fields.borrow().get(field).cloned().unwrap_or(DgmValue::Null);
                            self.apply_binop(&op[..op.len()-1], current, val)?
                        };
                        let is_new = !fields.borrow().contains_key(field);
                        if is_new {
                            reserve_map_growth(1, field.len())?;
                        }
                        fields.borrow_mut().insert(field.clone(), final_val.clone());
                        Ok(final_val)
                    }
                    DgmValue::Map(m) => {
                        let final_val = if op == "=" { val } else {
                            let current = m.borrow().get(field).cloned().unwrap_or(DgmValue::Null);
                            self.apply_binop(&op[..op.len()-1], current, val)?
                        };
                        let is_new = !m.borrow().contains_key(field);
                        if is_new {
                            reserve_map_growth(1, field.len())?;
                        }
                        m.borrow_mut().insert(field.clone(), final_val.clone());
                        Ok(final_val)
                    }
                    DgmValue::Json(_) => Err(DgmError::RuntimeError { msg: "cannot assign field on json value".into() }),
                    _ => Err(DgmError::RuntimeError { msg: "field assign on non-instance".into() }),
                }
            }
            Expr::Index { object, index } => {
                let obj = self.eval_expr(object, Rc::clone(&env))?;
                let idx = self.eval_expr(index, Rc::clone(&env))?;
                match (&obj, &idx) {
                    (DgmValue::List(l), DgmValue::Int(i)) => {
                        let len = l.borrow().len();
                        let idx = if *i < 0 { len as i64 + i } else { *i };
                        if idx < 0 {
                            return Err(DgmError::RuntimeError { msg: "index out of range".into() });
                        }
                        let i = idx as usize;
                        if i >= len { return Err(DgmError::RuntimeError { msg: "index out of range".into() }); }
                        let final_val = if op == "=" { val } else { self.apply_binop(&op[..op.len()-1], l.borrow()[i].clone(), val)? };
                        l.borrow_mut()[i] = final_val.clone();
                        Ok(final_val)
                    }
                    (DgmValue::Map(m), DgmValue::Str(k)) => {
                        let is_new = !m.borrow().contains_key(k);
                        if is_new {
                            reserve_map_growth(1, k.len())?;
                        }
                        m.borrow_mut().insert(k.clone(), val.clone());
                        Ok(val)
                    }
                    (DgmValue::Json(_), _) => Err(DgmError::RuntimeError { msg: "cannot assign into json value".into() }),
                    _ => Err(DgmError::RuntimeError { msg: "invalid index assign".into() }),
                }
            }
            _ => Err(DgmError::RuntimeError { msg: "invalid assignment target".into() }),
        }
    }

    fn call_function(&mut self, callee: DgmValue, args: Vec<EvaluatedArg>, _env: Rc<RefCell<Environment>>) -> Result<DgmValue, DgmError> {
        match callee {
            DgmValue::NativeFunction { name, func } => {
                let _profile_guard = runtime_enter_function_profile(&name);
                let positional = self.ensure_positional_args(&name, args)?;
                trap_dgm(&format!("native function '{}'", name), || func(positional))
            }
            DgmValue::NativeMethod { name, func, state } => {
                let _profile_guard = runtime_enter_function_profile(&name);
                let positional = self.ensure_positional_args(&name, args)?;
                trap_dgm(&format!("native method '{}'", name), || func(*state, positional))
            }
            DgmValue::Function { name, params, body, closure, compiled } => {
                self.invoke_user_function(name.as_deref(), &params, &body, closure, args, None, compiled)
            }
            class @ DgmValue::Class { .. } => {
                let profile_name = callable_profile_name(&class);
                let _profile_guard = runtime_enter_function_profile(&profile_name);
                self.instantiate_class_value(class, args)
            }
            _ => Err(DgmError::RuntimeError { msg: "value is not callable".into() }),
        }
    }

    fn invoke_user_function(
        &mut self,
        name: Option<&str>,
        params: &[Param],
        body: &[Stmt],
        closure: Rc<RefCell<Environment>>,
        args: Vec<EvaluatedArg>,
        self_value: Option<DgmValue>,
        compiled: Option<Arc<CompiledFunctionBlob>>,
    ) -> Result<DgmValue, DgmError> {
        let profile_name = name.unwrap_or("<lambda>");
        let _profile_guard = runtime_enter_function_profile(profile_name);
        let _call_guard = runtime_enter_call(Some(profile_name))?;
        let call_env = new_child_environment_ref(closure);
        if let Some(self_value) = self_value {
            call_env.borrow_mut().set("__self__", self_value);
        }
        self.bind_function_args(profile_name, params, args, Rc::clone(&call_env))?;
        if let Some(compiled) = compiled {
            match vm::execute_function(self, &compiled, call_env)? {
                ControlFlow::Return(v) => Ok(v),
                _ => Ok(DgmValue::Null),
            }
        } else {
            match self.exec_block(body, call_env)? {
                ControlFlow::Return(v) => Ok(v),
                _ => Ok(DgmValue::Null),
            }
        }
    }

    pub(crate) fn is_truthy(&self, value: &DgmValue) -> bool {
        match value {
            DgmValue::Bool(b) => *b,
            DgmValue::Null => false,
            DgmValue::Int(n) => *n != 0,
            DgmValue::Float(f) => *f != 0.0,
            DgmValue::Str(s) => !s.is_empty(),
            DgmValue::RawJson(text) => text.len() > 0,
            _ => true,
        }
    }

    pub(crate) fn apply_binop(&self, op: &str, left: DgmValue, right: DgmValue) -> Result<DgmValue, DgmError> {
        match op {
            "+" => match (left, right) {
                (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Int(a.wrapping_add(b))),
                (DgmValue::Float(a), DgmValue::Float(b)) => Ok(DgmValue::Float(a + b)),
                (DgmValue::Int(a), DgmValue::Float(b)) => Ok(DgmValue::Float(a as f64 + b)),
                (DgmValue::Float(a), DgmValue::Int(b)) => Ok(DgmValue::Float(a + b as f64)),
                (DgmValue::Str(a), DgmValue::Str(b)) => {
                    let result = a + &b;
                    reserve_string_bytes(result.len())?;
                    Ok(DgmValue::Str(result))
                }
                (DgmValue::Str(a), b) => {
                    let result = a + &format!("{}", b);
                    reserve_string_bytes(result.len())?;
                    Ok(DgmValue::Str(result))
                }
                (a, DgmValue::Str(b)) => {
                    let result = format!("{}", a) + &b;
                    reserve_string_bytes(result.len())?;
                    Ok(DgmValue::Str(result))
                }
                (DgmValue::List(a), DgmValue::List(b)) => {
                    let mut v = a.borrow().clone();
                    v.extend(b.borrow().clone());
                    reserve_list_growth(v.len())?;
                    Ok(DgmValue::List(Rc::new(RefCell::new(v))))
                }
                _ => Err(DgmError::RuntimeError { msg: "'+' type mismatch".into() }),
            },
            "-" => numeric_op!(left, right, wrapping_sub, -,  "-"),
            "*" => numeric_op!(left, right, wrapping_mul, *, "*"),
            "/" => {
                match (&left, &right) {
                    (_, DgmValue::Int(0)) => Err(DgmError::RuntimeError { msg: "division by zero".into() }),
                    (_, DgmValue::Float(f)) if *f == 0.0 => Err(DgmError::RuntimeError { msg: "division by zero".into() }),
                    (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Int(a / b)),
                    (DgmValue::Float(a), DgmValue::Float(b)) => Ok(DgmValue::Float(a / b)),
                    (DgmValue::Int(a), DgmValue::Float(b)) => Ok(DgmValue::Float(*a as f64 / b)),
                    (DgmValue::Float(a), DgmValue::Int(b)) => Ok(DgmValue::Float(a / *b as f64)),
                    _ => Err(DgmError::RuntimeError { msg: "'/' type mismatch".into() }),
                }
            }
            "%" => match (left, right) {
                (DgmValue::Int(a), DgmValue::Int(b)) => { if b == 0 { Err(DgmError::RuntimeError { msg: "modulo by zero".into() }) } else { Ok(DgmValue::Int(a % b)) } }
                (DgmValue::Float(a), DgmValue::Float(b)) => Ok(DgmValue::Float(a % b)),
                (DgmValue::Int(a), DgmValue::Float(b)) => Ok(DgmValue::Float(a as f64 % b)),
                (DgmValue::Float(a), DgmValue::Int(b)) => Ok(DgmValue::Float(a % b as f64)),
                _ => Err(DgmError::RuntimeError { msg: "'%' type mismatch".into() }),
            },
            "**" => match (left, right) {
                (DgmValue::Int(a), DgmValue::Int(b)) => { if b >= 0 { Ok(DgmValue::Int(a.wrapping_pow(b as u32))) } else { Ok(DgmValue::Float((a as f64).powi(b as i32))) } }
                (DgmValue::Float(a), DgmValue::Float(b)) => Ok(DgmValue::Float(a.powf(b))),
                (DgmValue::Int(a), DgmValue::Float(b)) => Ok(DgmValue::Float((a as f64).powf(b))),
                (DgmValue::Float(a), DgmValue::Int(b)) => Ok(DgmValue::Float(a.powi(b as i32))),
                _ => Err(DgmError::RuntimeError { msg: "'**' type mismatch".into() }),
            },
            "&" => match (left, right) { (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Int(a & b)), _ => Err(DgmError::RuntimeError { msg: "'&' requires ints".into() }) },
            "|" => match (left, right) { (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Int(a | b)), _ => Err(DgmError::RuntimeError { msg: "'|' requires ints".into() }) },
            "^" => match (left, right) { (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Int(a ^ b)), _ => Err(DgmError::RuntimeError { msg: "'^' requires ints".into() }) },
            "<<" => match (left, right) { (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Int(a << b)), _ => Err(DgmError::RuntimeError { msg: "'<<' requires ints".into() }) },
            ">>" => match (left, right) { (DgmValue::Int(a), DgmValue::Int(b)) => Ok(DgmValue::Int(a >> b)), _ => Err(DgmError::RuntimeError { msg: "'>>' requires ints".into() }) },
            "==" => Ok(DgmValue::Bool(dgm_eq(&left, &right))),
            "!=" => Ok(DgmValue::Bool(!dgm_eq(&left, &right))),
            "<" => cmp_op!(left, right, <, "<"),
            ">" => cmp_op!(left, right, >, ">"),
            "<=" => cmp_op!(left, right, <=, "<="),
            ">=" => cmp_op!(left, right, >=, ">="),
            "and" => Ok(DgmValue::Bool(self.is_truthy(&left) && self.is_truthy(&right))),
            "or" => Ok(DgmValue::Bool(self.is_truthy(&left) || self.is_truthy(&right))),
            "in" => {
                match right {
                    DgmValue::List(l) => Ok(DgmValue::Bool(l.borrow().iter().any(|v| dgm_eq(&left, v)))),
                    DgmValue::Map(m) => { if let DgmValue::Str(k) = &left { Ok(DgmValue::Bool(m.borrow().contains_key(k))) } else { Ok(DgmValue::Bool(false)) } }
                    DgmValue::Request(_) | DgmValue::RequestShell(_) | DgmValue::RequestMap(_) | DgmValue::RequestMapView(_) => {
                        if let DgmValue::Str(k) = &left {
                            Ok(DgmValue::Bool(map_like_contains(&right, k)))
                        } else {
                            Ok(DgmValue::Bool(false))
                        }
                    }
                    DgmValue::Json(state) => match json_resolved_value(&state) {
                        serde_json::Value::Array(_) => Ok(DgmValue::Bool(
                            json_array_values(&state)?
                                .into_iter()
                                .any(|value| dgm_eq(&left, &value)),
                        )),
                        serde_json::Value::Object(_) => {
                            if let DgmValue::Str(k) = &left {
                                Ok(DgmValue::Bool(matches!(
                                    json_resolved_value(&state),
                                    serde_json::Value::Object(map) if map.contains_key(k)
                                )))
                            } else {
                                Ok(DgmValue::Bool(false))
                            }
                        }
                        serde_json::Value::String(text) => {
                            if let DgmValue::Str(sub) = &left {
                                Ok(DgmValue::Bool(text.contains(sub.as_str())))
                            } else {
                                Ok(DgmValue::Bool(false))
                            }
                        }
                        _ => Err(DgmError::RuntimeError { msg: "'in' requires list/map/string".into() }),
                    },
                    DgmValue::Str(s) => { if let DgmValue::Str(sub) = &left { Ok(DgmValue::Bool(s.contains(sub.as_str()))) } else { Ok(DgmValue::Bool(false)) } }
                    _ => Err(DgmError::RuntimeError { msg: "'in' requires list/map/string".into() }),
                }
            }
            _ => Err(DgmError::RuntimeError { msg: format!("unknown op '{}'", op) }),
        }
    }

    pub(crate) fn apply_unary_op(&self, op: &str, value: DgmValue) -> Result<DgmValue, DgmError> {
        match op {
            "not" => Ok(DgmValue::Bool(!self.is_truthy(&value))),
            "-" => match value {
                DgmValue::Int(n) => Ok(DgmValue::Int(-n)),
                DgmValue::Float(f) => Ok(DgmValue::Float(-f)),
                _ => Err(DgmError::RuntimeError { msg: "unary '-' on non-number".into() }),
            },
            "~" => match value {
                DgmValue::Int(n) => Ok(DgmValue::Int(!n)),
                _ => Err(DgmError::RuntimeError { msg: "bitwise '~' requires int".into() }),
            },
            _ => Err(DgmError::RuntimeError { msg: format!("unknown unary op '{}'", op) }),
        }
    }

    fn do_import(&mut self, name: &str, env: Rc<RefCell<Environment>>) -> Result<(), DgmError> {
        if let Some(module) = self.load_cached_stdlib(name)? {
            env.borrow_mut().set(&module_binding_name(name, None), module);
            return Ok(());
        }
        if let Some((binding_name, module)) = self.load_bundled_module(name)? {
            env.borrow_mut().set(&binding_name, module);
            return Ok(());
        }
        let path = self.resolve_import_path(name)?;
        let binding_name = module_binding_name(name, Some(&path));
        let cache_key = format!("@file:{}", path.display());
        if let Some(module) = self.module_cache.get(&cache_key).cloned() {
            env.borrow_mut().set(&binding_name, module);
            return Ok(());
        }
        let source = std::fs::read_to_string(&path).map_err(|e| DgmError::ImportError { msg: format!("cannot import '{}': {}", name, e) })?;
        register_source(path.to_string_lossy().to_string(), source.clone());
        let mut lexer = crate::lexer::Lexer::new(&source);
        let tokens = match lexer.tokenize() {
            Ok(tokens) => tokens,
            Err(err) => {
                return Err(err.with_path(path.to_string_lossy().to_string()));
            }
        };
        let mut parser = crate::parser::Parser::new(tokens);
        let stmts = match parser.parse() {
            Ok(stmts) => stmts,
            Err(err) => {
                return Err(err.with_path(path.to_string_lossy().to_string()));
            }
        };
        let module = self.execute_module_as_map(&cache_key, path.clone(), None, &stmts, None)?;
        env.borrow_mut().set(&binding_name, module);
        Ok(())
    }

    fn load_cached_stdlib(&mut self, name: &str) -> Result<Option<DgmValue>, DgmError> {
        let binding_name = module_binding_name(name, None);
        let cache_key = format!("@stdlib:{}", binding_name);
        if let Some(module) = self.module_cache.get(&cache_key).cloned() {
            return Ok(Some(module));
        }
        if let Some(module) = crate::stdlib::load_module(name) {
            self.module_cache.insert(cache_key, module.clone());
            return Ok(Some(module));
        }
        Ok(None)
    }

    fn resolve_import_path(&self, name: &str) -> Result<PathBuf, DgmError> {
        let trimmed = name.trim_matches('"');
        let raw = PathBuf::from(trimmed);
        let with_ext = if raw.extension().is_some() { raw } else { raw.with_extension("dgm") };
        let candidate = if with_ext.is_absolute() {
            with_ext
        } else {
            self.current_base_dir().join(with_ext)
        };
        let candidate = if candidate.exists() {
            std::fs::canonicalize(&candidate).unwrap_or(candidate)
        } else {
            candidate
        };
        if candidate.exists() {
            return Ok(candidate);
        }
        if !trimmed.contains('/') && !trimmed.contains('\\') {
            if let Some(package_entry) = crate::package_manager::resolve_installed_package(&self.current_base_dir(), trimmed)? {
                return Ok(package_entry);
            }
        }
        Ok(candidate)
    }

    fn current_base_dir(&self) -> PathBuf {
        if let (Some(bundle), Some(module_key)) = (&self.bundle, self.current_module_keys.last()) {
            if bundle.manifest().build_mode == "debug" {
                if let Ok(cwd) = std::env::current_dir() {
                    if let Some(base) = bundle::debug_base_dir_for_module_key(module_key, &cwd) {
                        return base;
                    }
                }
            }
        }
        self.current_paths
            .last()
            .and_then(|path| path.parent().map(Path::to_path_buf))
            .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }

    fn attach_stmt_frame(&self, err: DgmError, span: Span) -> DgmError {
        err.with_frame(self.stack_frame(span))
    }

    pub(crate) fn attach_span_frame(&self, err: DgmError, span: Span) -> DgmError {
        self.attach_stmt_frame(err, span)
    }

    pub(crate) fn note_vm_span(&self, _span: Span) {}

    fn stack_frame(&self, span: Span) -> StackFrame {
        StackFrame {
            function: runtime_current_function(),
            path: self.current_paths
                .last()
                .map(|path| path.to_string_lossy().to_string())
                .unwrap_or_else(|| "<repl>".into()),
            span,
        }
    }
}

impl Drop for Interpreter {
    fn drop(&mut self) {
        let mut seen_envs = HashSet::new();
        let mut seen_maps = HashSet::new();
        let mut seen_lists = HashSet::new();
        let cached_modules = self.module_cache.values().cloned().collect::<Vec<_>>();
        for module in &cached_modules {
            teardown_value_graph(module, &mut seen_envs, &mut seen_maps, &mut seen_lists);
        }
        teardown_environment_graph(Rc::clone(&self.globals), &mut seen_envs, &mut seen_maps, &mut seen_lists);
        self.module_cache.clear();
        self.loading_modules.clear();
        self.current_paths.clear();
        self.current_module_keys.clear();
        self.bundle = None;
        self.globals.borrow_mut().clear_values();
    }
}

fn module_binding_name(name: &str, path: Option<&Path>) -> String {
    let trimmed = name.trim_matches('"');
    if trimmed.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.') && !trimmed.contains('/') {
        return trimmed.trim_end_matches(".dgm").to_string();
    }
    path.and_then(|p| p.file_stem())
        .and_then(|s| s.to_str())
        .unwrap_or("module")
        .to_string()
}

fn thrown_value_from_dgm(value: &DgmValue) -> ThrownValue {
    match value {
        DgmValue::Int(n) => ThrownValue::Int(*n),
        DgmValue::Float(n) => ThrownValue::Float(*n),
        DgmValue::Str(s) => ThrownValue::Str(s.clone()),
        DgmValue::Bool(value) => ThrownValue::Bool(*value),
        DgmValue::Null => ThrownValue::Null,
        DgmValue::List(items) => ThrownValue::List(
            items
                .borrow()
                .iter()
                .map(thrown_value_from_dgm)
                .collect(),
        ),
        DgmValue::Map(map) => ThrownValue::Map(
            sorted_map_entries(&map.borrow())
                .into_iter()
                .map(|(key, value)| (key, thrown_value_from_dgm(&value)))
                .collect(),
        ),
        DgmValue::Request(request) => ThrownValue::Map(
            request_entries(request.as_ref())
                .into_iter()
                .map(|(key, value)| (key, thrown_value_from_dgm(&value)))
                .collect(),
        ),
        DgmValue::RequestShell(request) => ThrownValue::Map(
            request_entries(request.as_ref())
                .into_iter()
                .map(|(key, value)| (key, thrown_value_from_dgm(&value)))
                .collect(),
        ),
        DgmValue::RequestMap(map) => ThrownValue::Map(
            request_map_entries(map)
                .into_iter()
                .map(|(key, value)| (key, thrown_value_from_dgm(&value)))
                .collect(),
        ),
        DgmValue::RequestMapView(values) => ThrownValue::Map(
            request_map_view_entries(values)
                .into_iter()
                .map(|(key, value)| (key, thrown_value_from_dgm(&value)))
                .collect(),
        ),
        DgmValue::RawJson(text) => ThrownValue::Str(text.to_string_lossy()),
        DgmValue::Json(state) => match json_resolved_value(state) {
            serde_json::Value::Array(_) => ThrownValue::List(
                json_array_values(state)
                    .unwrap_or_default()
                    .iter()
                    .map(thrown_value_from_dgm)
                    .collect(),
            ),
            serde_json::Value::Object(_) => ThrownValue::Map(
                json_entries(state)
                    .unwrap_or_default()
                    .iter()
                    .map(|(key, value)| (key.clone(), thrown_value_from_dgm(value)))
                    .collect(),
            ),
            serde_json::Value::Null => ThrownValue::Null,
            serde_json::Value::Bool(value) => ThrownValue::Bool(*value),
            serde_json::Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    ThrownValue::Int(value)
                } else {
                    ThrownValue::Float(number.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(text) => ThrownValue::Str(text.clone()),
        },
        other => ThrownValue::Opaque(format!("{}", other)),
    }
}

fn dgm_from_thrown_value(value: &ThrownValue) -> DgmValue {
    match value {
        ThrownValue::Int(value) => DgmValue::Int(*value),
        ThrownValue::Float(value) => DgmValue::Float(*value),
        ThrownValue::Str(value) => DgmValue::Str(value.clone()),
        ThrownValue::Bool(value) => DgmValue::Bool(*value),
        ThrownValue::Null => DgmValue::Null,
        ThrownValue::List(items) => DgmValue::List(Rc::new(RefCell::new(
            items.iter().map(dgm_from_thrown_value).collect(),
        ))),
        ThrownValue::Map(entries) => DgmValue::Map(Rc::new(RefCell::new(
            entries
                .iter()
                .map(|(key, value)| (key.clone(), dgm_from_thrown_value(value)))
                .collect(),
        ))),
        ThrownValue::Opaque(value) => DgmValue::Str(value.clone()),
    }
}

pub(crate) fn value_repr(value: &DgmValue) -> String {
    match value {
        DgmValue::Str(s) => format!("{:?}", s),
        DgmValue::List(items) => {
            let parts: Vec<String> = items.borrow().iter().map(value_repr).collect();
            format!("[{}]", parts.join(", "))
        }
        DgmValue::Map(map) => {
            let parts: Vec<String> = sorted_map_entries(&map.borrow()).into_iter().map(|(k, v)| {
                if is_sensitive_key(&k) {
                    format!("{:?}: {:?}", k, masked_secret_value(&v))
                } else {
                    format!("{:?}: {}", k, value_repr(&v))
                }
            }).collect();
            format!("{{{}}}", parts.join(", "))
        }
        DgmValue::Request(request) => {
            let parts: Vec<String> = request_entries(request.as_ref()).into_iter().map(|(k, v)| {
                if is_sensitive_key(&k) {
                    format!("{:?}: {:?}", k, masked_secret_value(&v))
                } else {
                    format!("{:?}: {}", k, value_repr(&v))
                }
            }).collect();
            format!("{{{}}}", parts.join(", "))
        }
        DgmValue::RequestShell(request) => {
            let parts: Vec<String> = request_entries(request.as_ref()).into_iter().map(|(k, v)| {
                if is_sensitive_key(&k) {
                    format!("{:?}: {:?}", k, masked_secret_value(&v))
                } else {
                    format!("{:?}: {}", k, value_repr(&v))
                }
            }).collect();
            format!("{{{}}}", parts.join(", "))
        }
        DgmValue::RequestMap(map) => {
            let parts: Vec<String> = request_map_entries(map).into_iter().map(|(k, v)| {
                if is_sensitive_key(&k) {
                    format!("{:?}: {:?}", k, masked_secret_value(&v))
                } else {
                    format!("{:?}: {}", k, value_repr(&v))
                }
            }).collect();
            format!("{{{}}}", parts.join(", "))
        }
        DgmValue::RequestMapView(values) => {
            let parts: Vec<String> = request_map_view_entries(values).into_iter().map(|(k, v)| {
                if is_sensitive_key(&k) {
                    format!("{:?}: {:?}", k, masked_secret_value(&v))
                } else {
                    format!("{:?}: {}", k, value_repr(&v))
                }
            }).collect();
            format!("{{{}}}", parts.join(", "))
        }
        DgmValue::Json(state) => serde_json::to_string(json_resolved_value(state)).unwrap_or_else(|_| "<json>".into()),
        DgmValue::RawJson(text) => format!("{:?}", text.to_string_lossy()),
        DgmValue::Class { name, .. } => format!("<class {}>", name),
        DgmValue::Instance { class_name, .. } => format!("<{} instance>", class_name),
        other => format!("{}", other),
    }
}

pub(crate) fn value_key(value: &DgmValue) -> String {
    match value {
        DgmValue::Null => "nul".into(),
        DgmValue::Bool(b) => format!("bool:{b}"),
        DgmValue::Int(n) => format!("int:{n}"),
        DgmValue::Float(f) => format!("float:{f}"),
        DgmValue::Str(s) => format!("str:{s}"),
        DgmValue::Channel(channel) => format!("channel:{:p}", std::sync::Arc::as_ptr(channel)),
        DgmValue::TaskHandle(handle) => format!("task:{:p}", std::sync::Arc::as_ptr(handle)),
        DgmValue::Future(future) => format!("future:{:p}", std::sync::Arc::as_ptr(future)),
        DgmValue::Class { name, .. } => format!("class:{name}"),
        DgmValue::Instance { class_name, .. } => format!("instance:{class_name}:{}", value_repr(value)),
        DgmValue::Request(request) => format!("request:{:p}", Rc::as_ptr(request)),
        DgmValue::RequestShell(request) => format!("request_shell:{:p}", Rc::as_ptr(request)),
        DgmValue::RequestMap(map) => format!("request_map:{:p}", Rc::as_ptr(map)),
        DgmValue::RequestMapView(values) => format!("request_map_view:{:p}", Arc::as_ptr(values)),
        DgmValue::RequestJson(state) => format!("request_json:{:p}", Rc::as_ptr(state)),
        DgmValue::RawJson(text) => format!("raw_json:{:p}:{}", Arc::as_ptr(text), text.to_string_lossy()),
        DgmValue::Json(state) => format!(
            "json:{:p}:{}",
            Arc::as_ptr(&state.root),
            value_repr(value)
        ),
        DgmValue::List(_) | DgmValue::Map(_) | DgmValue::Function { .. } | DgmValue::NativeFunction { .. } | DgmValue::NativeMethod { .. } => value_repr(value),
    }
}

pub(crate) fn dgm_eq(a: &DgmValue, b: &DgmValue) -> bool {
    match (a, b) {
        (DgmValue::Int(x), DgmValue::Int(y)) => x == y,
        (DgmValue::Float(x), DgmValue::Float(y)) => x == y,
        (DgmValue::Int(x), DgmValue::Float(y)) => (*x as f64) == *y,
        (DgmValue::Float(x), DgmValue::Int(y)) => *x == (*y as f64),
        (DgmValue::Str(x), DgmValue::Str(y)) => x == y,
        (DgmValue::Bool(x), DgmValue::Bool(y)) => x == y,
        (DgmValue::Null, DgmValue::Null) => true,
        (DgmValue::Channel(a), DgmValue::Channel(b)) => std::sync::Arc::ptr_eq(a, b),
        (DgmValue::TaskHandle(a), DgmValue::TaskHandle(b)) => std::sync::Arc::ptr_eq(a, b),
        (DgmValue::Future(a), DgmValue::Future(b)) => std::sync::Arc::ptr_eq(a, b),
        (DgmValue::Request(a), DgmValue::Request(b)) => Rc::ptr_eq(a, b),
        (DgmValue::RequestShell(a), DgmValue::RequestShell(b)) => Rc::ptr_eq(a, b),
        (DgmValue::RequestMap(a), DgmValue::RequestMap(b)) => Rc::ptr_eq(a, b),
        (DgmValue::RequestMapView(a), DgmValue::RequestMapView(b)) => Arc::ptr_eq(a, b),
        (DgmValue::RequestJson(a), DgmValue::RequestJson(b)) => Rc::ptr_eq(a, b),
        (DgmValue::RawJson(a), DgmValue::RawJson(b)) => a.to_string_lossy() == b.to_string_lossy(),
        (DgmValue::Json(a), DgmValue::Json(b)) => json_resolved_value(a) == json_resolved_value(b),
        (DgmValue::Class { name: a, .. }, DgmValue::Class { name: b, .. }) => a == b,
        _ => false,
    }
}

// ─── Native Functions ────────────────────────────────────────
fn native_len(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(l)) => Ok(DgmValue::Int(l.borrow().len() as i64)),
        Some(DgmValue::Str(s)) => Ok(DgmValue::Int(s.chars().count() as i64)),
        Some(DgmValue::RawJson(text)) => Ok(DgmValue::Int(text.char_len() as i64)),
        Some(DgmValue::Map(m)) => Ok(DgmValue::Int(m.borrow().len() as i64)),
        Some(DgmValue::Request(_)) => Ok(DgmValue::Int(request_field_names().len() as i64)),
        Some(DgmValue::RequestShell(_)) => Ok(DgmValue::Int(request_field_names().len() as i64)),
        Some(DgmValue::RequestMap(map)) => Ok(DgmValue::Int(map.values.len() as i64)),
        Some(DgmValue::RequestMapView(values)) => Ok(DgmValue::Int(values.len() as i64)),
        Some(DgmValue::Json(state)) => match json_resolved_value(state) {
            serde_json::Value::Array(items) => Ok(DgmValue::Int(items.len() as i64)),
            serde_json::Value::Object(map) => Ok(DgmValue::Int(map.len() as i64)),
            _ => Err(DgmError::RuntimeError { msg: "len() requires list/string/map".into() }),
        },
        _ => Err(DgmError::RuntimeError { msg: "len() requires list/string/map".into() }),
    }
}
fn native_type(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let t = match args.first() {
        Some(DgmValue::Int(_)) => "int", Some(DgmValue::Float(_)) => "float",
        Some(DgmValue::Str(_)) => "str", Some(DgmValue::Bool(_)) => "bool",
        Some(DgmValue::RawJson(_)) => "raw_json",
        Some(DgmValue::Null) => "nul", Some(DgmValue::List(_)) => "list",
        Some(DgmValue::Map(_)) | Some(DgmValue::Request(_)) | Some(DgmValue::RequestShell(_)) | Some(DgmValue::RequestMap(_)) | Some(DgmValue::RequestMapView(_)) => "map",
        Some(DgmValue::Json(state)) => match json_resolved_value(state) {
            serde_json::Value::Array(_) => "list",
            serde_json::Value::Object(_) => "map",
            serde_json::Value::String(_) => "str",
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Null => "nul",
            serde_json::Value::Number(number) => if number.as_i64().is_some() { "int" } else { "float" },
        },
        Some(DgmValue::RequestJson(_)) => "request_json",
        Some(DgmValue::Function { .. }) | Some(DgmValue::NativeFunction { .. }) | Some(DgmValue::NativeMethod { .. }) => "function",
        Some(DgmValue::Channel(_)) => "channel",
        Some(DgmValue::TaskHandle(_)) => "task",
        Some(DgmValue::Future(_)) => "future",
        Some(DgmValue::Class { .. }) => "class",
        Some(DgmValue::Instance { class_name, .. }) => return Ok(DgmValue::Str(class_name.clone())),
        None => return Err(DgmError::RuntimeError { msg: "type() requires 1 arg".into() }),
    };
    Ok(DgmValue::Str(t.into()))
}
fn native_str(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let out = format!("{}", args.first().unwrap_or(&DgmValue::Null));
    reserve_string_bytes(out.len())?;
    Ok(DgmValue::Str(out))
}
fn native_int(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Int(n)) => Ok(DgmValue::Int(*n)),
        Some(DgmValue::Float(f)) => Ok(DgmValue::Int(*f as i64)),
        Some(DgmValue::Bool(b)) => Ok(DgmValue::Int(if *b { 1 } else { 0 })),
        Some(DgmValue::Str(s)) => s.trim().parse::<i64>().map(DgmValue::Int).map_err(|_| DgmError::RuntimeError { msg: format!("cannot convert '{}' to int", s) }),
        Some(DgmValue::RawJson(text)) => {
            let rendered = text.to_string_lossy();
            rendered.trim().parse::<i64>().map(DgmValue::Int).map_err(|_| DgmError::RuntimeError { msg: format!("cannot convert '{}' to int", rendered) })
        }
        _ => Err(DgmError::RuntimeError { msg: "int() invalid arg".into() }),
    }
}
fn native_float(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Int(n)) => Ok(DgmValue::Float(*n as f64)),
        Some(DgmValue::Float(f)) => Ok(DgmValue::Float(*f)),
        Some(DgmValue::Str(s)) => s.trim().parse::<f64>().map(DgmValue::Float).map_err(|_| DgmError::RuntimeError { msg: format!("cannot convert '{}' to float", s) }),
        Some(DgmValue::RawJson(text)) => {
            let rendered = text.to_string_lossy();
            rendered.trim().parse::<f64>().map(DgmValue::Float).map_err(|_| DgmError::RuntimeError { msg: format!("cannot convert '{}' to float", rendered) })
        }
        _ => Err(DgmError::RuntimeError { msg: "float() invalid arg".into() }),
    }
}
fn native_push(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(v)) => {
            reserve_list_growth(1)?;
            l.borrow_mut().push(v.clone());
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "push(list, value) required".into() }),
    }
}
fn native_pop(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(l)) => l.borrow_mut().pop().ok_or_else(|| DgmError::RuntimeError { msg: "pop() on empty list".into() }),
        _ => Err(DgmError::RuntimeError { msg: "pop() requires a list".into() }),
    }
}
fn native_range(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let (start, end, step) = match args.len() {
        1 => match &args[0] { DgmValue::Int(n) => (0, *n, 1), _ => return Err(DgmError::RuntimeError { msg: "range() requires int".into() }) },
        2 => match (&args[0], &args[1]) { (DgmValue::Int(a), DgmValue::Int(b)) => (*a, *b, 1), _ => return Err(DgmError::RuntimeError { msg: "range() requires ints".into() }) },
        3 => match (&args[0], &args[1], &args[2]) { (DgmValue::Int(a), DgmValue::Int(b), DgmValue::Int(c)) => (*a, *b, *c), _ => return Err(DgmError::RuntimeError { msg: "range() requires ints".into() }) },
        _ => return Err(DgmError::RuntimeError { msg: "range(end) or range(start, end) or range(start, end, step)".into() }),
    };
    if step == 0 { return Err(DgmError::RuntimeError { msg: "range() step cannot be 0".into() }); }
    let mut list = vec![];
    let mut i = start;
    if step > 0 { while i < end { list.push(DgmValue::Int(i)); i += step; } }
    else { while i > end { list.push(DgmValue::Int(i)); i += step; } }
    reserve_list_growth(list.len())?;
    Ok(DgmValue::List(Rc::new(RefCell::new(list))))
}
fn native_input(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if let Some(DgmValue::Str(prompt)) = args.first() { print!("{}", prompt); use std::io::Write; std::io::stdout().flush().ok(); }
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|e| DgmError::RuntimeError { msg: format!("input error: {}", e) })?;
    let out = line.trim_end_matches('\n').trim_end_matches('\r').to_string();
    reserve_string_bytes(out.len())?;
    Ok(DgmValue::Str(out))
}
fn native_abs(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Int(n)) => Ok(DgmValue::Int(n.abs())),
        Some(DgmValue::Float(f)) => Ok(DgmValue::Float(f.abs())),
        _ => Err(DgmError::RuntimeError { msg: "abs() requires number".into() }),
    }
}
fn native_min(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if args.len() == 1 { if let DgmValue::List(l) = &args[0] { let b = l.borrow(); return b.iter().cloned().reduce(|a, b| if cmp_lt(&a, &b) { a } else { b }).ok_or_else(|| DgmError::RuntimeError { msg: "min() empty list".into() }); } }
    args.into_iter().reduce(|a, b| if cmp_lt(&a, &b) { a } else { b }).ok_or_else(|| DgmError::RuntimeError { msg: "min() requires args".into() })
}
fn native_max(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if args.len() == 1 { if let DgmValue::List(l) = &args[0] { let b = l.borrow(); return b.iter().cloned().reduce(|a, b| if cmp_lt(&a, &b) { b } else { a }).ok_or_else(|| DgmError::RuntimeError { msg: "max() empty list".into() }); } }
    args.into_iter().reduce(|a, b| if cmp_lt(&a, &b) { b } else { a }).ok_or_else(|| DgmError::RuntimeError { msg: "max() requires args".into() })
}
fn cmp_lt(a: &DgmValue, b: &DgmValue) -> bool {
    match (a, b) { (DgmValue::Int(x), DgmValue::Int(y)) => x < y, (DgmValue::Float(x), DgmValue::Float(y)) => x < y,
        (DgmValue::Int(x), DgmValue::Float(y)) => (*x as f64) < *y, (DgmValue::Float(x), DgmValue::Int(y)) => *x < (*y as f64), _ => false }
}
fn native_sort(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(l)) => {
            let mut v = l.borrow().clone();
            v.sort_by(|a, b| { if cmp_lt(a, b) { std::cmp::Ordering::Less } else if dgm_eq(a, b) { std::cmp::Ordering::Equal } else { std::cmp::Ordering::Greater } });
            reserve_list_growth(v.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(v))))
        }
        _ => Err(DgmError::RuntimeError { msg: "sort() requires list".into() }),
    }
}
fn native_reverse(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(l)) => {
            let mut v = l.borrow().clone();
            v.reverse();
            reserve_list_growth(v.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(v))))
        }
        Some(DgmValue::Str(s)) => {
            let out: String = s.chars().rev().collect();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        Some(DgmValue::RawJson(text)) => {
            let out: String = text.to_string_lossy().chars().rev().collect();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "reverse() requires list or string".into() }),
    }
}
fn native_keys(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Map(m)) => {
            let keys = sorted_map_entries(&m.borrow())
                .into_iter()
                .map(|(key, _)| DgmValue::Str(key))
                .collect();
            let out = DgmValue::List(Rc::new(RefCell::new(keys)));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        Some(value @ DgmValue::Request(_)) | Some(value @ DgmValue::RequestShell(_)) | Some(value @ DgmValue::RequestMap(_)) | Some(value @ DgmValue::RequestMapView(_)) => {
            let keys = map_like_entries(value)
                .unwrap_or_default()
                .into_iter()
                .map(|(key, _)| DgmValue::Str(key))
                .collect();
            let out = DgmValue::List(Rc::new(RefCell::new(keys)));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        Some(DgmValue::Json(state)) => {
            let keys = match json_resolved_value(state) {
                serde_json::Value::Object(map) => {
                    let mut keys = map.keys().cloned().collect::<Vec<_>>();
                    keys.sort();
                    keys.into_iter().map(DgmValue::Str).collect::<Vec<_>>()
                }
                _ => return Err(DgmError::RuntimeError { msg: "keys() requires map".into() }),
            };
            let out = DgmValue::List(Rc::new(RefCell::new(keys)));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "keys() requires map".into() }),
    }
}
fn native_values(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Map(m)) => {
            let values = sorted_map_entries(&m.borrow())
                .into_iter()
                .map(|(_, value)| value)
                .collect();
            let out = DgmValue::List(Rc::new(RefCell::new(values)));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        Some(value @ DgmValue::Request(_)) | Some(value @ DgmValue::RequestShell(_)) | Some(value @ DgmValue::RequestMap(_)) | Some(value @ DgmValue::RequestMapView(_)) => {
            let values = map_like_entries(value)
                .unwrap_or_default()
                .into_iter()
                .map(|(_, value)| value)
                .collect();
            let out = DgmValue::List(Rc::new(RefCell::new(values)));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        Some(DgmValue::Json(state)) => {
            let values = match json_resolved_value(state) {
                serde_json::Value::Object(_) => json_entries(state)?
                    .into_iter()
                    .map(|(_, value)| value)
                    .collect::<Vec<_>>(),
                _ => return Err(DgmError::RuntimeError { msg: "values() requires map".into() }),
            };
            let out = DgmValue::List(Rc::new(RefCell::new(values)));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "values() requires map".into() }),
    }
}
fn native_has_key(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Map(m)), Some(DgmValue::Str(k))) => Ok(DgmValue::Bool(m.borrow().contains_key(k))),
        (Some(value @ DgmValue::Request(_)), Some(DgmValue::Str(k)))
        | (Some(value @ DgmValue::RequestShell(_)), Some(DgmValue::Str(k)))
        | (Some(value @ DgmValue::RequestMap(_)), Some(DgmValue::Str(k)))
        | (Some(value @ DgmValue::RequestMapView(_)), Some(DgmValue::Str(k))) => {
            Ok(DgmValue::Bool(map_like_contains(value, k)))
        }
        (Some(DgmValue::Json(state)), Some(DgmValue::Str(k))) => Ok(DgmValue::Bool(matches!(
            json_resolved_value(state),
            serde_json::Value::Object(map) if map.contains_key(k)
        ))),
        _ => Err(DgmError::RuntimeError { msg: "has_key(map, key) required".into() }),
    }
}
fn native_slice(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1), args.get(2)) {
        (Some(DgmValue::List(l)), Some(DgmValue::Int(start)), end) => {
            let list = l.borrow(); let s = *start as usize;
            let e = end.and_then(|v| if let DgmValue::Int(n) = v { Some(*n as usize) } else { None }).unwrap_or(list.len());
            let out = list[s..e.min(list.len())].to_vec();
            reserve_list_growth(out.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(out))))
        }
        (Some(DgmValue::Str(s)), Some(DgmValue::Int(start)), end) => {
            let chars: Vec<char> = s.chars().collect();
            let len = chars.len() as i64;
            let st = if *start < 0 { len + start } else { *start }.max(0) as usize;
            let e = end.and_then(|v| if let DgmValue::Int(n) = v { Some(if *n < 0 { len + n } else { *n }) } else { None }).unwrap_or(len).max(0) as usize;
            let out: String = chars[st.min(chars.len())..e.min(chars.len())].iter().collect();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        (Some(DgmValue::RawJson(text)), Some(DgmValue::Int(start)), end) => {
            let rendered = text.to_string_lossy();
            let chars: Vec<char> = rendered.chars().collect();
            let len = chars.len() as i64;
            let st = if *start < 0 { len + start } else { *start }.max(0) as usize;
            let e = end.and_then(|v| if let DgmValue::Int(n) = v { Some(if *n < 0 { len + n } else { *n }) } else { None }).unwrap_or(len).max(0) as usize;
            let out: String = chars[st.min(chars.len())..e.min(chars.len())].iter().collect();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "slice(list/str, start, end?) required".into() }),
    }
}
fn native_join(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(DgmValue::Str(sep))) => {
            let items: Vec<String> = l.borrow().iter().map(|v| format!("{}", v)).collect();
            let out = items.join(sep);
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        (Some(DgmValue::List(l)), None) => {
            let items: Vec<String> = l.borrow().iter().map(|v| format!("{}", v)).collect();
            let out = items.join("");
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "join(list, sep?) required".into() }),
    }
}
fn native_split(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(sep))) => {
            let out = DgmValue::List(Rc::new(RefCell::new(s.split(sep.as_str()).map(|p| DgmValue::Str(p.to_string())).collect())));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "split(str, sep) required".into() }),
    }
}
fn native_replace(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1), args.get(2)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(from)), Some(DgmValue::Str(to))) => {
            let out = s.replace(from.as_str(), to);
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "replace(str, from, to) required".into() }),
    }
}
fn native_upper(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(s)) => {
            let out = s.to_uppercase();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "upper() requires string".into() }),
    }
}
fn native_lower(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(s)) => {
            let out = s.to_lowercase();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "lower() requires string".into() }),
    }
}
fn native_trim(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(s)) => {
            let out = s.trim().to_string();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "trim() requires string".into() }),
    }
}
fn native_contains(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(sub))) => Ok(DgmValue::Bool(s.contains(sub.as_str()))),
        (Some(DgmValue::RawJson(text)), Some(DgmValue::Str(sub))) => Ok(DgmValue::Bool(text.contains(sub.as_str()))),
        (Some(DgmValue::List(l)), Some(v)) => Ok(DgmValue::Bool(l.borrow().iter().any(|x| dgm_eq(x, v)))),
        _ => Err(DgmError::RuntimeError { msg: "contains(str/list, val) required".into() }),
    }
}
fn native_starts_with(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(p))) => Ok(DgmValue::Bool(s.starts_with(p.as_str()))),
        (Some(DgmValue::RawJson(text)), Some(DgmValue::Str(p))) => Ok(DgmValue::Bool(text.starts_with(p.as_str()))),
        _ => Err(DgmError::RuntimeError { msg: "starts_with(str, prefix) required".into() })
    }
}
fn native_ends_with(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(p))) => Ok(DgmValue::Bool(s.ends_with(p.as_str()))),
        (Some(DgmValue::RawJson(text)), Some(DgmValue::Str(p))) => Ok(DgmValue::Bool(text.ends_with(p.as_str()))),
        _ => Err(DgmError::RuntimeError { msg: "ends_with(str, suffix) required".into() })
    }
}
fn native_chars(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(s)) => {
            let out = DgmValue::List(Rc::new(RefCell::new(s.chars().map(|c| DgmValue::Str(c.to_string())).collect())));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        Some(DgmValue::RawJson(text)) => {
            let out = DgmValue::List(Rc::new(RefCell::new(text.to_string_lossy().chars().map(|c| DgmValue::Str(c.to_string())).collect())));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "chars() requires string".into() }),
    }
}
fn native_format(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if args.is_empty() { return Err(DgmError::RuntimeError { msg: "format() requires args".into() }); }
    let template = match &args[0] { DgmValue::Str(s) => s.clone(), _ => return Err(DgmError::RuntimeError { msg: "format() first arg must be string".into() }) };
    let mut result = template;
    for arg in &args[1..] { result = result.replacen("{}", &format!("{}", arg), 1); }
    reserve_string_bytes(result.len())?;
    Ok(DgmValue::Str(result))
}
fn native_map_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(func)) => {
            let mut results = vec![];
            for item in l.borrow().iter() {
                let r = call_native_hof(func, vec![item.clone()])?;
                results.push(r);
            }
            reserve_list_growth(results.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(results))))
        }
        _ => Err(DgmError::RuntimeError { msg: "map(list, fn) required".into() }),
    }
}
fn native_filter(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(func)) => {
            let mut results = vec![];
            for item in l.borrow().iter() {
                let r = call_native_hof(func, vec![item.clone()])?;
                if matches!(r, DgmValue::Bool(true)) { results.push(item.clone()); }
            }
            reserve_list_growth(results.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(results))))
        }
        _ => Err(DgmError::RuntimeError { msg: "filter(list, fn) required".into() }),
    }
}
fn native_reduce(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1), args.get(2)) {
        (Some(DgmValue::List(l)), Some(init), Some(func)) => {
            let mut acc = init.clone();
            for item in l.borrow().iter() { acc = call_native_hof(func, vec![acc, item.clone()])?; }
            Ok(acc)
        }
        _ => Err(DgmError::RuntimeError { msg: "reduce(list, init, fn) required".into() }),
    }
}
fn native_each(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(func)) => {
            for item in l.borrow().iter() { call_native_hof(func, vec![item.clone()])?; }
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "each(list, fn) required".into() }),
    }
}
fn native_find(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(func)) => {
            for item in l.borrow().iter() {
                let r = call_native_hof(func, vec![item.clone()])?;
                if matches!(r, DgmValue::Bool(true)) { return Ok(item.clone()); }
            }
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "find(list, fn) required".into() }),
    }
}
fn native_index_of(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(val)) => {
            for (i, item) in l.borrow().iter().enumerate() { if dgm_eq(item, val) { return Ok(DgmValue::Int(i as i64)); } }
            Ok(DgmValue::Int(-1))
        }
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(sub))) => { Ok(DgmValue::Int(s.find(sub.as_str()).map(|i| i as i64).unwrap_or(-1))) }
        _ => Err(DgmError::RuntimeError { msg: "index_of(list/str, val) required".into() }),
    }
}
fn native_flat(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(l)) => {
            let mut result = vec![];
            for item in l.borrow().iter() {
                if let DgmValue::List(inner) = item { result.extend(inner.borrow().clone()); }
                else { result.push(item.clone()); }
            }
            reserve_list_growth(result.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(result))))
        }
        _ => Err(DgmError::RuntimeError { msg: "flat() requires list".into() }),
    }
}
fn native_zip(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(a)), Some(DgmValue::List(b))) => {
            let a = a.borrow(); let b = b.borrow();
            let result: Vec<DgmValue> = a.iter().zip(b.iter()).map(|(x, y)| DgmValue::List(Rc::new(RefCell::new(vec![x.clone(), y.clone()])))).collect();
            runtime_reserve_value(&DgmValue::List(Rc::new(RefCell::new(result.clone()))))?;
            Ok(DgmValue::List(Rc::new(RefCell::new(result))))
        }
        _ => Err(DgmError::RuntimeError { msg: "zip(list, list) required".into() }),
    }
}
fn native_sum(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(l)) => {
            let mut total = DgmValue::Int(0);
            for item in l.borrow().iter() {
                total = match (&total, item) {
                    (DgmValue::Int(a), DgmValue::Int(b)) => DgmValue::Int(a + b),
                    (DgmValue::Int(a), DgmValue::Float(b)) => DgmValue::Float(*a as f64 + b),
                    (DgmValue::Float(a), DgmValue::Int(b)) => DgmValue::Float(a + *b as f64),
                    (DgmValue::Float(a), DgmValue::Float(b)) => DgmValue::Float(a + b),
                    _ => return Err(DgmError::RuntimeError { msg: "sum() requires list of numbers".into() }),
                };
            }
            Ok(total)
        }
        _ => Err(DgmError::RuntimeError { msg: "sum() requires list".into() }),
    }
}
fn native_any(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(func)) => {
            for item in l.borrow().iter() {
                let r = call_native_hof(func, vec![item.clone()])?;
                if matches!(r, DgmValue::Bool(true)) { return Ok(DgmValue::Bool(true)); }
            }
            Ok(DgmValue::Bool(false))
        }
        (Some(DgmValue::List(l)), None) => { Ok(DgmValue::Bool(l.borrow().iter().any(|v| !matches!(v, DgmValue::Bool(false) | DgmValue::Null)))) }
        _ => Err(DgmError::RuntimeError { msg: "any(list, fn?) required".into() }),
    }
}
fn native_all(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(l)), Some(func)) => {
            for item in l.borrow().iter() {
                let r = call_native_hof(func, vec![item.clone()])?;
                if !matches!(r, DgmValue::Bool(true)) { return Ok(DgmValue::Bool(false)); }
            }
            Ok(DgmValue::Bool(true))
        }
        (Some(DgmValue::List(l)), None) => { Ok(DgmValue::Bool(l.borrow().iter().all(|v| !matches!(v, DgmValue::Bool(false) | DgmValue::Null)))) }
        _ => Err(DgmError::RuntimeError { msg: "all(list, fn?) required".into() }),
    }
}
fn native_print(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let parts: Vec<String> = args.iter().map(|v| format!("{}", v)).collect();
    print!("{}", parts.join(" "));
    use std::io::Write; std::io::stdout().flush().ok();
    Ok(DgmValue::Null)
}
fn native_println(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let parts: Vec<String> = args.iter().map(|v| format!("{}", v)).collect();
    println!("{}", parts.join(" "));
    Ok(DgmValue::Null)
}
fn native_repr(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let out = value_repr(args.first().unwrap_or(&DgmValue::Null));
    reserve_string_bytes(out.len())?;
    Ok(DgmValue::Str(out))
}
fn native_dbg(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let value = args.first().cloned().unwrap_or(DgmValue::Null);
    eprintln!("[dbg] {}", value_repr(&value));
    Ok(value)
}
fn native_assert(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let cond = args.first().cloned().unwrap_or(DgmValue::Bool(false));
    let message = match args.get(1) {
        Some(DgmValue::Str(msg)) => msg.clone(),
        Some(other) => format!("{}", other),
        None => "assertion failed".into(),
    };
    let truthy = match &cond {
        DgmValue::Bool(b) => *b,
        DgmValue::Null => false,
        DgmValue::Int(n) => *n != 0,
        DgmValue::Float(f) => *f != 0.0,
        DgmValue::Str(s) => !s.is_empty(),
        _ => true,
    };
    if truthy {
        Ok(DgmValue::Null)
    } else {
        Err(DgmError::RuntimeError { msg: message })
    }
}
fn native_assert_eq(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if args.len() < 2 {
        return Err(DgmError::RuntimeError { msg: "assert_eq(a, b, msg?) requires at least 2 args".into() });
    }
    if dgm_eq(&args[0], &args[1]) {
        return Ok(DgmValue::Null);
    }
    let message = match args.get(2) {
        Some(DgmValue::Str(msg)) => msg.clone(),
        Some(other) => format!("{}", other),
        None => format!("assert_eq failed: left={} right={}", value_repr(&args[0]), value_repr(&args[1])),
    };
    Err(DgmError::RuntimeError { msg: message })
}
fn native_panic(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let message = args.first().map(value_repr).unwrap_or_else(|| "panic".into());
    Err(DgmError::RuntimeError { msg: message })
}
fn native_chr(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() { Some(DgmValue::Int(n)) => Ok(DgmValue::Str(char::from_u32(*n as u32).unwrap_or('\0').to_string())), _ => Err(DgmError::RuntimeError { msg: "chr() requires int".into() }) }
}
fn native_ord(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() { Some(DgmValue::Str(s)) => s.chars().next().map(|c| DgmValue::Int(c as i64)).ok_or_else(|| DgmError::RuntimeError { msg: "ord() empty string".into() }), _ => Err(DgmError::RuntimeError { msg: "ord() requires string".into() }) }
}
fn native_hex(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() { Some(DgmValue::Int(n)) => Ok(DgmValue::Str(format!("0x{:x}", n))), _ => Err(DgmError::RuntimeError { msg: "hex() requires int".into() }) }
}
fn native_bin(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() { Some(DgmValue::Int(n)) => Ok(DgmValue::Str(format!("0b{:b}", n))), _ => Err(DgmError::RuntimeError { msg: "bin() requires int".into() }) }
}
fn native_exit(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let code = match args.first() { Some(DgmValue::Int(n)) => *n as i32, _ => 0 };
    std::process::exit(code);
}

pub(crate) fn call_native_hof(func: &DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match func {
        DgmValue::NativeFunction { name, func: f } => {
            let _profile_guard = runtime_enter_function_profile(name);
            trap_dgm(&format!("native function '{}'", name), || f(args))
        }
        DgmValue::NativeMethod { name, func: f, state, .. } => {
            let _profile_guard = runtime_enter_function_profile(name);
            trap_dgm(&format!("native method '{}'", name), || f((**state).clone(), args))
        }
        DgmValue::Function { name, params, body, closure, compiled } => {
            let mut args = args.into_iter();
            call_user_function_with_positional_resolver(
                name.as_deref(),
                params,
                body,
                Rc::clone(closure),
                compiled.clone(),
                move || Ok(args.next()),
            )
        }
        _ => Err(DgmError::RuntimeError { msg: "not callable".into() }),
    }
}

pub(crate) fn call_user_function_with_positional_resolver<F>(
    name: Option<&str>,
    params: &[Param],
    body: &[Stmt],
    closure: Rc<RefCell<Environment>>,
    compiled: Option<Arc<CompiledFunctionBlob>>,
    mut next_arg: F,
) -> Result<DgmValue, DgmError>
where
    F: FnMut() -> Result<Option<DgmValue>, DgmError>,
{
    let profile_name = name.unwrap_or("<lambda>");
    let _profile_guard = runtime_enter_function_profile(profile_name);
    let _call_guard = runtime_enter_call(Some(profile_name))?;
    let current_path = module_path_from_env(&closure);
    let mut interp = Interpreter {
        globals: Rc::clone(&closure),
        module_cache: HashMap::new(),
        loading_modules: HashSet::new(),
        bundle: None,
        current_paths: current_path.into_iter().collect(),
        current_module_keys: vec![],
    };
    let call_env = new_child_environment_ref(closure);
    for param in params {
        let value = if let Some(value) = next_arg()? {
            value
        } else if let Some(default) = &param.default {
            interp.eval_expr(default, Rc::clone(&call_env))?
        } else {
            return Err(DgmError::RuntimeError {
                msg: format!("missing required argument '{}' for {}", param.name, profile_name),
            });
        };
        call_env.borrow_mut().set(&param.name, value);
    }
    if next_arg()?.is_some() {
        return Err(DgmError::RuntimeError {
            msg: format!("expected at most {} args, got more in {}", params.len(), profile_name),
        });
    }
    if let Some(compiled) = compiled {
        match vm::execute_function(&mut interp, &compiled, call_env)? {
            ControlFlow::Return(v) => Ok(v),
            _ => Ok(DgmValue::Null),
        }
    } else {
        match interp.exec_block(body, call_env)? {
            ControlFlow::Return(v) => Ok(v),
            _ => Ok(DgmValue::Null),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sensitive_map_values_are_masked_in_display_and_repr() {
        let nested = DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
            ("Authorization".to_string(), DgmValue::Str("Bearer super-secret-token".into())),
        ]))));
        let root = DgmValue::Map(Rc::new(RefCell::new(HashMap::from([
            ("API_KEY".to_string(), DgmValue::Str("sk-prod-1234567890abcd".into())),
            ("headers".to_string(), nested),
        ]))));

        let display = format!("{}", root);
        let repr = value_repr(&root);

        assert!(!display.contains("1234567890"));
        assert!(!repr.contains("super-secret-token"));
        assert!(display.contains("****abcd"));
        assert!(repr.contains("****oken"));
    }
}
