use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::mem;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::ast::{Param, Stmt};
use crate::environment::{begin_environment_cleanup_scope, new_child_environment_ref, new_environment_ref, Environment};
use crate::error::{trap_dgm, DgmError};
use crate::interpreter::{
    call_native_hof, call_user_function_with_positional_resolver, runtime_apply_config, runtime_check_limits,
    runtime_config, runtime_merge_profile,
    runtime_profile_snapshot, runtime_reserve_labeled, runtime_reserve_restored_payload_labeled, runtime_reserve_value,
    runtime_reserve_value_labeled,
    runtime_reset_all, runtime_reset_usage, runtime_usage_snapshot, json_resolved_value, request_backing_live_size,
    request_shell_from_backing, DgmValue, RequestBacking, RuntimeConfig, RuntimeProfile, RuntimeUsageSnapshot,
};
use crate::memory::{maybe_trim_allocator, TrimTrigger};
use crate::request_scope::{begin_request_scope, track_request_json_ref, track_request_map_ref};
use crate::vm::CompiledFunctionBlob;

const ASYNC_WORKER_STACK_SIZE: usize = 512 * 1024;

#[derive(Debug, Clone)]
struct OwnedCall {
    envs: Arc<HashMap<usize, OwnedEnv>>,
    func: Arc<OwnedValue>,
    args: Arc<Vec<OwnedValue>>,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedPayload {
    pub(crate) envs: Arc<HashMap<usize, OwnedEnv>>,
    pub(crate) value: OwnedValue,
}

#[derive(Debug, Clone)]
pub(crate) struct OwnedEnv {
    parent: Option<usize>,
    values: Arc<HashMap<String, OwnedValue>>,
}

#[derive(Debug, Clone)]
pub(crate) enum OwnedValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
    List(Vec<OwnedValue>),
    Map(HashMap<String, OwnedValue>),
    Request(Arc<RequestBacking>),
    RequestMap {
        values: HashMap<String, String>,
    },
    RequestJson {
        content_type: String,
        body: String,
    },
    Function {
        name: Option<String>,
        params: Vec<Param>,
        body: Vec<Stmt>,
        closure: usize,
        compiled: Option<Arc<CompiledFunctionBlob>>,
    },
    NativeFunction { name: String, func: fn(Vec<DgmValue>) -> Result<DgmValue, DgmError> },
    NativeMethod { name: String, state: Box<OwnedValue>, func: fn(DgmValue, Vec<DgmValue>) -> Result<DgmValue, DgmError> },
    Class { name: String, parent: Option<String>, methods: Vec<Stmt>, closure: usize },
    Instance { class_name: String, fields: HashMap<String, OwnedValue> },
    Channel(Arc<ChannelState>),
    TaskHandle(Arc<TaskHandleState>),
    Future(Arc<FutureState>),
}

pub struct ChannelState {
    sender: Mutex<mpsc::Sender<OwnedPayload>>,
    receiver: Mutex<mpsc::Receiver<OwnedPayload>>,
}

pub struct TaskHandleState {
    future: Arc<FutureState>,
}

pub(crate) struct PreparedCall {
    runtime: RuntimeConfig,
    call: OwnedCall,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedCallable {
    runtime: RuntimeConfig,
    envs: Arc<HashMap<usize, OwnedEnv>>,
    func: Arc<OwnedValue>,
}

#[derive(Debug, Clone, Default)]
struct SnapshotMetricsState {
    prepared_call_count: u64,
    prepared_callable_count: u64,
    payload_snapshot_count: u64,
    env_snapshot_count: u64,
    env_snapshot_bytes_estimate: u64,
    value_snapshot_count: u64,
    value_snapshot_bytes_estimate: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct SnapshotMetricsDelta {
    prepared_call_count: u64,
    prepared_callable_count: u64,
    payload_snapshot_count: u64,
    env_snapshot_count: u64,
    env_snapshot_bytes_estimate: u64,
    value_snapshot_count: u64,
    value_snapshot_bytes_estimate: u64,
}

type AsyncValueJobFn = Box<dyn FnOnce() -> Result<DgmValue, DgmError> + Send + 'static>;
type AsyncPayloadJobFn = Box<dyn FnOnce() -> Result<OwnedPayload, DgmError> + Send + 'static>;

enum AsyncJobFn {
    Value(AsyncValueJobFn),
    Payload(AsyncPayloadJobFn),
}

fn empty_owned_envs() -> Arc<HashMap<usize, OwnedEnv>> {
    static EMPTY: OnceLock<Arc<HashMap<usize, OwnedEnv>>> = OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| Arc::new(HashMap::new())))
}

fn empty_owned_value_map() -> Arc<HashMap<String, OwnedValue>> {
    static EMPTY: OnceLock<Arc<HashMap<String, OwnedValue>>> = OnceLock::new();
    Arc::clone(EMPTY.get_or_init(|| Arc::new(HashMap::new())))
}

struct AsyncJob {
    future: Arc<FutureState>,
    runtime: RuntimeConfig,
    func: AsyncJobFn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsyncStatus {
    Pending,
    Running,
    Ready,
    Failed,
    Cancelled,
}

impl AsyncStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone)]
struct FutureInner {
    status: AsyncStatus,
    cancel_requested: bool,
    result: Option<Result<OwnedPayload, DgmError>>,
    profile: Option<RuntimeProfile>,
    profile_merged: bool,
    usage: Option<RuntimeUsageSnapshot>,
    started_at: Option<Instant>,
    completed_at: Option<Instant>,
    worker: Option<String>,
    error: Option<String>,
}

pub struct FutureState {
    id: u64,
    kind: &'static str,
    label: String,
    created_at: Instant,
    inner: Mutex<FutureInner>,
    ready: Condvar,
}

struct AsyncRuntime {
    inner: Arc<AsyncRuntimeInner>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

struct AsyncRuntimeInner {
    state: Mutex<AsyncRuntimeState>,
    notify: Condvar,
}

struct AsyncRuntimeState {
    queue: VecDeque<AsyncJob>,
    worker_count: usize,
    active_jobs: usize,
    submitted_jobs: u64,
    completed_jobs: u64,
    cancelled_jobs: u64,
    next_future_id: u64,
    shutting_down: bool,
    registry: HashMap<u64, Weak<FutureState>>,
}

#[derive(Debug, Clone)]
pub struct AsyncRuntimeSnapshot {
    pub max_threads: usize,
    pub pending_jobs: usize,
    pub running_jobs: usize,
    pub submitted_jobs: u64,
    pub completed_jobs: u64,
    pub cancelled_jobs: u64,
    pub retained_tasks: usize,
    pub oldest_pending_ms: u128,
    pub possible_deadlock: bool,
}

#[derive(Debug, Clone)]
pub struct AsyncTaskSnapshot {
    pub id: u64,
    pub kind: String,
    pub label: String,
    pub status: String,
    pub cancel_requested: bool,
    pub total_time_ms: f64,
    pub queued_ms: f64,
    pub run_ms: f64,
    pub worker: Option<String>,
    pub error: Option<String>,
    pub profile: Option<RuntimeProfile>,
    pub usage: Option<RuntimeUsageSnapshot>,
}

static ASYNC_RUNTIME: OnceLock<Mutex<Option<Arc<AsyncRuntime>>>> = OnceLock::new();
static SNAPSHOT_METRICS: OnceLock<Mutex<SnapshotMetricsState>> = OnceLock::new();

thread_local! {
    static CURRENT_FUTURE: RefCell<Option<Weak<FutureState>>> = const { RefCell::new(None) };
}

struct CurrentFutureGuard;

impl Drop for CurrentFutureGuard {
    fn drop(&mut self) {
        CURRENT_FUTURE.with(|slot| *slot.borrow_mut() = None);
    }
}

impl fmt::Debug for ChannelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ChannelState(..)")
    }
}

impl fmt::Debug for TaskHandleState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status = self.future.status();
        write!(f, "TaskHandleState(id={}, status={})", self.future.id, status.as_str())
    }
}

impl fmt::Debug for FutureState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FutureState(id={}, kind={}, label={})", self.id, self.kind, self.label)
    }
}

impl TaskHandleState {
    fn new(future: Arc<FutureState>) -> Self {
        Self { future }
    }

    fn join_payload(&self) -> Result<OwnedPayload, DgmError> {
        await_future_payload(&self.future)
    }

    fn cancel(&self) -> bool {
        self.future.request_cancel()
    }

    fn snapshot(&self) -> AsyncTaskSnapshot {
        self.future.snapshot()
    }

    fn is_done(&self) -> bool {
        self.future.is_done()
    }
}

impl FutureState {
    fn new(id: u64, kind: &'static str, label: String) -> Self {
        Self {
            id,
            kind,
            label,
            created_at: Instant::now(),
            inner: Mutex::new(FutureInner {
                status: AsyncStatus::Pending,
                cancel_requested: false,
                result: None,
                profile: None,
                profile_merged: false,
                usage: None,
                started_at: None,
                completed_at: None,
                worker: None,
                error: None,
            }),
            ready: Condvar::new(),
        }
    }

    fn status(&self) -> AsyncStatus {
        self.inner.lock().unwrap().status
    }

    fn is_done(&self) -> bool {
        matches!(self.status(), AsyncStatus::Ready | AsyncStatus::Failed | AsyncStatus::Cancelled)
    }

    fn request_cancel(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        match inner.status {
            AsyncStatus::Ready | AsyncStatus::Failed | AsyncStatus::Cancelled => return false,
            AsyncStatus::Pending => {
                inner.cancel_requested = true;
                inner.status = AsyncStatus::Cancelled;
                inner.completed_at = Some(Instant::now());
                inner.error = Some(format!("{} cancelled", self.kind));
                self.ready.notify_all();
                true
            }
            AsyncStatus::Running => {
                inner.cancel_requested = true;
                inner.error = Some(format!("{} cancelled", self.kind));
                self.ready.notify_all();
                true
            }
        }
    }

    fn cancel_requested(&self) -> bool {
        self.inner.lock().unwrap().cancel_requested
    }

    fn mark_running(&self, worker: String) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.status == AsyncStatus::Cancelled {
            inner.worker = Some(worker);
            return false;
        }
        inner.status = AsyncStatus::Running;
        inner.started_at = Some(Instant::now());
        inner.worker = Some(worker);
        true
    }

    fn complete(
        &self,
        result: Result<OwnedPayload, DgmError>,
        profile: RuntimeProfile,
        usage: RuntimeUsageSnapshot,
        worker: String,
    ) {
        let mut inner = self.inner.lock().unwrap();
        inner.worker = Some(worker);
        inner.completed_at = Some(Instant::now());
        inner.profile = Some(profile);
        inner.usage = Some(usage);
        let cancelled = inner.cancel_requested;
        inner.error = if cancelled {
            Some(format!("{} cancelled", self.kind))
        } else {
            result.as_ref().err().map(|err| format!("{}", err))
        };
        inner.status = if cancelled {
            AsyncStatus::Cancelled
        } else if result.is_ok() {
            AsyncStatus::Ready
        } else {
            AsyncStatus::Failed
        };
        inner.result = Some(result);
        self.ready.notify_all();
    }

    fn poll_inner(&self) -> FutureInner {
        self.inner.lock().unwrap().clone()
    }

    fn snapshot(&self) -> AsyncTaskSnapshot {
        let now = Instant::now();
        let inner = self.poll_inner();
        let started = inner.started_at.unwrap_or(now);
        let ended = inner.completed_at.unwrap_or(now);
        let total_time_ms = ended.duration_since(self.created_at).as_secs_f64() * 1000.0;
        let queued_ms = if inner.started_at.is_some() {
            started.duration_since(self.created_at).as_secs_f64() * 1000.0
        } else {
            now.duration_since(self.created_at).as_secs_f64() * 1000.0
        };
        let run_ms = if inner.started_at.is_some() {
            ended.duration_since(started).as_secs_f64() * 1000.0
        } else {
            0.0
        };
        AsyncTaskSnapshot {
            id: self.id,
            kind: self.kind.into(),
            label: self.label.clone(),
            status: inner.status.as_str().into(),
            cancel_requested: inner.cancel_requested,
            total_time_ms,
            queued_ms,
            run_ms,
            worker: inner.worker.clone(),
            error: inner.error.clone(),
            profile: inner.profile.clone(),
            usage: inner.usage.clone(),
        }
    }

    fn cancelled_error(&self) -> DgmError {
        DgmError::RuntimeError {
            msg: format!("{} cancelled", self.kind),
        }
    }
}

impl AsyncRuntime {
    fn new(worker_count: usize) -> Self {
        let inner = Arc::new(AsyncRuntimeInner {
            state: Mutex::new(AsyncRuntimeState {
                queue: VecDeque::new(),
                worker_count,
                active_jobs: 0,
                submitted_jobs: 0,
                completed_jobs: 0,
                cancelled_jobs: 0,
                next_future_id: 0,
                shutting_down: false,
                registry: HashMap::new(),
            }),
            notify: Condvar::new(),
        });
        let mut workers = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let worker_inner = Arc::clone(&inner);
            let fallback_inner = Arc::clone(&inner);
            let name = format!("dgm-async-worker-{}", index + 1);
            let worker = thread::Builder::new()
                .name(name)
                .stack_size(ASYNC_WORKER_STACK_SIZE)
                .spawn(move || worker_loop(worker_inner, index))
                .unwrap_or_else(|_| thread::spawn(move || worker_loop(fallback_inner, index)));
            workers.push(worker);
        }
        Self {
            inner,
            workers: Mutex::new(workers),
        }
    }

    fn worker_count(&self) -> usize {
        self.inner.state.lock().unwrap().worker_count
    }

    fn is_idle(&self) -> bool {
        let state = self.inner.state.lock().unwrap();
        state.queue.is_empty() && state.active_jobs == 0
    }

    fn shutdown(&self) {
        {
            let mut state = self.inner.state.lock().unwrap();
            state.shutting_down = true;
            self.inner.notify.notify_all();
        }
        for worker in self.workers.lock().unwrap().drain(..) {
            let _ = worker.join();
        }
    }

    fn submit<F>(&self, kind: &'static str, label: String, runtime: RuntimeConfig, job: F) -> Result<Arc<FutureState>, DgmError>
    where
        F: FnOnce() -> Result<DgmValue, DgmError> + Send + 'static,
    {
        self.submit_inner(kind, label, runtime, AsyncJobFn::Value(Box::new(job)))
    }

    fn submit_payload<F>(&self, kind: &'static str, label: String, runtime: RuntimeConfig, job: F) -> Result<Arc<FutureState>, DgmError>
    where
        F: FnOnce() -> Result<OwnedPayload, DgmError> + Send + 'static,
    {
        self.submit_inner(kind, label, runtime, AsyncJobFn::Payload(Box::new(job)))
    }

    fn submit_inner(
        &self,
        kind: &'static str,
        label: String,
        runtime: RuntimeConfig,
        func: AsyncJobFn,
    ) -> Result<Arc<FutureState>, DgmError> {
        let future = {
            let mut state = self.inner.state.lock().unwrap();
            if state.shutting_down {
                return Err(DgmError::RuntimeError { msg: "async runtime unavailable".into() });
            }
            state.next_future_id = state.next_future_id.saturating_add(1);
            let future = Arc::new(FutureState::new(state.next_future_id, kind, label));
            state.submitted_jobs = state.submitted_jobs.saturating_add(1);
            state.registry.insert(future.id, Arc::downgrade(&future));
            state.queue.push_back(AsyncJob {
                future: Arc::clone(&future),
                runtime,
                func,
            });
            future
        };
        self.inner.notify.notify_one();
        Ok(future)
    }

    fn snapshots(&self) -> (AsyncRuntimeSnapshot, Vec<AsyncTaskSnapshot>) {
        let (max_threads, pending_jobs, running_jobs, submitted_jobs, completed_jobs, cancelled_jobs, futures) = {
            let mut state = self.inner.state.lock().unwrap();
            state.registry.retain(|_, weak| weak.strong_count() > 0);
            let futures = state
                .registry
                .values()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>();
            (
                state.worker_count,
                state.queue.len(),
                state.active_jobs,
                state.submitted_jobs,
                state.completed_jobs,
                state.cancelled_jobs,
                futures,
            )
        };
        let mut tasks = futures
            .into_iter()
            .map(|future| future.snapshot())
            .collect::<Vec<_>>();
        tasks.sort_by(|a, b| a.id.cmp(&b.id));
        let oldest_pending_ms = tasks
            .iter()
            .filter(|task| task.status == "pending")
            .map(|task| task.total_time_ms as u128)
            .max()
            .unwrap_or(0);
        let snapshot = AsyncRuntimeSnapshot {
            max_threads,
            pending_jobs,
            running_jobs,
            submitted_jobs,
            completed_jobs,
            cancelled_jobs,
            retained_tasks: tasks.len(),
            oldest_pending_ms,
            possible_deadlock: pending_jobs > 0 && running_jobs >= max_threads && oldest_pending_ms >= 250,
        };
        (snapshot, tasks)
    }
}

fn worker_loop(inner: Arc<AsyncRuntimeInner>, index: usize) {
    let worker_name = format!("async-worker-{}", index + 1);
    loop {
        let job = {
            let mut state = inner.state.lock().unwrap();
            loop {
                if state.shutting_down {
                    return;
                }
                if let Some(job) = state.queue.pop_front() {
                    state.active_jobs = state.active_jobs.saturating_add(1);
                    break job;
                }
                state = inner.notify.wait(state).unwrap();
            }
        };

        if !job.future.mark_running(worker_name.clone()) {
            let mut state = inner.state.lock().unwrap();
            state.active_jobs = state.active_jobs.saturating_sub(1);
            state.cancelled_jobs = state.cancelled_jobs.saturating_add(1);
            continue;
        }

        let future = Arc::clone(&job.future);
        let runtime = job.runtime;
        let func = job.func;
        let result = trap_dgm("async task", || {
            runtime_reset_all();
            runtime_apply_config(&runtime)?;
            runtime_reset_usage();
            CURRENT_FUTURE.with(|slot| *slot.borrow_mut() = Some(Arc::downgrade(&future)));
            let _guard = CurrentFutureGuard;
            match func {
                AsyncJobFn::Value(job) => job().and_then(|value| snapshot_payload(&value)),
                AsyncJobFn::Payload(job) => job(),
            }
        });
        let profile = runtime_profile_snapshot();
        let usage = runtime_usage_snapshot();
        let payload = match result {
            Ok(payload) => Ok(payload),
            Err(err) => Err(err),
        };
        future.complete(payload, profile, usage, worker_name.clone());

        let mut state = inner.state.lock().unwrap();
        state.active_jobs = state.active_jobs.saturating_sub(1);
        if future.status() == AsyncStatus::Cancelled {
            state.cancelled_jobs = state.cancelled_jobs.saturating_add(1);
        } else {
            state.completed_jobs = state.completed_jobs.saturating_add(1);
        }
        let should_trim_idle = state.queue.is_empty() && state.active_jobs == 0;
        let should_trim_periodic = state.completed_jobs > 0 && state.completed_jobs % 64 == 0;
        drop(state);
        if should_trim_idle {
            let _ = maybe_trim_allocator(TrimTrigger::Idle);
        } else if should_trim_periodic {
            let _ = maybe_trim_allocator(TrimTrigger::Periodic);
        }
    }
}

fn async_runtime_holder() -> &'static Mutex<Option<Arc<AsyncRuntime>>> {
    ASYNC_RUNTIME.get_or_init(|| Mutex::new(None))
}

fn snapshot_metrics_holder() -> &'static Mutex<SnapshotMetricsState> {
    SNAPSHOT_METRICS.get_or_init(|| Mutex::new(SnapshotMetricsState::default()))
}

fn lock_snapshot_metrics() -> std::sync::MutexGuard<'static, SnapshotMetricsState> {
    match snapshot_metrics_holder().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn record_snapshot_metrics(delta: SnapshotMetricsDelta) {
    let mut metrics = lock_snapshot_metrics();
    metrics.prepared_call_count = metrics.prepared_call_count.saturating_add(delta.prepared_call_count);
    metrics.prepared_callable_count = metrics.prepared_callable_count.saturating_add(delta.prepared_callable_count);
    metrics.payload_snapshot_count = metrics.payload_snapshot_count.saturating_add(delta.payload_snapshot_count);
    metrics.env_snapshot_count = metrics.env_snapshot_count.saturating_add(delta.env_snapshot_count);
    metrics.env_snapshot_bytes_estimate = metrics
        .env_snapshot_bytes_estimate
        .saturating_add(delta.env_snapshot_bytes_estimate);
    metrics.value_snapshot_count = metrics.value_snapshot_count.saturating_add(delta.value_snapshot_count);
    metrics.value_snapshot_bytes_estimate = metrics
        .value_snapshot_bytes_estimate
        .saturating_add(delta.value_snapshot_bytes_estimate);
}

fn current_async_runtime() -> Arc<AsyncRuntime> {
    let desired = runtime_config().max_threads.max(1);
    configure_async_runtime(desired);
    let mut holder = async_runtime_holder().lock().unwrap();
    if let Some(runtime) = holder.as_ref() {
        return Arc::clone(runtime);
    }
    let runtime = Arc::new(AsyncRuntime::new(desired));
    *holder = Some(Arc::clone(&runtime));
    runtime
}

pub(crate) fn configure_async_runtime(worker_count: usize) {
    if worker_count == 0 {
        return;
    }
    let old_runtime = {
        let mut holder = async_runtime_holder().lock().unwrap();
        match holder.as_ref() {
            Some(runtime) if runtime.worker_count() == worker_count => return,
            Some(runtime) if !runtime.is_idle() => return,
            _ => {}
        }
        let old = holder.take();
        *holder = Some(Arc::new(AsyncRuntime::new(worker_count)));
        old
    };
    if let Some(runtime) = old_runtime {
        runtime.shutdown();
    }
}

#[derive(Default)]
struct SnapshotContext {
    envs: HashMap<usize, OwnedEnv>,
    visiting_lists: HashSet<usize>,
    visiting_maps: HashSet<usize>,
    stats: SnapshotMetricsDelta,
}

struct RestoreContext {
    env_sets: Vec<Arc<HashMap<usize, OwnedEnv>>>,
    cache: HashMap<usize, Rc<RefCell<Environment>>>,
}

impl RestoreContext {
    fn new(envs: Arc<HashMap<usize, OwnedEnv>>) -> Self {
        Self { env_sets: vec![envs], cache: HashMap::new() }
    }

    fn push_envs(&mut self, envs: Arc<HashMap<usize, OwnedEnv>>) {
        if self.env_sets.iter().any(|existing| Arc::ptr_eq(existing, &envs)) {
            return;
        }
        self.env_sets.push(envs);
    }

    fn find_env(&self, id: usize) -> Option<OwnedEnv> {
        self.env_sets
            .iter()
            .rev()
            .find_map(|envs| envs.get(&id).cloned())
    }

    fn restore_env(&mut self, id: usize) -> Result<Rc<RefCell<Environment>>, DgmError> {
        if let Some(env) = self.cache.get(&id) {
            return Ok(Rc::clone(env));
        }
        let snapshot = self.find_env(id)
            .ok_or_else(|| DgmError::RuntimeError { msg: "missing environment snapshot".into() })?;
        let env = if let Some(parent) = snapshot.parent {
            new_child_environment_ref(self.restore_env(parent)?)
        } else {
            new_environment_ref()
        };
        self.cache.insert(id, Rc::clone(&env));
        for (key, value) in snapshot.values.iter() {
            let restored = self.restore_value(&value)?;
            env.borrow_mut().set(key, restored);
        }
        Ok(env)
    }

    fn restore_value(&mut self, value: &OwnedValue) -> Result<DgmValue, DgmError> {
        match value {
            OwnedValue::Int(n) => Ok(DgmValue::Int(*n)),
            OwnedValue::Float(f) => Ok(DgmValue::Float(*f)),
            OwnedValue::Str(s) => Ok(DgmValue::Str(s.clone())),
            OwnedValue::Bool(b) => Ok(DgmValue::Bool(*b)),
            OwnedValue::Null => Ok(DgmValue::Null),
            OwnedValue::List(items) => {
                let restored = items.iter().map(|item| self.restore_value(item)).collect::<Result<Vec<_>, _>>()?;
                Ok(DgmValue::List(Rc::new(RefCell::new(restored))))
            }
            OwnedValue::Map(map) => {
                let mut restored = HashMap::new();
                for (key, value) in map {
                    restored.insert(key.clone(), self.restore_value(value)?);
                }
                Ok(DgmValue::Map(Rc::new(RefCell::new(restored))))
            }
            OwnedValue::Request(backing) => Ok(request_shell_from_backing(Arc::clone(backing))),
            OwnedValue::RequestMap { values } => {
                let map = Rc::new(crate::interpreter::RequestMapValue::new(Arc::new(values.clone())));
                track_request_map_ref(&map);
                Ok(DgmValue::RequestMap(map))
            }
            OwnedValue::RequestJson { content_type, body } => {
                let json = Rc::new(crate::interpreter::RequestJsonValue::new(
                    Arc::<str>::from(content_type.clone()),
                    Arc::<str>::from(body.clone()),
                ));
                track_request_json_ref(&json);
                Ok(DgmValue::RequestJson(json))
            }
            OwnedValue::Function { name, params, body, closure, compiled } => Ok(DgmValue::Function {
                name: name.clone(),
                params: params.clone(),
                body: if compiled.is_some() { Vec::new() } else { body.clone() },
                closure: self.restore_env(*closure)?,
                compiled: compiled.clone().or_else(|| Some(crate::vm::compile_function_blob(name.clone(), params, body))),
            }),
            OwnedValue::NativeFunction { name, func } => Ok(DgmValue::NativeFunction { name: name.clone(), func: *func }),
            OwnedValue::NativeMethod { name, state, func } => Ok(DgmValue::NativeMethod {
                name: name.clone(),
                state: Box::new(self.restore_value(state)?),
                func: *func,
            }),
            OwnedValue::Class { name, parent, methods, closure } => Ok(DgmValue::Class {
                name: name.clone(),
                parent: parent.clone(),
                methods: methods.clone(),
                closure: self.restore_env(*closure)?,
            }),
            OwnedValue::Instance { class_name, fields } => {
                let mut restored = HashMap::new();
                for (key, value) in fields {
                    restored.insert(key.clone(), self.restore_value(value)?);
                }
                Ok(DgmValue::Instance {
                    class_name: class_name.clone(),
                    fields: Rc::new(RefCell::new(restored)),
                })
            }
            OwnedValue::Channel(state) => Ok(DgmValue::Channel(Arc::clone(state))),
            OwnedValue::TaskHandle(state) => Ok(DgmValue::TaskHandle(Arc::clone(state))),
            OwnedValue::Future(state) => Ok(DgmValue::Future(Arc::clone(state))),
        }
    }

    fn restore_owned_value(&mut self, value: OwnedValue) -> Result<DgmValue, DgmError> {
        match value {
            OwnedValue::Int(n) => Ok(DgmValue::Int(n)),
            OwnedValue::Float(f) => Ok(DgmValue::Float(f)),
            OwnedValue::Str(s) => Ok(DgmValue::Str(s)),
            OwnedValue::Bool(b) => Ok(DgmValue::Bool(b)),
            OwnedValue::Null => Ok(DgmValue::Null),
            OwnedValue::List(items) => {
                let restored = items
                    .into_iter()
                    .map(|item| self.restore_owned_value(item))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(DgmValue::List(Rc::new(RefCell::new(restored))))
            }
            OwnedValue::Map(map) => {
                let mut restored = HashMap::with_capacity(map.len());
                for (key, value) in map {
                    restored.insert(key, self.restore_owned_value(value)?);
                }
                Ok(DgmValue::Map(Rc::new(RefCell::new(restored))))
            }
            OwnedValue::Request(backing) => Ok(request_shell_from_backing(backing)),
            OwnedValue::RequestMap { values } => {
                let map = Rc::new(crate::interpreter::RequestMapValue::new(Arc::new(values)));
                track_request_map_ref(&map);
                Ok(DgmValue::RequestMap(map))
            }
            OwnedValue::RequestJson { content_type, body } => {
                let json = Rc::new(crate::interpreter::RequestJsonValue::new(
                    Arc::<str>::from(content_type),
                    Arc::<str>::from(body),
                ));
                track_request_json_ref(&json);
                Ok(DgmValue::RequestJson(json))
            }
            OwnedValue::Function { name, params, body, closure, compiled } => {
                let compiled_blob = compiled
                    .clone()
                    .or_else(|| Some(crate::vm::compile_function_blob(name.clone(), &params, &body)));
                Ok(DgmValue::Function {
                    name,
                    params,
                    body: if compiled.is_some() { Vec::new() } else { body },
                    closure: self.restore_env(closure)?,
                    compiled: compiled_blob,
                })
            }
            OwnedValue::NativeFunction { name, func } => Ok(DgmValue::NativeFunction { name, func }),
            OwnedValue::NativeMethod { name, state, func } => Ok(DgmValue::NativeMethod {
                name,
                state: Box::new(self.restore_owned_value(*state)?),
                func,
            }),
            OwnedValue::Class { name, parent, methods, closure } => Ok(DgmValue::Class {
                name,
                parent,
                methods,
                closure: self.restore_env(closure)?,
            }),
            OwnedValue::Instance { class_name, fields } => {
                let mut restored = HashMap::with_capacity(fields.len());
                for (key, value) in fields {
                    restored.insert(key, self.restore_owned_value(value)?);
                }
                Ok(DgmValue::Instance {
                    class_name,
                    fields: Rc::new(RefCell::new(restored)),
                })
            }
            OwnedValue::Channel(state) => Ok(DgmValue::Channel(state)),
            OwnedValue::TaskHandle(state) => Ok(DgmValue::TaskHandle(state)),
            OwnedValue::Future(state) => Ok(DgmValue::Future(state)),
        }
    }

    fn restore_payload(&mut self, payload: OwnedPayload) -> Result<DgmValue, DgmError> {
        if !payload.envs.is_empty() {
            self.push_envs(payload.envs);
        }
        self.restore_owned_value(payload.value)
    }

    fn teardown_restored_envs(&mut self) {
        for env in self.cache.values() {
            env.borrow_mut().clear_values();
        }
        self.cache.clear();
    }
}

struct RestoreContextTeardownGuard<'a>(&'a mut RestoreContext);

impl Drop for RestoreContextTeardownGuard<'_> {
    fn drop(&mut self) {
        self.0.teardown_restored_envs();
    }
}

impl SnapshotContext {
    fn snapshot_json_value(value: &serde_json::Value) -> OwnedValue {
        match value {
            serde_json::Value::Null => OwnedValue::Null,
            serde_json::Value::Bool(value) => OwnedValue::Bool(*value),
            serde_json::Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    OwnedValue::Int(value)
                } else {
                    OwnedValue::Float(number.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(text) => OwnedValue::Str(text.clone()),
            serde_json::Value::Array(items) => {
                OwnedValue::List(items.iter().map(Self::snapshot_json_value).collect())
            }
            serde_json::Value::Object(map) => OwnedValue::Map(
                map.iter()
                    .map(|(key, value)| (key.clone(), Self::snapshot_json_value(value)))
                    .collect(),
            ),
        }
    }

    fn snapshot_call(&mut self, func: &DgmValue, args: &[DgmValue]) -> Result<OwnedCall, DgmError> {
        let func = Arc::new(self.snapshot_value(func)?);
        let args = Arc::new(
            args.iter()
                .map(|arg| self.snapshot_value(arg))
                .collect::<Result<Vec<_>, _>>()?,
        );
        Ok(OwnedCall { envs: Arc::new(self.envs.clone()), func, args })
    }

    fn snapshot_payload(&mut self, value: &DgmValue) -> Result<OwnedPayload, DgmError> {
        let value = self.snapshot_value(value)?;
        let envs = if self.envs.is_empty() {
            empty_owned_envs()
        } else {
            Arc::new(self.envs.clone())
        };
        Ok(OwnedPayload { envs, value })
    }

    fn snapshot_env(&mut self, env: &Rc<RefCell<Environment>>) -> Result<usize, DgmError> {
        let id = Rc::as_ptr(env) as usize;
        if self.envs.contains_key(&id) {
            return Ok(id);
        }
        self.envs.insert(id, OwnedEnv { parent: None, values: empty_owned_value_map() });
        let env_ref = env.borrow();
        let parent = env_ref.parent().map(|parent| self.snapshot_env(&parent)).transpose()?;
        let mut values = HashMap::new();
        for (key, value) in env_ref.entries() {
            values.insert(key, self.snapshot_value(&value)?);
        }
        let owned_env = OwnedEnv {
            parent,
            values: if values.is_empty() { empty_owned_value_map() } else { Arc::new(values) },
        };
        self.stats.env_snapshot_count = self.stats.env_snapshot_count.saturating_add(1);
        self.stats.env_snapshot_bytes_estimate = self
            .stats
            .env_snapshot_bytes_estimate
            .saturating_add(estimate_owned_env_size(&owned_env) as u64);
        self.envs.insert(id, owned_env);
        Ok(id)
    }

    fn snapshot_value(&mut self, value: &DgmValue) -> Result<OwnedValue, DgmError> {
        let snap = match value {
            DgmValue::Int(n) => Ok(OwnedValue::Int(*n)),
            DgmValue::Float(f) => Ok(OwnedValue::Float(*f)),
            DgmValue::Str(s) => Ok(OwnedValue::Str(s.clone())),
            DgmValue::Bool(b) => Ok(OwnedValue::Bool(*b)),
            DgmValue::Null => Ok(OwnedValue::Null),
            DgmValue::List(items) => {
                let id = Rc::as_ptr(items) as usize;
                if !self.visiting_lists.insert(id) {
                    return Err(DgmError::RuntimeError { msg: "cannot send cyclic list across thread".into() });
                }
                let snap = items.borrow().iter().map(|item| self.snapshot_value(item)).collect::<Result<Vec<_>, _>>()?;
                self.visiting_lists.remove(&id);
                Ok(OwnedValue::List(snap))
            }
            DgmValue::Map(map) => {
                let id = Rc::as_ptr(map) as usize;
                if !self.visiting_maps.insert(id) {
                    return Err(DgmError::RuntimeError { msg: "cannot send cyclic map across thread".into() });
                }
                let mut snap = HashMap::new();
                for (key, value) in map.borrow().iter() {
                    snap.insert(key.clone(), self.snapshot_value(value)?);
                }
                self.visiting_maps.remove(&id);
                Ok(OwnedValue::Map(snap))
            }
            DgmValue::Request(request) => Ok(OwnedValue::Request(Arc::new(RequestBacking {
                method: request.method.clone(),
                url: request.url.clone(),
                path: request.path.clone(),
                headers_values: Arc::new(request.headers_values().clone()),
                query_values: Arc::new(request.query_values().clone()),
                params_values: Arc::new(request.params_values().clone()),
                body: Arc::<str>::from(request.body_text().to_owned()),
                content_type: Arc::<str>::from(request.content_type_text().to_owned()),
            }))),
            DgmValue::RequestShell(request) => Ok(OwnedValue::Request(Arc::clone(request.backing()))),
            DgmValue::RequestMap(map) => Ok(OwnedValue::RequestMap {
                values: map.values.as_ref().clone(),
            }),
            DgmValue::RequestMapView(values) => Ok(OwnedValue::RequestMap {
                values: values.as_ref().clone(),
            }),
            DgmValue::RequestJson(state) => Ok(OwnedValue::RequestJson {
                content_type: state.content_type.as_ref().to_owned(),
                body: state.body.as_ref().to_owned(),
            }),
            DgmValue::RawJson(text) => Ok(OwnedValue::Str(text.to_string_lossy())),
            DgmValue::Json(state) => Ok(Self::snapshot_json_value(json_resolved_value(state))),
            DgmValue::Function { name, params, body, closure, compiled } => Ok(OwnedValue::Function {
                name: name.clone(),
                params: params.clone(),
                body: body.clone(),
                closure: self.snapshot_env(closure)?,
                compiled: compiled.clone(),
            }),
            DgmValue::NativeFunction { name, func } => Ok(OwnedValue::NativeFunction { name: name.clone(), func: *func }),
            DgmValue::NativeMethod { name, state, func } => Ok(OwnedValue::NativeMethod {
                name: name.clone(),
                state: Box::new(self.snapshot_value(state)?),
                func: *func,
            }),
            DgmValue::Class { name, parent, methods, closure } => Ok(OwnedValue::Class {
                name: name.clone(),
                parent: parent.clone(),
                methods: methods.clone(),
                closure: self.snapshot_env(closure)?,
            }),
            DgmValue::Instance { class_name, fields } => {
                let id = Rc::as_ptr(fields) as usize;
                if !self.visiting_maps.insert(id) {
                    return Err(DgmError::RuntimeError { msg: "cannot send cyclic instance across thread".into() });
                }
                let mut snap = HashMap::new();
                for (key, value) in fields.borrow().iter() {
                    snap.insert(key.clone(), self.snapshot_value(value)?);
                }
                self.visiting_maps.remove(&id);
                Ok(OwnedValue::Instance { class_name: class_name.clone(), fields: snap })
            }
            DgmValue::Channel(state) => Ok(OwnedValue::Channel(Arc::clone(state))),
            DgmValue::TaskHandle(state) => Ok(OwnedValue::TaskHandle(Arc::clone(state))),
            DgmValue::Future(state) => Ok(OwnedValue::Future(Arc::clone(state))),
        }?;
        self.stats.value_snapshot_count = self.stats.value_snapshot_count.saturating_add(1);
        self.stats.value_snapshot_bytes_estimate = self
            .stats
            .value_snapshot_bytes_estimate
            .saturating_add(estimate_owned_value_shallow_size(&snap) as u64);
        Ok(snap)
    }
}

fn estimate_owned_env_size(env: &OwnedEnv) -> usize {
    let parent_size = if env.parent.is_some() { mem::size_of::<usize>() } else { 0 };
    mem::size_of::<OwnedEnv>()
        .saturating_add(parent_size)
        .saturating_add(mem::size_of::<HashMap<String, OwnedValue>>())
        .saturating_add(
            env.values
                .iter()
                .map(|(key, _)| mem::size_of::<String>().saturating_add(key.len()).saturating_add(mem::size_of::<OwnedValue>()))
                .sum::<usize>(),
        )
}

fn estimate_owned_value_shallow_size(value: &OwnedValue) -> usize {
    match value {
        OwnedValue::Int(_) => mem::size_of::<i64>(),
        OwnedValue::Float(_) => mem::size_of::<f64>(),
        OwnedValue::Str(text) => mem::size_of::<String>().saturating_add(text.len()),
        OwnedValue::Bool(_) => mem::size_of::<bool>(),
        OwnedValue::Null => 0,
        OwnedValue::List(items) => mem::size_of::<Vec<OwnedValue>>().saturating_add(items.len().saturating_mul(mem::size_of::<OwnedValue>())),
        OwnedValue::Map(map) => mem::size_of::<HashMap<String, OwnedValue>>().saturating_add(
            map.iter()
                .map(|(key, _)| mem::size_of::<String>().saturating_add(key.len()).saturating_add(mem::size_of::<OwnedValue>()))
                .sum::<usize>(),
        ),
        OwnedValue::Request(backing) => request_backing_live_size(backing.as_ref()),
        OwnedValue::RequestMap { values } => {
            values.iter().map(|(key, value)| key.len().saturating_add(value.len())).sum::<usize>()
        }
        OwnedValue::RequestJson { content_type, body } => content_type.len().saturating_add(body.len()),
        OwnedValue::Function { name, params, body, compiled, .. } => {
            mem::size_of::<OwnedValue>()
                .saturating_add(name.as_ref().map(|text| text.len()).unwrap_or(0))
                .saturating_add(params.iter().map(|param| param.name.len()).sum::<usize>())
                .saturating_add(body.len().saturating_mul(mem::size_of::<Stmt>()))
                .saturating_add(
                    compiled
                        .as_ref()
                        .map(|blob| mem::size_of::<Arc<CompiledFunctionBlob>>() + blob.chunk.instructions.len().saturating_mul(mem::size_of::<crate::vm::Instruction>()))
                        .unwrap_or(0),
                )
        }
        OwnedValue::NativeFunction { name, .. } => mem::size_of::<OwnedValue>().saturating_add(name.len()),
        OwnedValue::NativeMethod { name, state, .. } => {
            mem::size_of::<OwnedValue>()
                .saturating_add(name.len())
                .saturating_add(estimate_owned_value_shallow_size(state))
        }
        OwnedValue::Class { name, parent, methods, .. } => {
            mem::size_of::<OwnedValue>()
                .saturating_add(name.len())
                .saturating_add(parent.as_ref().map(|text| text.len()).unwrap_or(0))
                .saturating_add(methods.len().saturating_mul(mem::size_of::<Stmt>()))
        }
        OwnedValue::Instance { class_name, fields } => {
            mem::size_of::<OwnedValue>()
                .saturating_add(class_name.len())
                .saturating_add(
                    fields
                        .iter()
                        .map(|(key, _)| mem::size_of::<String>().saturating_add(key.len()).saturating_add(mem::size_of::<OwnedValue>()))
                        .sum::<usize>(),
                )
        }
        OwnedValue::Channel(_) | OwnedValue::TaskHandle(_) | OwnedValue::Future(_) => mem::size_of::<Arc<()>>(),
    }
}

pub fn native_spawn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "spawn(fn, args...) requires callable".into() });
    }
    let func = args[0].clone();
    if !is_callable(&func) {
        return Err(DgmError::RuntimeError { msg: "spawn requires callable first argument".into() });
    }
    let handle = spawn_task(func, args[1..].to_vec())?;
    task_handle_value(handle)
}

pub fn native_channel(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "channel() does not take arguments".into() });
    }
    let (sender, receiver) = mpsc::channel::<OwnedPayload>();
    channel_value(Arc::new(ChannelState {
        sender: Mutex::new(sender),
        receiver: Mutex::new(receiver),
    }))
}

pub(crate) fn prepare_isolated_call(func: DgmValue, args: Vec<DgmValue>) -> Result<PreparedCall, DgmError> {
    let runtime = runtime_config();
    let mut snapshot = SnapshotContext::default();
    let call = snapshot.snapshot_call(&func, &args)?;
    snapshot.stats.prepared_call_count = snapshot.stats.prepared_call_count.saturating_add(1);
    record_snapshot_metrics(snapshot.stats);
    Ok(PreparedCall { runtime, call })
}

pub(crate) fn prepare_isolated_callable(func: DgmValue) -> Result<PreparedCallable, DgmError> {
    let runtime = runtime_config();
    let mut snapshot = SnapshotContext::default();
    let func = Arc::new(snapshot.snapshot_value(&func)?);
    snapshot.stats.prepared_callable_count = snapshot.stats.prepared_callable_count.saturating_add(1);
    record_snapshot_metrics(snapshot.stats);
    Ok(PreparedCallable {
        runtime,
        envs: Arc::new(snapshot.envs),
        func,
    })
}

pub(crate) fn owned_payload(value: OwnedValue) -> OwnedPayload {
    OwnedPayload {
        envs: empty_owned_envs(),
        value,
    }
}

#[allow(dead_code)]
pub(crate) fn run_prepared_call(prepared: PreparedCall) -> Result<DgmValue, DgmError> {
    run_prepared_call_payload(prepared).and_then(restore_payload)
}

pub(crate) fn run_prepared_call_for_response(prepared: PreparedCall) -> Result<DgmValue, DgmError> {
    execute_owned_response_call(prepared.call, prepared.runtime)
}

pub(crate) fn run_prepared_callable(prepared: PreparedCallable, args: Vec<OwnedPayload>) -> Result<DgmValue, DgmError> {
    runtime_reset_all();
    runtime_apply_config(&prepared.runtime)?;
    runtime_reset_usage();
    let _request_scope = begin_request_scope();
    let _env_cleanup = begin_environment_cleanup_scope();
    let mut restore = RestoreContext::new(Arc::clone(&prepared.envs));
    if let OwnedValue::Function { name, params, body, closure, compiled } = prepared.func.as_ref() {
        let closure_env = restore.restore_env(*closure)?;
        let mut payloads = args.into_iter();
        let result = call_user_function_with_positional_resolver(
            name.as_deref(),
            params,
            body,
            closure_env,
            compiled.clone(),
            || match payloads.next() {
                Some(payload) => {
                    let value = restore.restore_payload(payload)?;
                    runtime_reserve_restored_payload_labeled("concurrency.restore.payload", &value)?;
                    Ok(Some(value))
                }
                None => Ok(None),
            },
        );
        restore.teardown_restored_envs();
        return result;
    }
    let func = restore.restore_value(prepared.func.as_ref())?;
    runtime_reserve_value_labeled("concurrency.restore.func", &func)?;
    let args = args
        .into_iter()
        .map(|payload| {
            let value = restore.restore_payload(payload)?;
            runtime_reserve_restored_payload_labeled("concurrency.restore.payload", &value)?;
            Ok(value)
        })
        .collect::<Result<Vec<_>, _>>()?;
    runtime_reserve_labeled(
        "concurrency.restore.args",
        mem::size_of::<Vec<DgmValue>>().saturating_add(args.len().saturating_mul(mem::size_of::<DgmValue>())),
    )?;
    // Prepared callables are used for request handlers and should only return data
    // that survives serialization, not closure environments. Clear restored envs
    // after execution to break env -> function -> env refcount cycles.
    let _teardown = RestoreContextTeardownGuard(&mut restore);
    call_native_hof(&func, args)
}

fn spawn_task(func: DgmValue, args: Vec<DgmValue>) -> Result<Arc<TaskHandleState>, DgmError> {
    let prepared = prepare_isolated_call(func.clone(), args)?;
    let label = callable_label(&func);
    let future = schedule_named_future_payload_state("task", label, prepared.runtime, move || run_prepared_call_payload(prepared))?;
    Ok(Arc::new(TaskHandleState::new(future)))
}

fn run_prepared_call_payload(prepared: PreparedCall) -> Result<OwnedPayload, DgmError> {
    execute_owned_call_to_payload(prepared.call, prepared.runtime)
}

fn execute_owned_call_to_payload(call: OwnedCall, runtime: RuntimeConfig) -> Result<OwnedPayload, DgmError> {
    runtime_reset_all();
    runtime_apply_config(&runtime)?;
    runtime_reset_usage();
    let _env_cleanup = begin_environment_cleanup_scope();
    let mut restore = RestoreContext::new(call.envs);
    if let OwnedValue::Function { name, params, body, closure, compiled } = call.func.as_ref() {
        let closure_env = restore.restore_env(*closure)?;
        let result = match Arc::try_unwrap(call.args) {
            Ok(args) => {
                let mut args = args.into_iter();
                call_user_function_with_positional_resolver(
                    name.as_deref(),
                    params,
                    body,
                    closure_env,
                    compiled.clone(),
                    || match args.next() {
                        Some(arg) => Ok(Some(restore.restore_owned_value(arg)?)),
                        None => Ok(None),
                    },
                )
            }
            Err(args) => {
                let mut index = 0usize;
                call_user_function_with_positional_resolver(
                    name.as_deref(),
                    params,
                    body,
                    closure_env,
                    compiled.clone(),
                    || {
                        if let Some(arg) = args.get(index) {
                            index += 1;
                            Ok(Some(restore.restore_value(arg)?))
                        } else {
                            Ok(None)
                        }
                    },
                )
            }
        };
        restore.teardown_restored_envs();
        let value = result?;
        let mut snapshot = SnapshotContext::default();
        let payload = snapshot.snapshot_payload(&value)?;
        runtime_reserve_restored_payload_labeled("concurrency.restore.payload", &value)?;
        return Ok(payload);
    }
    let func = match Arc::try_unwrap(call.func) {
        Ok(func) => restore.restore_owned_value(func)?,
        Err(func) => restore.restore_value(func.as_ref())?,
    };
    runtime_reserve_value_labeled("concurrency.restore.func", &func)?;
    let args = match Arc::try_unwrap(call.args) {
        Ok(args) => args
            .into_iter()
            .map(|arg| restore.restore_owned_value(arg))
            .collect::<Result<Vec<_>, _>>()?,
        Err(args) => args
            .iter()
            .map(|arg| restore.restore_value(arg))
            .collect::<Result<Vec<_>, _>>()?,
    };
    runtime_reserve_labeled(
        "concurrency.restore.args",
        mem::size_of::<Vec<DgmValue>>().saturating_add(args.len().saturating_mul(mem::size_of::<DgmValue>())),
    )?;
    let _teardown = RestoreContextTeardownGuard(&mut restore);
    let value = call_native_hof(&func, args)?;
    let mut snapshot = SnapshotContext::default();
    let payload = snapshot.snapshot_payload(&value)?;
    runtime_reserve_restored_payload_labeled("concurrency.restore.payload", &value)?;
    Ok(payload)
}

fn execute_owned_response_call(call: OwnedCall, runtime: RuntimeConfig) -> Result<DgmValue, DgmError> {
    runtime_reset_all();
    runtime_apply_config(&runtime)?;
    runtime_reset_usage();
    let _request_scope = begin_request_scope();
    let _env_cleanup = begin_environment_cleanup_scope();
    let mut restore = RestoreContext::new(call.envs);
    if let OwnedValue::Function { name, params, body, closure, compiled } = call.func.as_ref() {
        let closure_env = restore.restore_env(*closure)?;
        let result = match Arc::try_unwrap(call.args) {
            Ok(args) => {
                let mut args = args.into_iter();
                call_user_function_with_positional_resolver(
                    name.as_deref(),
                    params,
                    body,
                    closure_env,
                    compiled.clone(),
                    || match args.next() {
                        Some(arg) => Ok(Some(restore.restore_owned_value(arg)?)),
                        None => Ok(None),
                    },
                )
            }
            Err(args) => {
                let mut index = 0usize;
                call_user_function_with_positional_resolver(
                    name.as_deref(),
                    params,
                    body,
                    closure_env,
                    compiled.clone(),
                    || {
                        if let Some(arg) = args.get(index) {
                            index += 1;
                            Ok(Some(restore.restore_value(arg)?))
                        } else {
                            Ok(None)
                        }
                    },
                )
            }
        };
        restore.teardown_restored_envs();
        return result;
    }
    let func = match Arc::try_unwrap(call.func) {
        Ok(func) => restore.restore_owned_value(func)?,
        Err(func) => restore.restore_value(func.as_ref())?,
    };
    runtime_reserve_value_labeled("concurrency.restore.func", &func)?;
    let args = match Arc::try_unwrap(call.args) {
        Ok(args) => args
            .into_iter()
            .map(|arg| restore.restore_owned_value(arg))
            .collect::<Result<Vec<_>, _>>()?,
        Err(args) => args
            .iter()
            .map(|arg| restore.restore_value(arg))
            .collect::<Result<Vec<_>, _>>()?,
    };
    runtime_reserve_labeled(
        "concurrency.restore.args",
        mem::size_of::<Vec<DgmValue>>().saturating_add(args.len().saturating_mul(mem::size_of::<DgmValue>())),
    )?;
    let _teardown = RestoreContextTeardownGuard(&mut restore);
    call_native_hof(&func, args)
}

fn restore_payload(payload: OwnedPayload) -> Result<DgmValue, DgmError> {
    let mut restore = RestoreContext::new(payload.envs);
    let value = restore.restore_owned_value(payload.value)?;
    runtime_reserve_restored_payload_labeled("concurrency.restore.payload", &value)?;
    Ok(value)
}

fn snapshot_payload(value: &DgmValue) -> Result<OwnedPayload, DgmError> {
    let mut snapshot = SnapshotContext::default();
    snapshot.snapshot_payload(value)
}

fn is_callable(value: &DgmValue) -> bool {
    matches!(value, DgmValue::Function { .. } | DgmValue::NativeFunction { .. } | DgmValue::NativeMethod { .. })
}

fn callable_label(value: &DgmValue) -> String {
    match value {
        DgmValue::Function { name: Some(name), .. } => name.clone(),
        DgmValue::Function { .. } => "<lambda>".into(),
        DgmValue::NativeFunction { name, .. } => name.clone(),
        DgmValue::NativeMethod { name, .. } => name.clone(),
        DgmValue::Class { name, .. } => format!("new {}", name),
        _ => "<call>".into(),
    }
}

pub(crate) fn schedule_named_future<F>(kind: &'static str, label: impl Into<String>, job: F) -> Result<DgmValue, DgmError>
where
    F: FnOnce() -> Result<DgmValue, DgmError> + Send + 'static,
{
    let future = schedule_named_future_state(kind, label.into(), runtime_config(), job)?;
    Ok(DgmValue::Future(future))
}

#[allow(dead_code)]
pub(crate) fn schedule_future<F>(job: F) -> Result<DgmValue, DgmError>
where
    F: FnOnce() -> Result<DgmValue, DgmError> + Send + 'static,
{
    schedule_named_future("future", "future", job)
}

pub(crate) fn schedule_named_future_state<F>(
    kind: &'static str,
    label: String,
    runtime: RuntimeConfig,
    job: F,
) -> Result<Arc<FutureState>, DgmError>
where
    F: FnOnce() -> Result<DgmValue, DgmError> + Send + 'static,
{
    current_async_runtime().submit(kind, label, runtime, job)
}

pub(crate) fn schedule_named_future_payload_state<F>(
    kind: &'static str,
    label: String,
    runtime: RuntimeConfig,
    job: F,
) -> Result<Arc<FutureState>, DgmError>
where
    F: FnOnce() -> Result<OwnedPayload, DgmError> + Send + 'static,
{
    current_async_runtime().submit_payload(kind, label, runtime, job)
}

pub(crate) fn runtime_async_snapshot() -> AsyncRuntimeSnapshot {
    current_async_runtime().snapshots().0
}

pub(crate) fn runtime_task_snapshots() -> Vec<AsyncTaskSnapshot> {
    current_async_runtime().snapshots().1
}

pub(crate) fn runtime_async_stats_value() -> DgmValue {
    let snapshot = runtime_async_snapshot();
    let mut map = HashMap::new();
    map.insert("max_threads".into(), DgmValue::Int(clamp_usize(snapshot.max_threads)));
    map.insert("pending_jobs".into(), DgmValue::Int(clamp_usize(snapshot.pending_jobs)));
    map.insert("running_jobs".into(), DgmValue::Int(clamp_usize(snapshot.running_jobs)));
    map.insert("submitted_jobs".into(), DgmValue::Int(clamp_u64(snapshot.submitted_jobs)));
    map.insert("completed_jobs".into(), DgmValue::Int(clamp_u64(snapshot.completed_jobs)));
    map.insert("cancelled_jobs".into(), DgmValue::Int(clamp_u64(snapshot.cancelled_jobs)));
    map.insert("retained_tasks".into(), DgmValue::Int(clamp_usize(snapshot.retained_tasks)));
    map.insert("oldest_pending_ms".into(), DgmValue::Int(clamp_u128(snapshot.oldest_pending_ms)));
    map.insert("possible_deadlock".into(), DgmValue::Bool(snapshot.possible_deadlock));
    DgmValue::Map(Rc::new(RefCell::new(map)))
}

pub(crate) fn runtime_tasks_value() -> DgmValue {
    let values = runtime_task_snapshots()
        .into_iter()
        .map(task_snapshot_value)
        .collect::<Vec<_>>();
    DgmValue::List(Rc::new(RefCell::new(values)))
}

pub(crate) fn runtime_snapshot_stats_value() -> DgmValue {
    let snapshot = lock_snapshot_metrics().clone();
    let mut map = HashMap::new();
    map.insert("prepared_call_count".into(), DgmValue::Int(clamp_u64(snapshot.prepared_call_count)));
    map.insert(
        "prepared_callable_count".into(),
        DgmValue::Int(clamp_u64(snapshot.prepared_callable_count)),
    );
    map.insert(
        "payload_snapshot_count".into(),
        DgmValue::Int(clamp_u64(snapshot.payload_snapshot_count)),
    );
    map.insert("env_snapshot_count".into(), DgmValue::Int(clamp_u64(snapshot.env_snapshot_count)));
    map.insert(
        "env_snapshot_bytes_estimate".into(),
        DgmValue::Int(clamp_u64(snapshot.env_snapshot_bytes_estimate)),
    );
    map.insert("value_snapshot_count".into(), DgmValue::Int(clamp_u64(snapshot.value_snapshot_count)));
    map.insert(
        "value_snapshot_bytes_estimate".into(),
        DgmValue::Int(clamp_u64(snapshot.value_snapshot_bytes_estimate)),
    );
    DgmValue::Map(Rc::new(RefCell::new(map)))
}

pub(crate) fn runtime_reset_snapshot_stats() {
    *lock_snapshot_metrics() = SnapshotMetricsState::default();
}

fn await_future_payload(state: &Arc<FutureState>) -> Result<OwnedPayload, DgmError> {
    let parent = CURRENT_FUTURE.with(|slot| slot.borrow().as_ref().and_then(Weak::upgrade));
    if let Some(parent) = parent.as_ref() {
        if Arc::ptr_eq(parent, state) {
            return Err(DgmError::RuntimeError { msg: "task cannot await itself".into() });
        }
    }
    let mut inner = wait_for_future_completion(state, parent.as_ref())?;
    if !inner.profile_merged {
        if let Some(profile) = &inner.profile {
            runtime_merge_profile(profile);
        }
        inner.profile_merged = true;
        if let Ok(mut locked) = state.inner.lock() {
            locked.profile_merged = true;
        }
    }
    match inner.status {
        AsyncStatus::Ready | AsyncStatus::Failed => inner
            .result
            .ok_or_else(|| DgmError::RuntimeError { msg: "async result missing".into() })?,
        AsyncStatus::Cancelled => Err(state.cancelled_error()),
        AsyncStatus::Pending | AsyncStatus::Running => Err(DgmError::RuntimeError { msg: "async result unavailable".into() }),
    }
}

fn wait_for_future_completion(
    state: &Arc<FutureState>,
    parent: Option<&Arc<FutureState>>,
) -> Result<FutureInner, DgmError> {
    let mut inner = state.inner.lock().unwrap();
    while matches!(inner.status, AsyncStatus::Pending | AsyncStatus::Running) {
        runtime_check_limits()?;
        if let Some(parent) = parent {
            if parent.cancel_requested() {
                inner.cancel_requested = true;
                if inner.status == AsyncStatus::Pending {
                    inner.status = AsyncStatus::Cancelled;
                    inner.completed_at = Some(Instant::now());
                    inner.error = Some(format!("{} cancelled", state.kind));
                    state.ready.notify_all();
                } else {
                    inner.error = Some(format!("{} cancelled", state.kind));
                }
                return Err(parent.cancelled_error());
            }
        }
        let (next, _) = state.ready.wait_timeout(inner, Duration::from_millis(20)).unwrap();
        inner = next;
    }
    Ok(inner.clone())
}

pub(crate) fn await_future(state: &Arc<FutureState>) -> Result<DgmValue, DgmError> {
    await_future_payload(state).and_then(restore_payload)
}

#[allow(dead_code)]
pub(crate) fn poll_future(state: &Arc<FutureState>) -> Result<Option<DgmValue>, DgmError> {
    let inner = state.poll_inner();
    match inner.status {
        AsyncStatus::Pending | AsyncStatus::Running => Ok(None),
        AsyncStatus::Ready | AsyncStatus::Failed => inner
            .result
            .ok_or_else(|| DgmError::RuntimeError { msg: "async result missing".into() })?
            .and_then(restore_payload)
            .map(Some),
        AsyncStatus::Cancelled => Err(state.cancelled_error()),
    }
}

pub(crate) fn await_task_handle(handle: &Arc<TaskHandleState>) -> Result<DgmValue, DgmError> {
    match handle.join_payload() {
        Ok(payload) => restore_payload(payload),
        Err(err) => Err(task_join_error(err)),
    }
}

fn channel_value(state: Arc<ChannelState>) -> Result<DgmValue, DgmError> {
    let mut map = HashMap::new();
    let state_value = DgmValue::Channel(Arc::clone(&state));
    map.insert("send".into(), DgmValue::NativeMethod { name: "channel.send".into(), state: Box::new(state_value.clone()), func: channel_send });
    map.insert("recv".into(), DgmValue::NativeMethod { name: "channel.recv".into(), state: Box::new(state_value.clone()), func: channel_recv });
    map.insert("try_recv".into(), DgmValue::NativeMethod { name: "channel.try_recv".into(), state: Box::new(state_value.clone()), func: channel_try_recv });
    map.insert("recv_timeout".into(), DgmValue::NativeMethod { name: "channel.recv_timeout".into(), state: Box::new(state_value), func: channel_recv_timeout });
    let value = DgmValue::Map(Rc::new(RefCell::new(map)));
    runtime_reserve_value(&value)?;
    Ok(value)
}

fn task_handle_value(state: Arc<TaskHandleState>) -> Result<DgmValue, DgmError> {
    let mut map = HashMap::new();
    let hidden = DgmValue::TaskHandle(Arc::clone(&state));
    map.insert("join".into(), DgmValue::NativeMethod { name: "task.join".into(), state: Box::new(hidden.clone()), func: task_join });
    map.insert("cancel".into(), DgmValue::NativeMethod { name: "task.cancel".into(), state: Box::new(hidden.clone()), func: task_cancel });
    map.insert("done".into(), DgmValue::NativeMethod { name: "task.done".into(), state: Box::new(hidden.clone()), func: task_done });
    map.insert("state".into(), DgmValue::NativeMethod { name: "task.state".into(), state: Box::new(hidden.clone()), func: task_state });
    map.insert("__await__".into(), hidden);
    let value = DgmValue::Map(Rc::new(RefCell::new(map)));
    runtime_reserve_value(&value)?;
    Ok(value)
}

fn channel_send(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let DgmValue::Channel(channel) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid channel state".into() });
    };
    let value = args.first().cloned().ok_or_else(|| DgmError::RuntimeError { msg: "channel.send(value) requires 1 arg".into() })?;
    let mut snapshot = SnapshotContext::default();
    let payload = snapshot.snapshot_payload(&value)?;
    channel.sender.lock().unwrap().send(payload)
        .map_err(|_| DgmError::RuntimeError { msg: "channel closed".into() })?;
    Ok(DgmValue::Null)
}

fn channel_recv(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "channel.recv() does not take arguments".into() });
    }
    let DgmValue::Channel(channel) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid channel state".into() });
    };
    let payload = channel.receiver.lock().unwrap().recv()
        .map_err(|_| DgmError::RuntimeError { msg: "channel closed".into() })?;
    restore_payload(payload)
}

fn channel_try_recv(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "channel.try_recv() does not take arguments".into() });
    }
    let DgmValue::Channel(channel) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid channel state".into() });
    };
    let result = match channel.receiver.lock().unwrap().try_recv() {
        Ok(payload) => restore_payload(payload),
        Err(mpsc::TryRecvError::Empty) => Ok(DgmValue::Null),
        Err(mpsc::TryRecvError::Disconnected) => Err(DgmError::RuntimeError { msg: "channel closed".into() }),
    };
    result
}

fn channel_recv_timeout(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let timeout = match args.first() {
        Some(DgmValue::Int(ms)) if *ms >= 0 => Duration::from_millis(*ms as u64),
        Some(DgmValue::Int(_)) => return Err(DgmError::RuntimeError { msg: "channel.recv_timeout(ms) requires ms >= 0".into() }),
        _ => return Err(DgmError::RuntimeError { msg: "channel.recv_timeout(ms) requires int".into() }),
    };
    let DgmValue::Channel(channel) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid channel state".into() });
    };
    let result = match channel.receiver.lock().unwrap().recv_timeout(timeout) {
        Ok(payload) => restore_payload(payload),
        Err(mpsc::RecvTimeoutError::Timeout) => Ok(DgmValue::Null),
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(DgmError::RuntimeError { msg: "channel closed".into() }),
    };
    result
}

fn task_join(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "task.join() does not take arguments".into() });
    }
    let DgmValue::TaskHandle(handle) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid task handle state".into() });
    };
    match handle.join_payload() {
        Ok(payload) => restore_payload(payload),
        Err(err) => Err(task_join_error(err)),
    }
}

fn task_cancel(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "task.cancel() does not take arguments".into() });
    }
    let DgmValue::TaskHandle(handle) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid task handle state".into() });
    };
    Ok(DgmValue::Bool(handle.cancel()))
}

fn task_done(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "task.done() does not take arguments".into() });
    }
    let DgmValue::TaskHandle(handle) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid task handle state".into() });
    };
    Ok(DgmValue::Bool(handle.is_done()))
}

fn task_state(state: DgmValue, args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if !args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "task.state() does not take arguments".into() });
    }
    let DgmValue::TaskHandle(handle) = state else {
        return Err(DgmError::RuntimeError { msg: "invalid task handle state".into() });
    };
    Ok(task_snapshot_value(handle.snapshot()))
}

fn task_snapshot_value(snapshot: AsyncTaskSnapshot) -> DgmValue {
    let mut map = HashMap::new();
    map.insert("id".into(), DgmValue::Int(clamp_u64(snapshot.id)));
    map.insert("kind".into(), DgmValue::Str(snapshot.kind));
    map.insert("label".into(), DgmValue::Str(snapshot.label));
    map.insert("status".into(), DgmValue::Str(snapshot.status));
    map.insert("cancel_requested".into(), DgmValue::Bool(snapshot.cancel_requested));
    map.insert("total_time_ms".into(), DgmValue::Float(snapshot.total_time_ms));
    map.insert("queued_ms".into(), DgmValue::Float(snapshot.queued_ms));
    map.insert("run_ms".into(), DgmValue::Float(snapshot.run_ms));
    map.insert("worker".into(), snapshot.worker.map(DgmValue::Str).unwrap_or(DgmValue::Null));
    map.insert("error".into(), snapshot.error.map(DgmValue::Str).unwrap_or(DgmValue::Null));
    map.insert(
        "profile".into(),
        snapshot.profile.map(runtime_profile_to_value).unwrap_or(DgmValue::Null),
    );
    map.insert(
        "usage".into(),
        snapshot.usage.map(runtime_usage_to_value).unwrap_or(DgmValue::Null),
    );
    DgmValue::Map(Rc::new(RefCell::new(map)))
}

fn runtime_profile_to_value(profile: RuntimeProfile) -> DgmValue {
    let mut map = HashMap::new();
    map.insert("total_time".into(), DgmValue::Int(clamp_u128(profile.total_time)));
    map.insert("total_time_ms".into(), DgmValue::Float(profile.total_time as f64 / 1_000_000.0));
    map.insert("function_time".into(), DgmValue::Map(Rc::new(RefCell::new(time_map_to_values(&profile.function_time)))));
    map.insert("function_calls".into(), DgmValue::Map(Rc::new(RefCell::new(count_map_to_values(&profile.function_calls)))));
    map.insert("module_time".into(), DgmValue::Map(Rc::new(RefCell::new(time_map_to_values(&profile.module_time)))));
    map.insert("alloc_count".into(), DgmValue::Int(clamp_u64(profile.alloc_count)));
    map.insert("alloc_bytes".into(), DgmValue::Int(clamp_u64(profile.alloc_bytes)));
    map.insert("peak_heap_bytes".into(), DgmValue::Int(clamp_u64(profile.peak_heap_bytes)));
    map.insert("io_call_count".into(), DgmValue::Int(clamp_u64(profile.io_call_count)));
    map.insert("io_time".into(), DgmValue::Int(clamp_u128(profile.io_time_ns)));
    map.insert("io_time_ms".into(), DgmValue::Float(profile.io_time_ns as f64 / 1_000_000.0));
    DgmValue::Map(Rc::new(RefCell::new(map)))
}

fn runtime_usage_to_value(usage: RuntimeUsageSnapshot) -> DgmValue {
    let mut map = HashMap::new();
    map.insert("max_steps".into(), DgmValue::Int(clamp_u64(usage.max_steps)));
    map.insert("steps".into(), DgmValue::Int(clamp_u64(usage.steps)));
    map.insert("max_call_depth".into(), DgmValue::Int(clamp_usize(usage.max_call_depth)));
    map.insert("call_depth".into(), DgmValue::Int(clamp_usize(usage.call_depth)));
    map.insert("max_heap_bytes".into(), DgmValue::Int(clamp_usize(usage.max_heap_bytes)));
    map.insert("used_heap_bytes".into(), DgmValue::Int(clamp_usize(usage.used_heap_bytes)));
    map.insert("peak_heap_bytes".into(), DgmValue::Int(clamp_usize(usage.peak_heap_bytes)));
    map.insert("max_threads".into(), DgmValue::Int(clamp_usize(usage.max_threads)));
    map.insert("max_wall_time_ms".into(), DgmValue::Int(clamp_u64(usage.max_wall_time_ms)));
    map.insert("elapsed_wall_time_ms".into(), DgmValue::Int(clamp_u64(usage.elapsed_wall_time_ms)));
    map.insert("max_open_sockets".into(), DgmValue::Int(clamp_usize(usage.max_open_sockets)));
    map.insert("open_sockets".into(), DgmValue::Int(clamp_usize(usage.open_sockets)));
    map.insert("max_concurrent_requests".into(), DgmValue::Int(clamp_usize(usage.max_concurrent_requests)));
    map.insert("concurrent_requests".into(), DgmValue::Int(clamp_usize(usage.concurrent_requests)));
    map.insert("peak_concurrent_requests".into(), DgmValue::Int(clamp_usize(usage.peak_concurrent_requests)));
    DgmValue::Map(Rc::new(RefCell::new(map)))
}

fn time_map_to_values(source: &HashMap<String, u128>) -> HashMap<String, DgmValue> {
    source
        .iter()
        .map(|(key, value)| (key.clone(), DgmValue::Int(clamp_u128(*value))))
        .collect()
}

fn count_map_to_values(source: &HashMap<String, u64>) -> HashMap<String, DgmValue> {
    source
        .iter()
        .map(|(key, value)| (key.clone(), DgmValue::Int(clamp_u64(*value))))
        .collect()
}

fn clamp_u128(value: u128) -> i64 {
    value.min(i64::MAX as u128) as i64
}

fn clamp_u64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn clamp_usize(value: usize) -> i64 {
    value.min(i64::MAX as usize) as i64
}

fn task_join_error(err: DgmError) -> DgmError {
    let normalized = normalize_task_message(&err.message_string());
    let message = match err.kind_name() {
        "RuntimeError" | "ThrownError" => format!("task failed: {}", normalized),
        kind => format!("task failed: {}: {}", kind, normalized),
    };
    DgmError::Structured {
        kind: "RuntimeError",
        message,
        thrown_value: err.thrown_value(),
        stack: err.stack().to_vec(),
    }
}

fn normalize_task_message(message: &str) -> String {
    if message.len() >= 2 && message.starts_with('"') && message.ends_with('"') {
        message[1..message.len() - 1].to_string()
    } else {
        message.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Expr, Param, Stmt, StmtKind};
    use crate::environment::{environment_live_snapshot, reset_environment_pool, Environment};
    use crate::error::register_source;
    use crate::request_scope::{request_scope_snapshot, reset_request_scope_stats};
    use crate::{parse_source, run_source, run_source_with_path};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Barrier;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn usage_snapshot() -> RuntimeUsageSnapshot {
        RuntimeUsageSnapshot {
            max_steps: 10_000_000,
            steps: 0,
            max_call_depth: 1_000,
            call_depth: 0,
            max_heap_bytes: 64 * 1024 * 1024,
            used_heap_bytes: 0,
            peak_heap_bytes: 0,
            max_threads: 4,
            max_wall_time_ms: 30_000,
            elapsed_wall_time_ms: 0,
            max_open_sockets: 64,
            open_sockets: 0,
            max_concurrent_requests: 16,
            concurrent_requests: 0,
            peak_concurrent_requests: 0,
        }
    }

    fn test_runtime_config() -> RuntimeConfig {
        RuntimeConfig {
            max_steps: 10_000_000,
            max_call_depth: 1_000,
            max_heap_bytes: 64 * 1024 * 1024,
            max_threads: 8,
            max_wall_time_ms: 30_000,
            max_open_sockets: 64,
            max_concurrent_requests: 16,
        }
    }

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        match LOCK.get_or_init(|| Mutex::new(())).lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn temp_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let dir = std::env::temp_dir().join(format!("dgm-{name}-{stamp}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn prepared_callable_preserves_compiled_function_blob() {
        let _guard = test_lock();
        let body = vec![Stmt::new(Default::default(), StmtKind::Return(Some(Expr::IntLit(7))))];
        let params: Vec<Param> = vec![];
        let func = DgmValue::Function {
            name: Some("answer".into()),
            params: params.clone(),
            body: body.clone(),
            closure: Rc::new(RefCell::new(Environment::new())),
            compiled: Some(crate::vm::compile_function_blob(Some("answer".into()), &params, &body)),
        };
        let prepared = prepare_isolated_callable(func).unwrap();
        match prepared.func.as_ref() {
            OwnedValue::Function { compiled, .. } => assert!(compiled.is_some()),
            other => panic!("unexpected prepared func: {:?}", other),
        }
        let first = run_prepared_callable(prepared.clone(), vec![]).unwrap();
        let second = run_prepared_callable(prepared, vec![]).unwrap();
        assert!(matches!(first, DgmValue::Int(7)));
        assert!(matches!(second, DgmValue::Int(7)));
    }

    #[test]
    fn prepared_callable_does_not_accumulate_restored_env_cycles() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let closure = Rc::new(RefCell::new(Environment::new()));
        let body = vec![Stmt::new(Default::default(), StmtKind::Return(Some(Expr::IntLit(7))))];
        let params: Vec<Param> = vec![];
        let func = DgmValue::Function {
            name: Some("answer".into()),
            params: params.clone(),
            body: body.clone(),
            closure: Rc::clone(&closure),
            compiled: Some(crate::vm::compile_function_blob(Some("answer".into()), &params, &body)),
        };
        closure.borrow_mut().set("answer", func.clone());

        let prepared = prepare_isolated_callable(func).unwrap();

        for _ in 0..256 {
            let value = run_prepared_callable(prepared.clone(), vec![]).unwrap();
            assert!(matches!(value, DgmValue::Int(7)));
        }

        let scope = request_scope_snapshot();
        assert_eq!(scope.scopes_with_assertable_survivors, 0);
        assert_eq!(scope.total_survivors.environments, 0);
        assert_eq!(scope.total_survivors.requests, 0);
        assert_eq!(scope.total_survivors.request_maps, 0);
        assert_eq!(scope.total_survivors.request_json, 0);
    }

    #[test]
    fn prepared_response_call_does_not_accumulate_restored_env_cycles() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let closure = Rc::new(RefCell::new(Environment::new()));
        let body = vec![Stmt::new(Default::default(), StmtKind::Return(Some(Expr::IntLit(7))))];
        let params: Vec<Param> = vec![];
        let func = DgmValue::Function {
            name: Some("answer".into()),
            params: params.clone(),
            body: body.clone(),
            closure: Rc::clone(&closure),
            compiled: Some(crate::vm::compile_function_blob(Some("answer".into()), &params, &body)),
        };
        closure.borrow_mut().set("answer", func.clone());

        let prepared = prepare_isolated_call(func, vec![]).unwrap();

        for _ in 0..256 {
            let value = run_prepared_call_for_response(PreparedCall {
                runtime: prepared.runtime,
                call: prepared.call.clone(),
            })
            .unwrap();
            assert!(matches!(value, DgmValue::Int(7)));
        }

        let scope = request_scope_snapshot();
        assert_eq!(scope.scopes_with_assertable_survivors, 0);
        assert_eq!(scope.total_survivors.environments, 0);
        assert_eq!(scope.total_survivors.requests, 0);
        assert_eq!(scope.total_survivors.request_maps, 0);
        assert_eq!(scope.total_survivors.request_json, 0);
    }

    #[test]
    fn spawn_join_named_closure_does_not_accumulate_environments() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let source = r#"
imprt runtime
runtime.set_max_threads(4)

let baseline = runtime.stats().environments.live
let x = 7

def work() {
    retrun x
}

let i = 0
whl i < 1000 {
    let task = spawn(work)
    assert_eq(task.join(), 7)
    i = i + 1
}

let stats = runtime.stats()
assert_eq(stats.request_scope.scopes_with_survivors, 0)
assert_eq(stats.request_scope.scopes_with_assertable_survivors, 0)
assert_eq(stats.environments.live, baseline)
"#;

        run_source(source).unwrap();
    }

    #[test]
    fn closure_escape_repeated_calls_do_not_accumulate_environments() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let source = r#"
imprt runtime

let baseline = runtime.stats().environments.live

def make(n) {
    let x = n
    retrun fn() {
        retrun x
    }
}

def apply(n) {
    let f = make(n)
    retrun f()
}

let i = 0
let total = 0
whl i < 2000 {
    total = total + apply(i)
    i = i + 1
}

assert_eq(total, 1999000)
assert_eq(runtime.stats().environments.live, baseline)
"#;

        run_source(source).unwrap();
    }

    #[test]
    fn deep_await_chain_does_not_accumulate_environments() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let source = r#"
imprt runtime
runtime.set_max_threads(4)

let baseline = runtime.stats().environments.live

let task = spawn(fn() {
    retrun 0
})

let i = 0
whl i < 500 {
    let prev = task
    task = spawn(fn() {
        retrun awt prev + 1
    })
    i = i + 1
}

assert_eq(awt task, 500)
assert_eq(runtime.stats().environments.live, baseline)
"#;

        run_source(source).unwrap();
    }

    #[test]
    fn cross_module_closure_does_not_accumulate_envs() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let dir = temp_dir("cross-module-env");
        let mod_a = dir.join("mod_a.dgm");
        let main = dir.join("main.dgm");
        let module_source = r#"
def make_closure(n) {
    let x = n
    retrun fn() {
        retrun x
    }
}
"#;
        let main_source = r#"
imprt runtime
imprt "./mod_a.dgm"

let baseline = runtime.stats().environments.live

def call(n) {
    let f = mod_a.make_closure(n)
    retrun f()
}

let i = 0
let total = 0
whl i < 500 {
    total = total + call(i)
    i = i + 1
}

assert_eq(total, 124750)

let stats = runtime.stats()
assert_eq(stats.request_scope.scopes_with_survivors, 0)
assert_eq(stats.request_scope.scopes_with_assertable_survivors, 0)
assert_eq(stats.environments.live, baseline)
"#;
        fs::write(&mod_a, module_source).unwrap();
        fs::write(&main, main_source).unwrap();

        let baseline = environment_live_snapshot().live;
        for _ in 0..256 {
            run_source_with_path(main_source, Some(main.clone())).unwrap();
        }

        let after = environment_live_snapshot();
        let scope = request_scope_snapshot();
        assert_eq!(after.live, baseline);
        assert_eq!(scope.scopes_with_survivors, 0);
        assert_eq!(scope.scopes_with_assertable_survivors, 0);
        assert_eq!(scope.total_survivors.environments, 0);
    }

    #[test]
    #[ignore = "stress: 100k spawn/join runs beyond default debug-test wall-clock"]
    fn spawn_join_100k_no_env_leak() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let source = r#"
imprt runtime
runtime.set_max_threads(4)
runtime.set_max_wall_time_ms(600000)

let baseline = runtime.stats().environments.live
let x = 1

def work() {
    let f = fn() {
        retrun x
    }
    retrun f()
}

let i = 0
let total = 0
whl i < 100000 {
    let task = spawn(work)
    total = total + task.join()
    i = i + 1
}

assert_eq(total, 100000)

let stats = runtime.stats()
assert_eq(stats.request_scope.scopes_with_survivors, 0)
assert_eq(stats.request_scope.scopes_with_assertable_survivors, 0)
assert_eq(stats.environments.live, baseline)
"#;

        run_source(source).unwrap();
    }

    #[test]
    fn mixed_execution_paths_do_not_accumulate_environments() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let dir = temp_dir("mixed-execution");
        let mod_a = dir.join("mod_a.dgm");
        let main = dir.join("main.dgm");
        let module_source = r#"
def make_closure(n) {
    let x = n
    retrun fn() {
        retrun x
    }
}
"#;
        let main_source = r#"
imprt runtime
imprt "./mod_a.dgm"
runtime.set_max_threads(4)

let baseline = runtime.stats().environments.live

def do_spawn(n) {
    let f = mod_a.make_closure(n)
    let task = spawn(fn() {
        retrun f()
    })
    retrun task.join()
}

def do_chain(seed) {
    let task = spawn(fn() {
        retrun seed
    })
    let depth = 0
    whl depth < 8 {
        let prev = task
        task = spawn(fn() {
            retrun awt prev + 1
        })
        depth = depth + 1
    }
    retrun awt task
}

let i = 0
let total = 0
whl i < 200 {
    total = total + do_spawn(i)
    total = total + do_chain(i)
    i = i + 1
}

assert(total > 0)

let stats = runtime.stats()
assert_eq(stats.request_scope.scopes_with_survivors, 0)
assert_eq(stats.request_scope.scopes_with_assertable_survivors, 0)
assert_eq(stats.environments.live, baseline)
"#;
        fs::write(&mod_a, module_source).unwrap();
        fs::write(&main, main_source).unwrap();

        let closure = Rc::new(RefCell::new(Environment::new()));
        let body = vec![Stmt::new(Default::default(), StmtKind::Return(Some(Expr::IntLit(7))))];
        let params: Vec<Param> = vec![];
        let func = DgmValue::Function {
            name: Some("answer".into()),
            params: params.clone(),
            body: body.clone(),
            closure: Rc::clone(&closure),
            compiled: Some(crate::vm::compile_function_blob(Some("answer".into()), &params, &body)),
        };
        closure.borrow_mut().set("answer", func.clone());
        let prepared_callable = prepare_isolated_callable(func.clone()).unwrap();
        let prepared_call = prepare_isolated_call(func, vec![]).unwrap();

        let baseline = environment_live_snapshot().live;
        for _ in 0..8 {
            let value = run_prepared_callable(prepared_callable.clone(), vec![]).unwrap();
            assert!(matches!(value, DgmValue::Int(7)));

            let response = run_prepared_call_for_response(PreparedCall {
                runtime: prepared_call.runtime,
                call: prepared_call.call.clone(),
            })
            .unwrap();
            assert!(matches!(response, DgmValue::Int(7)));

            run_source_with_path(main_source, Some(main.clone())).unwrap();
        }

        let scope = request_scope_snapshot();
        assert_eq!(environment_live_snapshot().live, baseline);
        assert_eq!(scope.scopes_with_survivors, 0);
        assert_eq!(scope.scopes_with_assertable_survivors, 0);
        assert_eq!(scope.total_survivors.environments, 0);
        assert_eq!(scope.total_survivors.requests, 0);
        assert_eq!(scope.total_survivors.request_maps, 0);
        assert_eq!(scope.total_survivors.request_json, 0);

        drop(prepared_call);
        drop(prepared_callable);
        closure.borrow_mut().clear_values();
        drop(closure);

        assert_eq!(environment_live_snapshot().live, 0);
    }

    #[test]
    fn long_lived_runtime_no_env_growth() {
        let _guard = test_lock();
        reset_environment_pool();
        reset_request_scope_stats();

        let dir = temp_dir("long-lived-runtime");
        let mod_a = dir.join("mod_a.dgm");
        let main = dir.join("main.dgm");
        let module_source = r#"
def make_closure(n) {
    let x = n
    retrun fn() {
        retrun x
    }
}
"#;
        let warmup_source = r#"
imprt "./mod_a.dgm"
let f = nul
"#;
        let loop_source = r#"
imprt "./mod_a.dgm"
f = mod_a.make_closure(42)
assert_eq(f(), 42)
f = nul
"#;
        fs::write(&mod_a, module_source).unwrap();
        fs::write(&main, loop_source).unwrap();

        register_source(main.to_string_lossy().to_string(), warmup_source.to_string());
        let warmup_stmts = parse_source(warmup_source).unwrap();
        register_source(main.to_string_lossy().to_string(), loop_source.to_string());
        let loop_stmts = parse_source(loop_source).unwrap();

        let mut interp = crate::interpreter::Interpreter::new();
        interp.set_entry_path(Some(main.clone()));
        interp.run(warmup_stmts).unwrap();

        let baseline = environment_live_snapshot().live;
        for _ in 0..50_000 {
            interp.run(loop_stmts.clone()).unwrap();
        }

        let after = environment_live_snapshot().live;
        let scope = request_scope_snapshot();
        assert_eq!(after, baseline);
        assert_eq!(scope.scopes_with_survivors, 0);
        assert_eq!(scope.scopes_with_assertable_survivors, 0);
        assert_eq!(scope.total_survivors.environments, 0);

        drop(interp);
        assert_eq!(environment_live_snapshot().live, 0);
    }

    #[test]
    fn running_parent_cancel_propagates_to_awaited_child() {
        let _guard = test_lock();
        let parent = Arc::new(FutureState::new(1, "task", "parent".into()));
        let child = Arc::new(FutureState::new(2, "http", "child".into()));
        assert!(parent.mark_running("worker-1".into()));

        let cancel_parent = Arc::clone(&parent);
        let cancel_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            assert!(cancel_parent.request_cancel());
        });

        let complete_child = Arc::clone(&child);
        let complete_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(60));
                complete_child.complete(
                    Ok(OwnedPayload {
                        envs: Arc::new(HashMap::new()),
                        value: OwnedValue::Null,
                    }),
                RuntimeProfile::default(),
                usage_snapshot(),
                "worker-2".into(),
            );
        });

        let err = wait_for_future_completion(&child, Some(&parent)).unwrap_err();
        assert_eq!(err.message_string(), "task cancelled");

        cancel_thread.join().unwrap();
        complete_thread.join().unwrap();
        assert_eq!(child.status(), AsyncStatus::Cancelled);
    }

    #[test]
    fn stress_ten_thousand_tasks_complete_or_cancel_without_leak() {
        let _guard = test_lock();
        configure_async_runtime(8);
        let baseline = runtime_async_snapshot();
        let runtime = test_runtime_config();
        let mut futures = Vec::with_capacity(10_000);
        for index in 0..10_000u64 {
            let future = schedule_named_future_state("stress", format!("job-{index}"), runtime, move || {
                thread::sleep(Duration::from_millis(1));
                Ok(DgmValue::Int(index as i64))
            })
            .unwrap();
            if index % 3 == 0 {
                let _ = future.request_cancel();
            }
            futures.push((index, future));
        }

        let mut completed = 0usize;
        let mut cancelled = 0usize;
        for (index, future) in &futures {
            match await_future_payload(future) {
                Ok(payload) => match payload.value {
                    OwnedValue::Int(value) => {
                        assert_eq!(value, *index as i64);
                        completed += 1;
                    }
                    other => panic!("unexpected payload: {:?}", other),
                },
                Err(err) => {
                    assert_eq!(err.message_string(), "stress cancelled");
                    cancelled += 1;
                }
            }
        }
        assert_eq!(completed + cancelled, 10_000);
        assert!(cancelled >= 1_000);

        drop(futures);
        thread::sleep(Duration::from_millis(50));
        let snapshot = runtime_async_snapshot();
        assert_eq!(snapshot.pending_jobs, 0);
        assert_eq!(snapshot.running_jobs, 0);
        assert_eq!(
            snapshot.submitted_jobs.saturating_sub(baseline.submitted_jobs),
            10_000
        );
        assert_eq!(snapshot.retained_tasks, 0);
    }

    #[test]
    fn cancel_complete_race_keeps_consistent_status() {
        let _guard = test_lock();
        for index in 0..256u64 {
            let future = Arc::new(FutureState::new(index + 10, "race", format!("race-{index}")));
            assert!(future.mark_running("race-worker".into()));
            let barrier = Arc::new(Barrier::new(3));

            let cancel_future = Arc::clone(&future);
            let cancel_barrier = Arc::clone(&barrier);
            let cancel_thread = thread::spawn(move || {
                cancel_barrier.wait();
                let _ = cancel_future.request_cancel();
            });

            let complete_future = Arc::clone(&future);
            let complete_barrier = Arc::clone(&barrier);
            let complete_thread = thread::spawn(move || {
                complete_barrier.wait();
                complete_future.complete(
                    Ok(OwnedPayload {
                        envs: Arc::new(HashMap::new()),
                        value: OwnedValue::Int(index as i64),
                    }),
                    RuntimeProfile::default(),
                    usage_snapshot(),
                    "race-worker".into(),
                );
            });

            barrier.wait();
            cancel_thread.join().unwrap();
            complete_thread.join().unwrap();

            match future.status() {
                AsyncStatus::Ready => {
                    let payload = await_future_payload(&future).unwrap();
                    match payload.value {
                        OwnedValue::Int(value) => assert_eq!(value, index as i64),
                        other => panic!("unexpected payload: {:?}", other),
                    }
                }
                AsyncStatus::Cancelled => {
                    let err = await_future_payload(&future).unwrap_err();
                    assert_eq!(err.message_string(), "race cancelled");
                }
                other => panic!("unexpected status: {:?}", other),
            }
        }
    }
}
