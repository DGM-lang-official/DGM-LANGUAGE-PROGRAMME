use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::{Rc, Weak as RcWeak};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak as ArcWeak};

use crate::environment::Environment;
use crate::interpreter::{
    JsonValueState, RawJsonValue, RequestJsonValue, RequestMapValue, RequestShellValue, RequestValue,
};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RequestScopeSurvivorSnapshot {
    pub environments: u64,
    pub requests: u64,
    pub request_maps: u64,
    pub request_json: u64,
    pub json_views: u64,
    pub raw_json: u64,
}

impl RequestScopeSurvivorSnapshot {
    pub(crate) fn total(self) -> u64 {
        self.environments
            .saturating_add(self.requests)
            .saturating_add(self.request_maps)
            .saturating_add(self.request_json)
            .saturating_add(self.json_views)
            .saturating_add(self.raw_json)
    }

    pub(crate) fn assertable_total(self) -> u64 {
        self.environments
            .saturating_add(self.requests)
            .saturating_add(self.request_maps)
            .saturating_add(self.request_json)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RequestScopeSnapshot {
    pub started: u64,
    pub completed: u64,
    pub scopes_with_survivors: u64,
    pub scopes_with_assertable_survivors: u64,
    pub scopes_with_report_only_survivors: u64,
    pub total_survivors: RequestScopeSurvivorSnapshot,
    pub last_survivors: RequestScopeSurvivorSnapshot,
}

#[derive(Default)]
struct RequestScopeState {
    depth: usize,
    tracked: Vec<TrackedRequestObject>,
    response_owned_raw_json: HashSet<usize>,
}

enum TrackedRequestObject {
    Environment(RcWeak<RefCell<Environment>>),
    #[allow(dead_code)]
    Request(RcWeak<RequestValue>),
    RequestShell(RcWeak<RequestShellValue>),
    RequestMap(RcWeak<RequestMapValue>),
    RequestJson(RcWeak<RequestJsonValue>),
    JsonView(ArcWeak<JsonValueState>),
    RawJson(ArcWeak<RawJsonValue>),
}

thread_local! {
    static REQUEST_SCOPE_STATE: RefCell<RequestScopeState> = RefCell::new(RequestScopeState::default());
}

static REQUEST_SCOPES_STARTED: AtomicU64 = AtomicU64::new(0);
static REQUEST_SCOPES_COMPLETED: AtomicU64 = AtomicU64::new(0);
static REQUEST_SCOPES_WITH_SURVIVORS: AtomicU64 = AtomicU64::new(0);
static REQUEST_SCOPES_WITH_ASSERTABLE_SURVIVORS: AtomicU64 = AtomicU64::new(0);
static REQUEST_SCOPES_WITH_REPORT_ONLY_SURVIVORS: AtomicU64 = AtomicU64::new(0);

static TOTAL_SURVIVOR_ENVIRONMENTS: AtomicU64 = AtomicU64::new(0);
static TOTAL_SURVIVOR_REQUESTS: AtomicU64 = AtomicU64::new(0);
static TOTAL_SURVIVOR_REQUEST_MAPS: AtomicU64 = AtomicU64::new(0);
static TOTAL_SURVIVOR_REQUEST_JSON: AtomicU64 = AtomicU64::new(0);
static TOTAL_SURVIVOR_JSON_VIEWS: AtomicU64 = AtomicU64::new(0);
static TOTAL_SURVIVOR_RAW_JSON: AtomicU64 = AtomicU64::new(0);

static LAST_SURVIVOR_ENVIRONMENTS: AtomicU64 = AtomicU64::new(0);
static LAST_SURVIVOR_REQUESTS: AtomicU64 = AtomicU64::new(0);
static LAST_SURVIVOR_REQUEST_MAPS: AtomicU64 = AtomicU64::new(0);
static LAST_SURVIVOR_REQUEST_JSON: AtomicU64 = AtomicU64::new(0);
static LAST_SURVIVOR_JSON_VIEWS: AtomicU64 = AtomicU64::new(0);
static LAST_SURVIVOR_RAW_JSON: AtomicU64 = AtomicU64::new(0);

pub(crate) struct RequestScopeGuard;

fn summarize_survivors(
    tracked: Vec<TrackedRequestObject>,
    response_owned_raw_json: &HashSet<usize>,
) -> RequestScopeSurvivorSnapshot {
    let mut survivors = RequestScopeSurvivorSnapshot::default();
    for object in tracked {
        match object {
            TrackedRequestObject::Environment(weak) if weak.upgrade().is_some() => {
                survivors.environments = survivors.environments.saturating_add(1);
            }
            TrackedRequestObject::Request(weak) if weak.upgrade().is_some() => {
                survivors.requests = survivors.requests.saturating_add(1);
            }
            TrackedRequestObject::RequestShell(weak) if weak.upgrade().is_some() => {
                survivors.requests = survivors.requests.saturating_add(1);
            }
            TrackedRequestObject::RequestMap(weak) if weak.upgrade().is_some() => {
                survivors.request_maps = survivors.request_maps.saturating_add(1);
            }
            TrackedRequestObject::RequestJson(weak) if weak.upgrade().is_some() => {
                survivors.request_json = survivors.request_json.saturating_add(1);
            }
            TrackedRequestObject::JsonView(weak) if weak.upgrade().is_some() => {
                survivors.json_views = survivors.json_views.saturating_add(1);
            }
            TrackedRequestObject::RawJson(weak) => {
                if let Some(raw) = weak.upgrade() {
                    let id = Arc::as_ptr(&raw) as usize;
                    if !response_owned_raw_json.contains(&id) {
                        survivors.raw_json = survivors.raw_json.saturating_add(1);
                    }
                }
            }
            _ => {}
        }
    }
    survivors
}

fn record_scope_survivors(survivors: RequestScopeSurvivorSnapshot) {
    LAST_SURVIVOR_ENVIRONMENTS.store(survivors.environments, Ordering::Relaxed);
    LAST_SURVIVOR_REQUESTS.store(survivors.requests, Ordering::Relaxed);
    LAST_SURVIVOR_REQUEST_MAPS.store(survivors.request_maps, Ordering::Relaxed);
    LAST_SURVIVOR_REQUEST_JSON.store(survivors.request_json, Ordering::Relaxed);
    LAST_SURVIVOR_JSON_VIEWS.store(survivors.json_views, Ordering::Relaxed);
    LAST_SURVIVOR_RAW_JSON.store(survivors.raw_json, Ordering::Relaxed);

    if survivors.assertable_total() > 0 {
        REQUEST_SCOPES_WITH_SURVIVORS.fetch_add(1, Ordering::Relaxed);
        REQUEST_SCOPES_WITH_ASSERTABLE_SURVIVORS.fetch_add(1, Ordering::Relaxed);
    } else if survivors.total() > 0 {
        REQUEST_SCOPES_WITH_REPORT_ONLY_SURVIVORS.fetch_add(1, Ordering::Relaxed);
    }
    if survivors.total() > 0 {
        TOTAL_SURVIVOR_ENVIRONMENTS.fetch_add(survivors.environments, Ordering::Relaxed);
        TOTAL_SURVIVOR_REQUESTS.fetch_add(survivors.requests, Ordering::Relaxed);
        TOTAL_SURVIVOR_REQUEST_MAPS.fetch_add(survivors.request_maps, Ordering::Relaxed);
        TOTAL_SURVIVOR_REQUEST_JSON.fetch_add(survivors.request_json, Ordering::Relaxed);
        TOTAL_SURVIVOR_JSON_VIEWS.fetch_add(survivors.json_views, Ordering::Relaxed);
        TOTAL_SURVIVOR_RAW_JSON.fetch_add(survivors.raw_json, Ordering::Relaxed);
    }
}

pub(crate) fn begin_request_scope() -> RequestScopeGuard {
    REQUEST_SCOPE_STATE.with(|state| {
        let mut state = state.borrow_mut();
        if state.depth == 0 {
            state.tracked.clear();
            state.response_owned_raw_json.clear();
            REQUEST_SCOPES_STARTED.fetch_add(1, Ordering::Relaxed);
        }
        state.depth = state.depth.saturating_add(1);
    });
    RequestScopeGuard
}

impl Drop for RequestScopeGuard {
    fn drop(&mut self) {
        let (tracked, response_owned_raw_json, finished) = REQUEST_SCOPE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if state.depth == 0 {
                return (vec![], HashSet::new(), false);
            }
            state.depth -= 1;
            if state.depth == 0 {
                (
                    std::mem::take(&mut state.tracked),
                    std::mem::take(&mut state.response_owned_raw_json),
                    true,
                )
            } else {
                (vec![], HashSet::new(), false)
            }
        });
        if !finished {
            return;
        }
        REQUEST_SCOPES_COMPLETED.fetch_add(1, Ordering::Relaxed);
        let survivors = summarize_survivors(tracked, &response_owned_raw_json);
        record_scope_survivors(survivors);
        if cfg!(debug_assertions) && !std::thread::panicking() && survivors.assertable_total() > 0 {
            panic!(
                "request scope retained request-owned objects: env={}, request={}, request_map={}, request_json={}, json_view={}, raw_json={}",
                survivors.environments,
                survivors.requests,
                survivors.request_maps,
                survivors.request_json,
                survivors.json_views,
                survivors.raw_json,
            );
        }
    }
}

fn track(object: TrackedRequestObject) {
    REQUEST_SCOPE_STATE.with(|state| {
        let mut state = state.borrow_mut();
        if state.depth > 0 {
            state.tracked.push(object);
        }
    });
}

pub(crate) fn track_request_environment_ref(env: &Rc<RefCell<Environment>>) {
    track(TrackedRequestObject::Environment(Rc::downgrade(env)));
}

#[allow(dead_code)]
pub(crate) fn track_request_ref(value: &Rc<RequestValue>) {
    track(TrackedRequestObject::Request(Rc::downgrade(value)));
}

pub(crate) fn track_request_shell_ref(value: &Rc<RequestShellValue>) {
    track(TrackedRequestObject::RequestShell(Rc::downgrade(value)));
}

pub(crate) fn track_request_map_ref(value: &Rc<RequestMapValue>) {
    track(TrackedRequestObject::RequestMap(Rc::downgrade(value)));
}

pub(crate) fn track_request_json_ref(value: &Rc<RequestJsonValue>) {
    track(TrackedRequestObject::RequestJson(Rc::downgrade(value)));
}

pub(crate) fn track_json_view_ref(value: &Arc<JsonValueState>) {
    track(TrackedRequestObject::JsonView(Arc::downgrade(value)));
}

pub(crate) fn track_raw_json_ref(value: &Arc<RawJsonValue>) {
    track(TrackedRequestObject::RawJson(Arc::downgrade(value)));
}

pub(crate) fn mark_response_owned_raw_json(value: &Arc<RawJsonValue>) {
    REQUEST_SCOPE_STATE.with(|state| {
        let mut state = state.borrow_mut();
        if state.depth > 0 {
            state
                .response_owned_raw_json
                .insert(Arc::as_ptr(value) as usize);
        }
    });
}

pub(crate) fn request_scope_snapshot() -> RequestScopeSnapshot {
    RequestScopeSnapshot {
        started: REQUEST_SCOPES_STARTED.load(Ordering::Relaxed),
        completed: REQUEST_SCOPES_COMPLETED.load(Ordering::Relaxed),
        scopes_with_survivors: REQUEST_SCOPES_WITH_SURVIVORS.load(Ordering::Relaxed),
        scopes_with_assertable_survivors: REQUEST_SCOPES_WITH_ASSERTABLE_SURVIVORS.load(Ordering::Relaxed),
        scopes_with_report_only_survivors: REQUEST_SCOPES_WITH_REPORT_ONLY_SURVIVORS.load(Ordering::Relaxed),
        total_survivors: RequestScopeSurvivorSnapshot {
            environments: TOTAL_SURVIVOR_ENVIRONMENTS.load(Ordering::Relaxed),
            requests: TOTAL_SURVIVOR_REQUESTS.load(Ordering::Relaxed),
            request_maps: TOTAL_SURVIVOR_REQUEST_MAPS.load(Ordering::Relaxed),
            request_json: TOTAL_SURVIVOR_REQUEST_JSON.load(Ordering::Relaxed),
            json_views: TOTAL_SURVIVOR_JSON_VIEWS.load(Ordering::Relaxed),
            raw_json: TOTAL_SURVIVOR_RAW_JSON.load(Ordering::Relaxed),
        },
        last_survivors: RequestScopeSurvivorSnapshot {
            environments: LAST_SURVIVOR_ENVIRONMENTS.load(Ordering::Relaxed),
            requests: LAST_SURVIVOR_REQUESTS.load(Ordering::Relaxed),
            request_maps: LAST_SURVIVOR_REQUEST_MAPS.load(Ordering::Relaxed),
            request_json: LAST_SURVIVOR_REQUEST_JSON.load(Ordering::Relaxed),
            json_views: LAST_SURVIVOR_JSON_VIEWS.load(Ordering::Relaxed),
            raw_json: LAST_SURVIVOR_RAW_JSON.load(Ordering::Relaxed),
        },
    }
}

#[cfg(test)]
pub(crate) fn reset_request_scope_stats() {
    REQUEST_SCOPE_STATE.with(|state| *state.borrow_mut() = RequestScopeState::default());
    REQUEST_SCOPES_STARTED.store(0, Ordering::Relaxed);
    REQUEST_SCOPES_COMPLETED.store(0, Ordering::Relaxed);
    REQUEST_SCOPES_WITH_SURVIVORS.store(0, Ordering::Relaxed);
    REQUEST_SCOPES_WITH_ASSERTABLE_SURVIVORS.store(0, Ordering::Relaxed);
    REQUEST_SCOPES_WITH_REPORT_ONLY_SURVIVORS.store(0, Ordering::Relaxed);
    TOTAL_SURVIVOR_ENVIRONMENTS.store(0, Ordering::Relaxed);
    TOTAL_SURVIVOR_REQUESTS.store(0, Ordering::Relaxed);
    TOTAL_SURVIVOR_REQUEST_MAPS.store(0, Ordering::Relaxed);
    TOTAL_SURVIVOR_REQUEST_JSON.store(0, Ordering::Relaxed);
    TOTAL_SURVIVOR_JSON_VIEWS.store(0, Ordering::Relaxed);
    TOTAL_SURVIVOR_RAW_JSON.store(0, Ordering::Relaxed);
    LAST_SURVIVOR_ENVIRONMENTS.store(0, Ordering::Relaxed);
    LAST_SURVIVOR_REQUESTS.store(0, Ordering::Relaxed);
    LAST_SURVIVOR_REQUEST_MAPS.store(0, Ordering::Relaxed);
    LAST_SURVIVOR_REQUEST_JSON.store(0, Ordering::Relaxed);
    LAST_SURVIVOR_JSON_VIEWS.store(0, Ordering::Relaxed);
    LAST_SURVIVOR_RAW_JSON.store(0, Ordering::Relaxed);
}
