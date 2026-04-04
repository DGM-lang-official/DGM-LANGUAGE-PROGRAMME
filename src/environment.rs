use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::error::DgmError;
use crate::interpreter::DgmValue;
use crate::request_scope::track_request_environment_ref;

const ENV_VALUE_POOL_MAX: usize = 256;
const ENV_VALUE_POOL_MAX_CAPACITY: usize = 64;

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct EnvironmentPoolSnapshot {
    pub available: usize,
    pub hits: u64,
    pub misses: u64,
    pub recycled: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct EnvironmentLiveSnapshot {
    pub live: u64,
    pub created: u64,
    pub dropped: u64,
    pub pooled_maps: u64,
    pub pooled_capacity: u64,
}

#[derive(Debug, Default)]
struct EnvironmentPoolState {
    maps: Vec<HashMap<String, DgmValue>>,
    hits: u64,
    misses: u64,
    recycled: u64,
}

thread_local! {
    static ENV_VALUE_POOL: RefCell<EnvironmentPoolState> = RefCell::new(EnvironmentPoolState::default());
    static ENV_CLEANUP_TRACKER: RefCell<EnvironmentCleanupTracker> = RefCell::new(EnvironmentCleanupTracker::default());
}

static LIVE_ENVIRONMENTS: AtomicU64 = AtomicU64::new(0);
static CREATED_ENVIRONMENTS: AtomicU64 = AtomicU64::new(0);
static DROPPED_ENVIRONMENTS: AtomicU64 = AtomicU64::new(0);
static POOLED_MAPS: AtomicU64 = AtomicU64::new(0);
static POOLED_CAPACITY: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct EnvironmentCleanupTracker {
    depth: usize,
    envs: Vec<Weak<RefCell<Environment>>>,
}

pub(crate) struct EnvironmentCleanupScope;

fn record_environment_created() {
    LIVE_ENVIRONMENTS.fetch_add(1, Ordering::Relaxed);
    CREATED_ENVIRONMENTS.fetch_add(1, Ordering::Relaxed);
}

fn record_environment_dropped() {
    LIVE_ENVIRONMENTS.fetch_sub(1, Ordering::Relaxed);
    DROPPED_ENVIRONMENTS.fetch_add(1, Ordering::Relaxed);
}

fn record_pooled_map_add(capacity: usize) {
    POOLED_MAPS.fetch_add(1, Ordering::Relaxed);
    POOLED_CAPACITY.fetch_add(capacity.min(u64::MAX as usize) as u64, Ordering::Relaxed);
}

fn record_pooled_map_remove(capacity: usize) {
    POOLED_MAPS.fetch_sub(1, Ordering::Relaxed);
    POOLED_CAPACITY.fetch_sub(capacity.min(u64::MAX as usize) as u64, Ordering::Relaxed);
}

fn track_environment_ref(env: &Rc<RefCell<Environment>>) {
    ENV_CLEANUP_TRACKER.with(|tracker| {
        let mut tracker = tracker.borrow_mut();
        if tracker.depth > 0 {
            tracker.envs.push(Rc::downgrade(env));
        }
    });
}

fn pooled_values_map() -> HashMap<String, DgmValue> {
    ENV_VALUE_POOL.with(|pool| {
        let mut state = pool.borrow_mut();
        if let Some(mut map) = state.maps.pop() {
            state.hits = state.hits.saturating_add(1);
            record_pooled_map_remove(map.capacity());
            map.clear();
            map
        } else {
            state.misses = state.misses.saturating_add(1);
            HashMap::new()
        }
    })
}

fn recycle_values_map(mut values: HashMap<String, DgmValue>) {
    if values.capacity() > ENV_VALUE_POOL_MAX_CAPACITY {
        return;
    }
    values.clear();
    ENV_VALUE_POOL.with(|pool| {
        let mut state = pool.borrow_mut();
        if state.maps.len() >= ENV_VALUE_POOL_MAX {
            return;
        }
        state.recycled = state.recycled.saturating_add(1);
        record_pooled_map_add(values.capacity());
        state.maps.push(values);
    });
}

#[derive(Debug, Clone)]
pub struct Environment {
    values: HashMap<String, DgmValue>,
    parent: Option<Rc<RefCell<Environment>>>,
}

impl Environment {
    pub fn new() -> Self {
        record_environment_created();
        Self { values: pooled_values_map(), parent: None }
    }

    pub fn new_child(parent: Rc<RefCell<Environment>>) -> Self {
        record_environment_created();
        Self { values: pooled_values_map(), parent: Some(parent) }
    }

    pub fn get(&self, name: &str) -> Option<DgmValue> {
        if let Some(v) = self.values.get(name) {
            Some(v.clone())
        } else if let Some(p) = &self.parent {
            p.borrow().get(name)
        } else {
            None
        }
    }

    pub fn get_current(&self, name: &str) -> Option<DgmValue> {
        self.values.get(name).cloned()
    }

    pub fn set(&mut self, name: &str, value: DgmValue) {
        self.values.insert(name.to_string(), value);
    }

    /// Assign to existing variable, walking up scope chain
    pub fn assign(&mut self, name: &str, value: DgmValue) -> Result<(), DgmError> {
        if self.values.contains_key(name) {
            self.values.insert(name.to_string(), value);
            Ok(())
        } else if let Some(p) = &self.parent {
            p.borrow_mut().assign(name, value)
        } else {
            Err(DgmError::RuntimeError { msg: format!("undefined variable '{}'", name) })
        }
    }

    /// Check if a variable exists in current or parent scope
    pub fn has(&self, name: &str) -> bool {
        if self.values.contains_key(name) {
            true
        } else if let Some(p) = &self.parent {
            p.borrow().has(name)
        } else {
            false
        }
    }

    /// Get all variable names in current scope (not parents)
    pub fn keys(&self) -> Vec<String> {
        self.values.keys().cloned().collect()
    }

    pub fn entries(&self) -> Vec<(String, DgmValue)> {
        self.values.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn parent(&self) -> Option<Rc<RefCell<Environment>>> {
        self.parent.clone()
    }

    /// Remove a variable from current scope
    pub fn remove(&mut self, name: &str) {
        self.values.remove(name);
    }

    pub(crate) fn clear_values(&mut self) {
        self.values.clear();
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        record_environment_dropped();
        let values = std::mem::take(&mut self.values);
        recycle_values_map(values);
    }
}

impl Drop for EnvironmentCleanupScope {
    fn drop(&mut self) {
        let envs = ENV_CLEANUP_TRACKER.with(|tracker| {
            let mut tracker = tracker.borrow_mut();
            if tracker.depth == 0 {
                return vec![];
            }
            tracker.depth -= 1;
            if tracker.depth == 0 {
                std::mem::take(&mut tracker.envs)
            } else {
                vec![]
            }
        });
        for env in envs {
            if let Some(env) = env.upgrade() {
                env.borrow_mut().clear_values();
            }
        }
    }
}

pub(crate) fn environment_live_snapshot() -> EnvironmentLiveSnapshot {
    EnvironmentLiveSnapshot {
        live: LIVE_ENVIRONMENTS.load(Ordering::Relaxed),
        created: CREATED_ENVIRONMENTS.load(Ordering::Relaxed),
        dropped: DROPPED_ENVIRONMENTS.load(Ordering::Relaxed),
        pooled_maps: POOLED_MAPS.load(Ordering::Relaxed),
        pooled_capacity: POOLED_CAPACITY.load(Ordering::Relaxed),
    }
}

pub(crate) fn begin_environment_cleanup_scope() -> EnvironmentCleanupScope {
    ENV_CLEANUP_TRACKER.with(|tracker| {
        let mut tracker = tracker.borrow_mut();
        tracker.depth = tracker.depth.saturating_add(1);
    });
    EnvironmentCleanupScope
}

pub(crate) fn new_environment_ref() -> Rc<RefCell<Environment>> {
    let env = Rc::new(RefCell::new(Environment::new()));
    track_environment_ref(&env);
    track_request_environment_ref(&env);
    env
}

pub(crate) fn new_child_environment_ref(parent: Rc<RefCell<Environment>>) -> Rc<RefCell<Environment>> {
    let env = Rc::new(RefCell::new(Environment::new_child(parent)));
    track_environment_ref(&env);
    track_request_environment_ref(&env);
    env
}

#[cfg(test)]
pub(crate) fn reset_environment_pool() {
    ENV_VALUE_POOL.with(|pool| {
        let mut state = pool.borrow_mut();
        for map in state.maps.drain(..) {
            record_pooled_map_remove(map.capacity());
        }
        *state = EnvironmentPoolState::default();
    });
    LIVE_ENVIRONMENTS.store(0, Ordering::Relaxed);
    CREATED_ENVIRONMENTS.store(0, Ordering::Relaxed);
    DROPPED_ENVIRONMENTS.store(0, Ordering::Relaxed);
    POOLED_MAPS.store(0, Ordering::Relaxed);
    POOLED_CAPACITY.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn environment_pool_snapshot() -> EnvironmentPoolSnapshot {
    ENV_VALUE_POOL.with(|pool| {
        let state = pool.borrow();
        EnvironmentPoolSnapshot {
            available: state.maps.len(),
            hits: state.hits,
            misses: state.misses,
            recycled: state.recycled,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_pool_reuses_small_scope_maps() {
        reset_environment_pool();
        {
            let mut env = Environment::new();
            env.set("x", DgmValue::Int(1));
        }
        let after_drop = environment_pool_snapshot();
        assert_eq!(after_drop.available, 1);
        assert!(after_drop.misses >= 1);
        assert!(after_drop.recycled >= 1);
        {
            let _env = Environment::new();
        }
        let after_reuse = environment_pool_snapshot();
        assert!(after_reuse.hits >= 1);
        assert_eq!(after_reuse.available, 1);
    }
}
