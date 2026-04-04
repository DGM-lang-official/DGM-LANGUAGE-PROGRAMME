use std::fs;
use std::sync::{Mutex, Once, OnceLock};
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub(crate) enum TrimTrigger {
    Idle,
    Periodic,
    Manual,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MemoryMaintenanceSnapshot {
    pub trim_supported: bool,
    pub trim_calls: u64,
    pub trim_successes: u64,
    pub trim_failures: u64,
    pub idle_trim_calls: u64,
    pub periodic_trim_calls: u64,
    pub manual_trim_calls: u64,
    pub last_trim_elapsed_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ProcessMemorySnapshot {
    pub rss_bytes: u64,
    pub virtual_bytes: u64,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AllocatorSnapshot {
    pub supported: bool,
    pub arena_bytes: u64,
    pub mmap_bytes: u64,
    pub in_use_bytes: u64,
    pub free_bytes: u64,
    pub releasable_bytes: u64,
    pub free_chunks: u64,
}

#[derive(Debug, Default)]
struct MemoryMaintenanceState {
    snapshot: MemoryMaintenanceSnapshot,
    started_at: Option<Instant>,
}

static MEMORY_STATE: OnceLock<Mutex<MemoryMaintenanceState>> = OnceLock::new();

fn memory_state() -> &'static Mutex<MemoryMaintenanceState> {
    MEMORY_STATE.get_or_init(|| {
        let mut state = MemoryMaintenanceState::default();
        state.snapshot.trim_supported = trim_supported();
        state.started_at = Some(Instant::now());
        Mutex::new(state)
    })
}

fn lock_memory_state() -> std::sync::MutexGuard<'static, MemoryMaintenanceState> {
    match memory_state().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn init_allocator_maintenance() {
    trim_impl::init();
    let mut state = lock_memory_state();
    state.snapshot.trim_supported = trim_supported();
    state.started_at.get_or_insert_with(Instant::now);
}

pub(crate) fn reset_memory_snapshot() {
    let mut state = lock_memory_state();
    state.snapshot = MemoryMaintenanceSnapshot {
        trim_supported: trim_supported(),
        ..MemoryMaintenanceSnapshot::default()
    };
    state.started_at = Some(Instant::now());
}

pub(crate) fn memory_snapshot() -> MemoryMaintenanceSnapshot {
    let mut snapshot = lock_memory_state().snapshot.clone();
    snapshot.trim_supported = trim_supported();
    snapshot
}

pub(crate) fn maybe_trim_allocator(trigger: TrimTrigger) -> bool {
    init_allocator_maintenance();
    let trimmed = trim_impl::trim();
    let mut state = lock_memory_state();
    state.snapshot.trim_calls = state.snapshot.trim_calls.saturating_add(1);
    match trigger {
        TrimTrigger::Idle => {
            state.snapshot.idle_trim_calls = state.snapshot.idle_trim_calls.saturating_add(1);
        }
        TrimTrigger::Periodic => {
            state.snapshot.periodic_trim_calls = state.snapshot.periodic_trim_calls.saturating_add(1);
        }
        TrimTrigger::Manual => {
            state.snapshot.manual_trim_calls = state.snapshot.manual_trim_calls.saturating_add(1);
        }
    }
    if trimmed {
        state.snapshot.trim_successes = state.snapshot.trim_successes.saturating_add(1);
    } else {
        state.snapshot.trim_failures = state.snapshot.trim_failures.saturating_add(1);
    }
    state.snapshot.last_trim_elapsed_ms = state
        .started_at
        .map(|started| started.elapsed().as_millis().min(u64::MAX as u128) as u64);
    trimmed
}

pub(crate) fn trim_supported() -> bool {
    trim_impl::supported()
}

pub(crate) fn force_trim_allocator() -> bool {
    maybe_trim_allocator(TrimTrigger::Manual)
}

pub(crate) fn process_memory_snapshot() -> ProcessMemorySnapshot {
    process_impl::snapshot()
}

pub(crate) fn allocator_snapshot() -> AllocatorSnapshot {
    init_allocator_maintenance();
    allocator_impl::snapshot()
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod trim_impl {
    use super::*;

    static INIT: Once = Once::new();

    const TRIM_THRESHOLD_BYTES: libc::c_int = 128 * 1024;
    const MMAP_THRESHOLD_BYTES: libc::c_int = 128 * 1024;
    const MAX_ARENAS: libc::c_int = 4;

    pub(crate) fn supported() -> bool {
        true
    }

    pub(crate) fn init() {
        INIT.call_once(|| unsafe {
            libc::mallopt(libc::M_TRIM_THRESHOLD, TRIM_THRESHOLD_BYTES);
            libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES);
            libc::mallopt(libc::M_ARENA_MAX, MAX_ARENAS);
        });
    }

    pub(crate) fn trim() -> bool {
        unsafe { libc::malloc_trim(0) != 0 }
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
mod trim_impl {
    pub(crate) fn supported() -> bool {
        false
    }

    pub(crate) fn init() {}

    pub(crate) fn trim() -> bool {
        false
    }
}

#[cfg(target_os = "linux")]
mod process_impl {
    use super::*;

    pub(crate) fn snapshot() -> ProcessMemorySnapshot {
        let statm = fs::read_to_string("/proc/self/statm").ok();
        let Some(statm) = statm else {
            return ProcessMemorySnapshot::default();
        };
        let mut parts = statm.split_whitespace();
        let total_pages = parts.next().and_then(|value| value.parse::<u64>().ok()).unwrap_or(0);
        let rss_pages = parts.next().and_then(|value| value.parse::<u64>().ok()).unwrap_or(0);
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page_size = if page_size > 0 { page_size as u64 } else { 4096 };
        ProcessMemorySnapshot {
            rss_bytes: rss_pages.saturating_mul(page_size),
            virtual_bytes: total_pages.saturating_mul(page_size),
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod process_impl {
    use super::*;

    pub(crate) fn snapshot() -> ProcessMemorySnapshot {
        ProcessMemorySnapshot::default()
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod allocator_impl {
    use super::*;

    fn clamp_usize(value: usize) -> u64 {
        value.min(u64::MAX as usize) as u64
    }

    pub(crate) fn snapshot() -> AllocatorSnapshot {
        let stats = unsafe { libc::mallinfo2() };
        AllocatorSnapshot {
            supported: true,
            arena_bytes: clamp_usize(stats.arena),
            mmap_bytes: clamp_usize(stats.hblkhd),
            in_use_bytes: clamp_usize(stats.uordblks),
            free_bytes: clamp_usize(stats.fordblks),
            releasable_bytes: clamp_usize(stats.keepcost),
            free_chunks: clamp_usize(stats.ordblks),
        }
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
mod allocator_impl {
    use super::*;

    pub(crate) fn snapshot() -> AllocatorSnapshot {
        AllocatorSnapshot::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    #[test]
    fn memory_snapshot_tracks_trim_attempts() {
        let _guard = test_lock();
        reset_memory_snapshot();
        let before = memory_snapshot();
        assert_eq!(before.trim_calls, 0);
        let _ = maybe_trim_allocator(TrimTrigger::Periodic);
        let after = memory_snapshot();
        assert_eq!(after.trim_calls, 1);
        assert_eq!(after.periodic_trim_calls, 1);
    }

    #[test]
    fn force_trim_tracks_manual_trigger() {
        let _guard = test_lock();
        reset_memory_snapshot();
        let _ = force_trim_allocator();
        let after = memory_snapshot();
        assert_eq!(after.trim_calls, 1);
        assert_eq!(after.manual_trim_calls, 1);
    }
}
