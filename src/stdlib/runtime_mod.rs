use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use crate::error::DgmError;
use crate::concurrency::{runtime_async_stats_value, runtime_reset_snapshot_stats, runtime_snapshot_stats_value, runtime_tasks_value};
use crate::memory::force_trim_allocator;
use crate::interpreter::{
    runtime_reset_usage, runtime_set_max_call_depth, runtime_set_max_concurrent_requests, runtime_set_max_heap_bytes,
    runtime_set_max_open_sockets, runtime_set_max_steps, runtime_set_max_threads, runtime_set_max_wall_time_ms,
    runtime_profile_value, runtime_request_metrics_value, runtime_reset_request_metrics, runtime_record_request, runtime_stats, DgmValue,
};

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("set_max_steps", runtime_set_steps_fn),
        ("set_max_call_depth", runtime_set_call_depth_fn),
        ("set_max_heap_bytes", runtime_set_heap_bytes_fn),
        ("set_max_threads", runtime_set_threads_fn),
        ("set_max_wall_time_ms", runtime_set_wall_time_fn),
        ("set_max_open_sockets", runtime_set_open_sockets_fn),
        ("set_max_concurrent_requests", runtime_set_concurrent_requests_fn),
        ("stats", runtime_stats_fn),
        ("profile", runtime_profile_fn),
        ("request_metrics", runtime_request_metrics_fn),
        ("record_request", runtime_record_request_fn),
        ("reset_request_metrics", runtime_reset_request_metrics_fn),
        ("async_stats", runtime_async_stats_fn),
        ("snapshot_stats", runtime_snapshot_stats_fn),
        ("reset_snapshot_stats", runtime_reset_snapshot_stats_fn),
        ("tasks", runtime_tasks_fn),
        ("trim_memory", runtime_trim_memory_fn),
        ("reset_usage", runtime_reset_usage_fn),
    ];
    for (name, func) in fns {
        m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("runtime.{}", name), func: *func });
    }
    m
}

fn parse_positive_int(args: &[DgmValue], usage: &str) -> Result<i64, DgmError> {
    match args.first() {
        Some(DgmValue::Int(n)) if *n > 0 => Ok(*n),
        Some(DgmValue::Int(_)) => Err(DgmError::RuntimeError { msg: format!("{usage} requires limit > 0") }),
        _ => Err(DgmError::RuntimeError { msg: format!("{usage} requires int limit") }),
    }
}

fn runtime_set_steps_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_set_max_steps(parse_positive_int(&args, "runtime.set_max_steps")? as u64)?;
    Ok(DgmValue::Null)
}

fn runtime_set_call_depth_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_set_max_call_depth(parse_positive_int(&args, "runtime.set_max_call_depth")? as usize)?;
    Ok(DgmValue::Null)
}

fn runtime_set_heap_bytes_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_set_max_heap_bytes(parse_positive_int(&args, "runtime.set_max_heap_bytes")? as usize)?;
    Ok(DgmValue::Null)
}

fn runtime_set_threads_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_set_max_threads(parse_positive_int(&args, "runtime.set_max_threads")? as usize)?;
    Ok(DgmValue::Null)
}

fn runtime_set_wall_time_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_set_max_wall_time_ms(parse_positive_int(&args, "runtime.set_max_wall_time_ms")? as u64)?;
    Ok(DgmValue::Null)
}

fn runtime_set_open_sockets_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_set_max_open_sockets(parse_positive_int(&args, "runtime.set_max_open_sockets")? as usize)?;
    Ok(DgmValue::Null)
}

fn runtime_set_concurrent_requests_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_set_max_concurrent_requests(parse_positive_int(&args, "runtime.set_max_concurrent_requests")? as usize)?;
    Ok(DgmValue::Null)
}

fn runtime_stats_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    Ok(DgmValue::Map(Rc::new(RefCell::new(runtime_stats()))))
}

fn runtime_profile_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    Ok(runtime_profile_value())
}

fn runtime_request_metrics_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    Ok(runtime_request_metrics_value())
}

fn runtime_record_request_fn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let label = match args.first() {
        Some(DgmValue::Str(value)) => value.clone(),
        _ => return Err(DgmError::RuntimeError { msg: "runtime.record_request(label, status, elapsed_ms, auth_failed?) requires string label".into() }),
    };
    let status = match args.get(1) {
        Some(DgmValue::Int(value)) => *value,
        _ => return Err(DgmError::RuntimeError { msg: "runtime.record_request(label, status, elapsed_ms, auth_failed?) requires int status".into() }),
    };
    let elapsed_ms = match args.get(2) {
        Some(DgmValue::Int(value)) if *value >= 0 => *value as u64,
        Some(DgmValue::Int(_)) => return Err(DgmError::RuntimeError { msg: "runtime.record_request(label, status, elapsed_ms, auth_failed?) requires elapsed_ms >= 0".into() }),
        _ => return Err(DgmError::RuntimeError { msg: "runtime.record_request(label, status, elapsed_ms, auth_failed?) requires int elapsed_ms".into() }),
    };
    let auth_failed = match args.get(3) {
        Some(DgmValue::Bool(value)) => *value,
        Some(DgmValue::Int(value)) => *value != 0,
        None => false,
        _ => return Err(DgmError::RuntimeError { msg: "runtime.record_request(label, status, elapsed_ms, auth_failed?) requires bool auth_failed".into() }),
    };
    runtime_record_request(&label, status, elapsed_ms, auth_failed);
    Ok(DgmValue::Null)
}

fn runtime_reset_request_metrics_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_reset_request_metrics();
    Ok(DgmValue::Null)
}

fn runtime_async_stats_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    Ok(runtime_async_stats_value())
}

fn runtime_snapshot_stats_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    Ok(runtime_snapshot_stats_value())
}

fn runtime_reset_snapshot_stats_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_reset_snapshot_stats();
    Ok(DgmValue::Null)
}

fn runtime_tasks_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    Ok(runtime_tasks_value())
}

fn runtime_trim_memory_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    Ok(DgmValue::Bool(force_trim_allocator()))
}

fn runtime_reset_usage_fn(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    runtime_reset_usage();
    Ok(DgmValue::Null)
}
