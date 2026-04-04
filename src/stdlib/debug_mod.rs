use std::collections::HashMap;
use crate::error::DgmError;
use crate::interpreter::{dgm_eq, value_repr, DgmValue};

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("repr", debug_repr),
        ("dbg", debug_dbg),
        ("assert", debug_assert),
        ("assert_eq", debug_assert_eq),
        ("panic", debug_panic),
        ("native_panic", debug_native_panic),
    ];
    for (name, func) in fns {
        m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("debug.{}", name), func: *func });
    }
    m
}

fn debug_repr(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    Ok(DgmValue::Str(value_repr(args.first().unwrap_or(&DgmValue::Null))))
}

fn debug_dbg(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let value = args.first().cloned().unwrap_or(DgmValue::Null);
    eprintln!("[debug] {}", value_repr(&value));
    Ok(value)
}

fn debug_assert(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Bool(true)) => Ok(DgmValue::Null),
        Some(DgmValue::Bool(false)) => Err(DgmError::RuntimeError { msg: "assertion failed".into() }),
        Some(other) => Err(DgmError::RuntimeError { msg: format!("assertion expected bool, got {}", other) }),
        None => Err(DgmError::RuntimeError { msg: "assert(value) required".into() }),
    }
}

fn debug_assert_eq(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(a), Some(b)) if dgm_eq(a, b) => Ok(DgmValue::Null),
        (Some(a), Some(b)) => Err(DgmError::RuntimeError { msg: format!("assert_eq failed: {} != {}", value_repr(a), value_repr(b)) }),
        _ => Err(DgmError::RuntimeError { msg: "assert_eq(a, b) required".into() }),
    }
}

fn debug_panic(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let msg = args.first().map(|v| format!("{}", v)).unwrap_or_else(|| "panic".into());
    Err(DgmError::RuntimeError { msg })
}

fn debug_native_panic(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let msg = args.first().map(|v| format!("{}", v)).unwrap_or_else(|| "native panic".into());
    panic!("{}", msg);
}
