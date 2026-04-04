use std::collections::HashMap;

use crate::error::DgmError;
use crate::interpreter::{value_repr, DgmValue};

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("info", log_info),
        ("warn", log_warn),
        ("error", log_error),
    ];
    for (name, func) in fns {
        m.insert(
            name.to_string(),
            DgmValue::NativeFunction {
                name: format!("log.{}", name),
                func: *func,
            },
        );
    }
    m
}

fn log_info(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    emit("info", &args);
    Ok(DgmValue::Null)
}

fn log_warn(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    emit("warn", &args);
    Ok(DgmValue::Null)
}

fn log_error(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    emit("error", &args);
    Ok(DgmValue::Null)
}

fn emit(level: &str, args: &[DgmValue]) {
    let body = args.iter().map(value_repr).collect::<Vec<_>>().join(" ");
    eprintln!("[{}] {}", level, body);
}
