use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use crate::error::DgmError;
use crate::interpreter::{reserve_string_bytes, runtime_reserve_value, DgmValue};

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("split", str_split),
        ("join", str_join),
        ("upper", str_upper),
        ("lower", str_lower),
        ("trim", str_trim),
        ("replace", str_replace),
        ("contains", str_contains),
        ("starts_with", str_starts_with),
        ("ends_with", str_ends_with),
        ("chars", str_chars),
        ("format", str_format),
    ];
    for (name, func) in fns {
        m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("strutils.{}", name), func: *func });
    }
    m
}

fn str_split(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(sep))) => {
            let out = DgmValue::List(Rc::new(RefCell::new(s.split(sep).map(|part| DgmValue::Str(part.to_string())).collect())));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "strutils.split(str, sep) required".into() }),
    }
}

fn str_join(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(items)), Some(DgmValue::Str(sep))) => {
            let out = items.borrow().iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join(sep);
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        (Some(DgmValue::List(items)), None) => {
            let out = items.borrow().iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join("");
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "strutils.join(list, sep?) required".into() }),
    }
}

fn str_upper(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(s)) => {
            let out = s.to_uppercase();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "strutils.upper(str) required".into() }),
    }
}

fn str_lower(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(s)) => {
            let out = s.to_lowercase();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "strutils.lower(str) required".into() }),
    }
}

fn str_trim(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(s)) => {
            let out = s.trim().to_string();
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "strutils.trim(str) required".into() }),
    }
}

fn str_replace(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1), args.get(2)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(from)), Some(DgmValue::Str(to))) => {
            let out = s.replace(from, to);
            reserve_string_bytes(out.len())?;
            Ok(DgmValue::Str(out))
        }
        _ => Err(DgmError::RuntimeError { msg: "strutils.replace(str, from, to) required".into() }),
    }
}

fn str_contains(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(needle))) => Ok(DgmValue::Bool(s.contains(needle))),
        _ => Err(DgmError::RuntimeError { msg: "strutils.contains(str, sub) required".into() }),
    }
}

fn str_starts_with(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(prefix))) => Ok(DgmValue::Bool(s.starts_with(prefix))),
        _ => Err(DgmError::RuntimeError { msg: "strutils.starts_with(str, prefix) required".into() }),
    }
}

fn str_ends_with(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(s)), Some(DgmValue::Str(suffix))) => Ok(DgmValue::Bool(s.ends_with(suffix))),
        _ => Err(DgmError::RuntimeError { msg: "strutils.ends_with(str, suffix) required".into() }),
    }
}

fn str_chars(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(s)) => {
            let out = DgmValue::List(Rc::new(RefCell::new(s.chars().map(|c| DgmValue::Str(c.to_string())).collect())));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "strutils.chars(str) required".into() }),
    }
}

fn str_format(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "strutils.format(str, ...args) required".into() });
    }
    let mut output = match &args[0] {
        DgmValue::Str(s) => s.clone(),
        _ => return Err(DgmError::RuntimeError { msg: "strutils.format(str, ...args) required".into() }),
    };
    for arg in &args[1..] {
        output = output.replacen("{}", &format!("{}", arg), 1);
    }
    reserve_string_bytes(output.len())?;
    Ok(DgmValue::Str(output))
}
