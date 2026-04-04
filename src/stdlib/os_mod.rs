use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use crate::interpreter::DgmValue;
use crate::error::DgmError;

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("exec", os_exec), ("env", os_env), ("envs", os_envs), ("set_env", os_set_env),
        ("platform", os_platform), ("exit", os_exit), ("args", os_args),
        ("pid", os_pid), ("sleep", os_sleep), ("home_dir", os_home_dir),
        ("arch", os_arch), ("num_cpus", os_num_cpus),
    ];
    for (name, func) in fns { m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("os.{}", name), func: *func }); }
    m
}
fn os_exec(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(cmd)) => {
            let output = std::process::Command::new("sh").arg("-c").arg(cmd).output().map_err(|e| DgmError::RuntimeError { msg: format!("exec: {}", e) })?;
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            let mut result = HashMap::new();
            result.insert("stdout".into(), DgmValue::Str(stdout));
            result.insert("stderr".into(), DgmValue::Str(stderr));
            result.insert("code".into(), DgmValue::Int(output.status.code().unwrap_or(-1) as i64));
            result.insert("ok".into(), DgmValue::Bool(output.status.success()));
            Ok(DgmValue::Map(Rc::new(RefCell::new(result))))
        }
        _ => Err(DgmError::RuntimeError { msg: "exec(cmd) required".into() }),
    }
}
fn os_env(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Str(k)) => Ok(std::env::var(k).map(DgmValue::Str).unwrap_or(DgmValue::Null)), _ => Err(DgmError::RuntimeError { msg: "env(key) required".into() }) }
}
fn os_envs(_a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let envs: HashMap<String, DgmValue> = std::env::vars().map(|(k, v)| (k, DgmValue::Str(v))).collect();
    Ok(DgmValue::Map(Rc::new(RefCell::new(envs))))
}
fn os_set_env(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (a.get(0), a.get(1)) { (Some(DgmValue::Str(k)), Some(DgmValue::Str(v))) => { std::env::set_var(k, v); Ok(DgmValue::Null) } _ => Err(DgmError::RuntimeError { msg: "set_env(k, v) required".into() }) }
}
fn os_platform(_a: Vec<DgmValue>) -> Result<DgmValue, DgmError> { Ok(DgmValue::Str(std::env::consts::OS.to_string())) }
fn os_exit(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> { let code = match a.first() { Some(DgmValue::Int(n)) => *n as i32, _ => 0 }; std::process::exit(code); }
fn os_args(_a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let args: Vec<DgmValue> = std::env::args().map(|a| DgmValue::Str(a)).collect();
    Ok(DgmValue::List(Rc::new(RefCell::new(args))))
}
fn os_pid(_a: Vec<DgmValue>) -> Result<DgmValue, DgmError> { Ok(DgmValue::Int(std::process::id() as i64)) }
fn os_sleep(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Int(ms)) => { std::thread::sleep(std::time::Duration::from_millis(*ms as u64)); Ok(DgmValue::Null) } _ => Err(DgmError::RuntimeError { msg: "sleep(ms) required".into() }) }
}
fn os_home_dir(_a: Vec<DgmValue>) -> Result<DgmValue, DgmError> { Ok(DgmValue::Str(std::env::var("HOME").unwrap_or_default())) }
fn os_arch(_a: Vec<DgmValue>) -> Result<DgmValue, DgmError> { Ok(DgmValue::Str(std::env::consts::ARCH.to_string())) }
fn os_num_cpus(_a: Vec<DgmValue>) -> Result<DgmValue, DgmError> { Ok(DgmValue::Int(std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as i64)) }
