use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::rc::Rc;
use crate::interpreter::DgmValue;
use crate::error::DgmError;

pub(crate) const DEFAULT_FILE_READ_LIMIT_BYTES: u64 = 10 * 1024 * 1024;

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("read_file", io_read_file), ("write_file", io_write_file), ("append_file", io_append_file),
        ("exists", io_exists), ("delete", io_delete), ("mkdir", io_mkdir), ("list_dir", io_list_dir),
        ("cwd", io_cwd), ("input", io_input), ("read_lines", io_read_lines),
        ("file_size", io_file_size), ("is_dir", io_is_dir), ("is_file", io_is_file),
        ("rename", io_rename), ("copy", io_copy), ("abs_path", io_abs_path),
    ];
    for (name, func) in fns { m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("io.{}", name), func: *func }); }
    m
}

pub(crate) fn read_text_file_limited(path: &str, context: &str, max_bytes: u64) -> Result<String, DgmError> {
    let file = File::open(path).map_err(|e| DgmError::RuntimeError { msg: format!("{context}: {}", e) })?;
    let mut limited = file.take(max_bytes + 1);
    let mut bytes = Vec::new();
    limited.read_to_end(&mut bytes).map_err(|e| DgmError::RuntimeError { msg: format!("{context}: {}", e) })?;
    if bytes.len() as u64 > max_bytes {
        return Err(DgmError::RuntimeError { msg: format!("{context}: file exceeds {} bytes", max_bytes) });
    }
    String::from_utf8(bytes).map_err(|_| DgmError::RuntimeError { msg: format!("{context}: file is not valid UTF-8") })
}

fn io_read_file(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(p)) => read_text_file_limited(p, "read_file", DEFAULT_FILE_READ_LIMIT_BYTES).map(DgmValue::Str),
        _ => Err(DgmError::RuntimeError { msg: "read_file(path) required".into() }),
    }
}
fn io_write_file(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (a.get(0), a.get(1)) { (Some(DgmValue::Str(p)), Some(DgmValue::Str(c))) => std::fs::write(p, c).map(|_| DgmValue::Null).map_err(|e| DgmError::RuntimeError { msg: format!("write_file: {}", e) }), _ => Err(DgmError::RuntimeError { msg: "write_file(path, content) required".into() }) }
}
fn io_append_file(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    use std::io::Write;
    match (a.get(0), a.get(1)) { (Some(DgmValue::Str(p)), Some(DgmValue::Str(c))) => { let mut f = std::fs::OpenOptions::new().append(true).create(true).open(p).map_err(|e| DgmError::RuntimeError { msg: format!("append: {}", e) })?; f.write_all(c.as_bytes()).map_err(|e| DgmError::RuntimeError { msg: format!("append: {}", e) })?; Ok(DgmValue::Null) } _ => Err(DgmError::RuntimeError { msg: "append_file(path, content) required".into() }) }
}
fn io_exists(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Str(p)) => Ok(DgmValue::Bool(std::path::Path::new(p).exists())), _ => Err(DgmError::RuntimeError { msg: "exists(path) required".into() }) }
}
fn io_delete(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Str(p)) => { let path = std::path::Path::new(p); if path.is_dir() { std::fs::remove_dir_all(p).map_err(|e| DgmError::RuntimeError { msg: format!("delete: {}", e) })?; } else { std::fs::remove_file(p).map_err(|e| DgmError::RuntimeError { msg: format!("delete: {}", e) })?; } Ok(DgmValue::Null) } _ => Err(DgmError::RuntimeError { msg: "delete(path) required".into() }) }
}
fn io_mkdir(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Str(p)) => std::fs::create_dir_all(p).map(|_| DgmValue::Null).map_err(|e| DgmError::RuntimeError { msg: format!("mkdir: {}", e) }), _ => Err(DgmError::RuntimeError { msg: "mkdir(path) required".into() }) }
}
fn io_list_dir(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(p)) => {
            let entries: Vec<DgmValue> = std::fs::read_dir(p).map_err(|e| DgmError::RuntimeError { msg: format!("list_dir: {}", e) })?.filter_map(|e| e.ok().map(|e| DgmValue::Str(e.file_name().to_string_lossy().to_string()))).collect();
            Ok(DgmValue::List(Rc::new(RefCell::new(entries))))
        } _ => Err(DgmError::RuntimeError { msg: "list_dir(path) required".into() })
    }
}
fn io_cwd(_a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    std::env::current_dir().map(|p| DgmValue::Str(p.to_string_lossy().to_string())).map_err(|e| DgmError::RuntimeError { msg: format!("cwd: {}", e) })
}
fn io_input(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if let Some(DgmValue::Str(prompt)) = a.first() { print!("{}", prompt); use std::io::Write; std::io::stdout().flush().ok(); }
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).map_err(|e| DgmError::RuntimeError { msg: format!("input: {}", e) })?;
    Ok(DgmValue::Str(line.trim().to_string()))
}
fn io_read_lines(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(p)) => {
            let content = read_text_file_limited(p, "read_lines", DEFAULT_FILE_READ_LIMIT_BYTES)?;
            let lines: Vec<DgmValue> = content.lines().map(|l| DgmValue::Str(l.to_string())).collect();
            Ok(DgmValue::List(Rc::new(RefCell::new(lines))))
        }
        _ => Err(DgmError::RuntimeError { msg: "read_lines(path) required".into() }),
    }
}
fn io_file_size(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Str(p)) => std::fs::metadata(p).map(|m| DgmValue::Int(m.len() as i64)).map_err(|e| DgmError::RuntimeError { msg: format!("file_size: {}", e) }), _ => Err(DgmError::RuntimeError { msg: "file_size(path) required".into() }) }
}
fn io_is_dir(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Str(p)) => Ok(DgmValue::Bool(std::path::Path::new(p).is_dir())), _ => Err(DgmError::RuntimeError { msg: "is_dir(path) required".into() }) }
}
fn io_is_file(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Str(p)) => Ok(DgmValue::Bool(std::path::Path::new(p).is_file())), _ => Err(DgmError::RuntimeError { msg: "is_file(path) required".into() }) }
}
fn io_rename(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (a.get(0), a.get(1)) { (Some(DgmValue::Str(f)), Some(DgmValue::Str(t))) => std::fs::rename(f, t).map(|_| DgmValue::Null).map_err(|e| DgmError::RuntimeError { msg: format!("rename: {}", e) }), _ => Err(DgmError::RuntimeError { msg: "rename(from, to) required".into() }) }
}
fn io_copy(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (a.get(0), a.get(1)) { (Some(DgmValue::Str(f)), Some(DgmValue::Str(t))) => std::fs::copy(f, t).map(|_| DgmValue::Null).map_err(|e| DgmError::RuntimeError { msg: format!("copy: {}", e) }), _ => Err(DgmError::RuntimeError { msg: "copy(from, to) required".into() }) }
}
fn io_abs_path(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() { Some(DgmValue::Str(p)) => std::fs::canonicalize(p).map(|p| DgmValue::Str(p.to_string_lossy().to_string())).map_err(|e| DgmError::RuntimeError { msg: format!("abs_path: {}", e) }), _ => Err(DgmError::RuntimeError { msg: "abs_path(path) required".into() }) }
}
