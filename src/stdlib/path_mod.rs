use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use crate::error::DgmError;
use crate::interpreter::DgmValue;

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("join", path_join),
        ("dirname", path_dirname),
        ("basename", path_basename),
        ("stem", path_stem),
        ("ext", path_ext),
        ("normalize", path_normalize),
        ("exists", path_exists),
        ("is_abs", path_is_abs),
        ("safe_join", path_safe_join),
        ("within", path_within),
    ];
    for (name, func) in fns {
        m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("path.{}", name), func: *func });
    }
    m
}

fn normalize_lexical(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(std::path::MAIN_SEPARATOR.to_string()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !path.is_absolute() {
                    normalized.push("..");
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    if normalized.as_os_str().is_empty() && path.is_absolute() {
        normalized.push(std::path::MAIN_SEPARATOR.to_string());
    }
    normalized
}

fn absolutize_lexical(path: &Path) -> Result<PathBuf, DgmError> {
    let base = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| DgmError::RuntimeError { msg: format!("path: {}", e) })?
            .join(path)
    };
    Ok(normalize_lexical(&base))
}

fn safe_join_impl(root: &str, child: &str) -> Result<PathBuf, DgmError> {
    let child_path = Path::new(child);
    if child_path.is_absolute() {
        return Err(DgmError::RuntimeError { msg: "path.safe_join: absolute child path is not allowed".into() });
    }
    let root_abs = absolutize_lexical(Path::new(root))?;
    let joined_abs = absolutize_lexical(&root_abs.join(child_path))?;
    if !joined_abs.starts_with(&root_abs) {
        return Err(DgmError::RuntimeError { msg: "path.safe_join: path traversal detected".into() });
    }
    Ok(joined_abs)
}

fn path_join(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    if args.is_empty() {
        return Err(DgmError::RuntimeError { msg: "path.join(...parts) requires args".into() });
    }
    let mut path = PathBuf::new();
    for arg in args {
        match arg {
            DgmValue::Str(part) => path.push(part),
            other => return Err(DgmError::RuntimeError { msg: format!("path.join expects strings, got {}", other) }),
        }
    }
    Ok(DgmValue::Str(path.to_string_lossy().to_string()))
}

fn path_dirname(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(path)) => Ok(DgmValue::Str(Path::new(path).parent().map(|p| p.to_string_lossy().to_string()).unwrap_or_default())),
        _ => Err(DgmError::RuntimeError { msg: "path.dirname(path) required".into() }),
    }
}

fn path_basename(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(path)) => Ok(DgmValue::Str(Path::new(path).file_name().and_then(|s| s.to_str()).unwrap_or("").to_string())),
        _ => Err(DgmError::RuntimeError { msg: "path.basename(path) required".into() }),
    }
}

fn path_stem(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(path)) => Ok(DgmValue::Str(Path::new(path).file_stem().and_then(|s| s.to_str()).unwrap_or("").to_string())),
        _ => Err(DgmError::RuntimeError { msg: "path.stem(path) required".into() }),
    }
}

fn path_ext(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(path)) => Ok(DgmValue::Str(Path::new(path).extension().and_then(|s| s.to_str()).unwrap_or("").to_string())),
        _ => Err(DgmError::RuntimeError { msg: "path.ext(path) required".into() }),
    }
}

fn path_normalize(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(path)) => {
            let p = PathBuf::from(path);
            let normalized = if p.exists() { std::fs::canonicalize(&p).unwrap_or(p) } else { normalize_lexical(&p) };
            Ok(DgmValue::Str(normalized.to_string_lossy().to_string()))
        }
        _ => Err(DgmError::RuntimeError { msg: "path.normalize(path) required".into() }),
    }
}

fn path_exists(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(path)) => Ok(DgmValue::Bool(Path::new(path).exists())),
        _ => Err(DgmError::RuntimeError { msg: "path.exists(path) required".into() }),
    }
}

fn path_is_abs(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Str(path)) => Ok(DgmValue::Bool(Path::new(path).is_absolute())),
        _ => Err(DgmError::RuntimeError { msg: "path.is_abs(path) required".into() }),
    }
}

fn path_safe_join(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(root)), Some(DgmValue::Str(child))) => Ok(DgmValue::Str(safe_join_impl(root, child)?.to_string_lossy().to_string())),
        _ => Err(DgmError::RuntimeError { msg: "path.safe_join(root, child) required".into() }),
    }
}

fn path_within(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Str(root)), Some(DgmValue::Str(candidate))) => {
            let root_abs = absolutize_lexical(Path::new(root))?;
            let candidate_path = if Path::new(candidate).is_absolute() {
                absolutize_lexical(Path::new(candidate))?
            } else {
                absolutize_lexical(&root_abs.join(candidate))?
            };
            Ok(DgmValue::Bool(candidate_path.starts_with(&root_abs)))
        }
        _ => Err(DgmError::RuntimeError { msg: "path.within(root, path) required".into() }),
    }
}
