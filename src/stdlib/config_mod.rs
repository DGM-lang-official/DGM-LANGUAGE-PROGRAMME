use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use crate::error::DgmError;
use crate::interpreter::{runtime_reserve_value, DgmValue};
use crate::stdlib::io_mod::{read_text_file_limited, DEFAULT_FILE_READ_LIMIT_BYTES};
use crate::stdlib::json_mod::{json_to_dgm, parse_json_limited};

#[derive(Clone)]
struct SchemaSpec {
    type_name: String,
    optional: bool,
    default_value: Option<DgmValue>,
}

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("load_env", config_load_env),
        ("load_json", config_load_json),
        ("merge", config_merge),
        ("require", config_require),
        ("validate", config_validate),
        ("optional", config_optional),
        ("default", config_default),
    ];
    for (name, func) in fns {
        m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("config.{}", name), func: *func });
    }
    for type_name in ["int", "float", "string", "str", "bool", "list", "map", "any"] {
        m.insert(type_name.to_string(), schema_descriptor(type_name));
    }
    m
}

fn schema_descriptor(type_name: &str) -> DgmValue {
    let mut map = HashMap::new();
    map.insert("__schema_type".into(), DgmValue::Str(type_name.to_string()));
    DgmValue::Map(Rc::new(RefCell::new(map)))
}

fn schema_from_value(value: &DgmValue) -> Result<SchemaSpec, DgmError> {
    match value {
        DgmValue::Str(spec) => parse_schema_string(spec),
        DgmValue::NativeFunction { name, .. } => parse_schema_string(name.rsplit('.').next().unwrap_or(name)),
        DgmValue::Map(map) => {
            let map = map.borrow();
            let type_name = match map.get("__schema_type").or_else(|| map.get("type")) {
                Some(DgmValue::Str(name)) => normalize_type_name(name)?,
                Some(DgmValue::NativeFunction { name, .. }) => normalize_type_name(name.rsplit('.').next().unwrap_or(name))?,
                Some(other) => return Err(DgmError::RuntimeError { msg: format!("config schema type must be string/function, got {}", other) }),
                None => return Err(DgmError::RuntimeError { msg: "config schema descriptor requires 'type'".into() }),
            };
            let optional = matches!(map.get("optional"), Some(DgmValue::Bool(true)));
            let default_value = map.get("default").cloned();
            Ok(SchemaSpec { type_name, optional, default_value })
        }
        other => Err(DgmError::RuntimeError { msg: format!("invalid config schema {}", other) }),
    }
}

fn parse_schema_string(spec: &str) -> Result<SchemaSpec, DgmError> {
    let trimmed = spec.trim();
    let optional = trimmed.ends_with('?');
    let core = trimmed.trim_end_matches('?');
    Ok(SchemaSpec { type_name: normalize_type_name(core)?, optional, default_value: None })
}

fn normalize_type_name(name: &str) -> Result<String, DgmError> {
    let normalized = match name.trim().to_ascii_lowercase().as_str() {
        "str" | "string" => "string",
        "int" => "int",
        "float" => "float",
        "bool" => "bool",
        "list" => "list",
        "map" => "map",
        "any" => "any",
        "nul" | "null" => "null",
        other => return Err(DgmError::RuntimeError { msg: format!("unsupported config type '{}'", other) }),
    };
    Ok(normalized.to_string())
}

fn schema_to_value(spec: SchemaSpec) -> DgmValue {
    let mut map = HashMap::new();
    map.insert("__schema_type".into(), DgmValue::Str(spec.type_name));
    if spec.optional {
        map.insert("optional".into(), DgmValue::Bool(true));
    }
    if let Some(default_value) = spec.default_value {
        map.insert("default".into(), default_value);
    }
    DgmValue::Map(Rc::new(RefCell::new(map)))
}

fn coerce_value_for_schema(key: &str, value: DgmValue, schema: &SchemaSpec) -> Result<DgmValue, DgmError> {
    match schema.type_name.as_str() {
        "any" => Ok(value),
        "null" => match value {
            DgmValue::Null => Ok(DgmValue::Null),
            _ => Err(DgmError::RuntimeError { msg: format!("config key '{}' must be null", key) }),
        },
        "int" => match value {
            DgmValue::Int(_) => Ok(value),
            DgmValue::Float(f) if f.fract() == 0.0 => Ok(DgmValue::Int(f as i64)),
            DgmValue::Str(s) => s.trim().parse::<i64>().map(DgmValue::Int).map_err(|_| DgmError::RuntimeError { msg: format!("config key '{}' must be int", key) }),
            _ => Err(DgmError::RuntimeError { msg: format!("config key '{}' must be int", key) }),
        },
        "float" => match value {
            DgmValue::Float(_) => Ok(value),
            DgmValue::Int(n) => Ok(DgmValue::Float(n as f64)),
            DgmValue::Str(s) => s.trim().parse::<f64>().map(DgmValue::Float).map_err(|_| DgmError::RuntimeError { msg: format!("config key '{}' must be float", key) }),
            _ => Err(DgmError::RuntimeError { msg: format!("config key '{}' must be float", key) }),
        },
        "string" => match value {
            DgmValue::Str(_) => Ok(value),
            _ => Err(DgmError::RuntimeError { msg: format!("config key '{}' must be string", key) }),
        },
        "bool" => match value {
            DgmValue::Bool(_) => Ok(value),
            DgmValue::Int(0) => Ok(DgmValue::Bool(false)),
            DgmValue::Int(1) => Ok(DgmValue::Bool(true)),
            DgmValue::Str(s) => match s.trim().to_ascii_lowercase().as_str() {
                "tru" | "true" | "1" | "yes" | "on" => Ok(DgmValue::Bool(true)),
                "fals" | "false" | "0" | "no" | "off" => Ok(DgmValue::Bool(false)),
                _ => Err(DgmError::RuntimeError { msg: format!("config key '{}' must be bool", key) }),
            },
            _ => Err(DgmError::RuntimeError { msg: format!("config key '{}' must be bool", key) }),
        },
        "list" => match value {
            DgmValue::List(_) => Ok(value),
            _ => Err(DgmError::RuntimeError { msg: format!("config key '{}' must be list", key) }),
        },
        "map" => match value {
            DgmValue::Map(_) => Ok(value),
            _ => Err(DgmError::RuntimeError { msg: format!("config key '{}' must be map", key) }),
        },
        _ => Err(DgmError::RuntimeError { msg: format!("unsupported config type '{}'", schema.type_name) }),
    }
}

fn config_load_env(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let path = match args.first() {
        Some(DgmValue::Str(path)) => path,
        _ => return Err(DgmError::RuntimeError { msg: "config.load_env(path) required".into() }),
    };
    let content = read_text_file_limited(path, "config.load_env", DEFAULT_FILE_READ_LIMIT_BYTES)?;
    let mut map = HashMap::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else { continue; };
        let value = value.trim().trim_matches('"').trim_matches('\'').to_string();
        map.insert(key.trim().to_string(), DgmValue::Str(value));
    }
    let out = DgmValue::Map(Rc::new(RefCell::new(map)));
    runtime_reserve_value(&out)?;
    Ok(out)
}

fn config_load_json(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let path = match args.first() {
        Some(DgmValue::Str(path)) => path,
        _ => return Err(DgmError::RuntimeError { msg: "config.load_json(path) required".into() }),
    };
    let content = read_text_file_limited(path, "config.load_json", DEFAULT_FILE_READ_LIMIT_BYTES)?;
    let value = parse_json_limited(&content, "config.load_json")?;
    let out = json_to_dgm(&value);
    runtime_reserve_value(&out)?;
    Ok(out)
}

fn config_merge(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Map(a)), Some(DgmValue::Map(b))) => {
            let mut merged = a.borrow().clone();
            for (key, value) in b.borrow().iter() {
                merged.insert(key.clone(), value.clone());
            }
            let out = DgmValue::Map(Rc::new(RefCell::new(merged)));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "config.merge(map, map) required".into() }),
    }
}

fn config_require(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Map(map)), Some(DgmValue::Str(key))) => map.borrow().get(key).cloned().ok_or_else(|| DgmError::RuntimeError { msg: format!("missing config key '{}'", key) }),
        _ => Err(DgmError::RuntimeError { msg: "config.require(map, key) required".into() }),
    }
}

fn config_optional(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let schema = args.first().ok_or_else(|| DgmError::RuntimeError { msg: "config.optional(schema) required".into() })?;
    let mut spec = schema_from_value(schema)?;
    spec.optional = true;
    Ok(schema_to_value(spec))
}

fn config_default(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(schema), Some(default_value)) => {
            let mut spec = schema_from_value(schema)?;
            spec.default_value = Some(default_value.clone());
            Ok(schema_to_value(spec))
        }
        _ => Err(DgmError::RuntimeError { msg: "config.default(schema, value) required".into() }),
    }
}

fn config_validate(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Map(config)), Some(DgmValue::Map(schema_map))) => {
            let source = config.borrow();
            let schema_entries = schema_map.borrow();
            let mut output = source.clone();
            for (key, schema_value) in schema_entries.iter() {
                let schema = schema_from_value(schema_value)?;
                let value = match source.get(key).cloned() {
                    Some(value) => coerce_value_for_schema(key, value, &schema)?,
                    None => match &schema.default_value {
                        Some(default_value) => coerce_value_for_schema(key, default_value.clone(), &schema)?,
                        None if schema.optional => continue,
                        None => return Err(DgmError::RuntimeError { msg: format!("missing config key '{}'", key) }),
                    },
                };
                output.insert(key.clone(), value);
            }
            let out = DgmValue::Map(Rc::new(RefCell::new(output)));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "config.validate(config_map, schema_map) required".into() }),
    }
}
