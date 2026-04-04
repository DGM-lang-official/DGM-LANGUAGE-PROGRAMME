use std::sync::{Arc, Mutex, OnceLock};
use std::collections::HashMap;
use std::cell::RefCell;
use std::fs;
use std::rc::Rc;
use std::mem;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use crate::interpreter::{
    json_resolved_value, json_value_from_root, runtime_reserve_labeled, runtime_reserve_value_labeled, DgmValue,
    RawJsonValue,
};
use crate::error::DgmError;
use crate::request_scope::track_raw_json_ref;
use crate::stdlib::io_mod::{read_text_file_limited, DEFAULT_FILE_READ_LIMIT_BYTES};

const DEFAULT_JSON_INPUT_LIMIT_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_JSON_DEPTH_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct JsonFileSignature {
    len: u64,
    modified_millis: u128,
}

#[derive(Debug, Clone)]
struct CachedJsonFile {
    signature: JsonFileSignature,
    text: Arc<str>,
    root: Arc<serde_json::Value>,
}

static JSON_FILE_CACHE: OnceLock<Mutex<HashMap<String, CachedJsonFile>>> = OnceLock::new();
static JSON_FILE_LOCKS: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("parse", json_parse), ("stringify", json_stringify), ("pretty", json_pretty),
        ("read_file_cached", json_read_file_cached), ("materialize", json_materialize),
        ("read_file_text_cached", json_read_file_text_cached), ("read_file_raw_cached", json_read_file_raw_cached),
        ("raw", json_raw), ("raw_trusted", json_raw_trusted), ("raw_parts", json_raw_parts),
        ("invalidate_file_cache", json_invalidate_file_cache), ("append_to_array_file", json_append_to_array_file),
    ];
    for (name, func) in fns { m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("json.{}", name), func: *func }); }
    m
}

fn json_file_cache() -> &'static Mutex<HashMap<String, CachedJsonFile>> {
    JSON_FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn json_file_locks() -> &'static Mutex<HashMap<String, Arc<Mutex<()>>>> {
    JSON_FILE_LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn json_file_lock(key: &str) -> Arc<Mutex<()>> {
    let mut locks = json_file_locks().lock().unwrap();
    Arc::clone(
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

fn json_cache_key(path: &str) -> String {
    fs::canonicalize(path)
        .unwrap_or_else(|_| PathBuf::from(path))
        .to_string_lossy()
        .to_string()
}

fn json_file_signature(path: &str) -> Result<JsonFileSignature, DgmError> {
    let meta = fs::metadata(path).map_err(|err| DgmError::RuntimeError { msg: format!("json.read_file_cached: {}", err) })?;
    let modified = meta
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    Ok(JsonFileSignature {
        len: meta.len(),
        modified_millis: modified,
    })
}

fn write_atomic_text(path: &str, content: &str, context: &str) -> Result<(), DgmError> {
    let target = PathBuf::from(path);
    let parent = target.parent().map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("data.json");
    let tmp = parent.join(format!(".{}.tmp-{}", file_name, std::process::id()));
    fs::write(&tmp, content)
        .map_err(|err| DgmError::RuntimeError { msg: format!("{context}: {}", err) })?;
    fs::rename(&tmp, &target)
        .map_err(|err| DgmError::RuntimeError { msg: format!("{context}: {}", err) })?;
    Ok(())
}

fn load_cached_json_file(path: &str) -> Result<CachedJsonFile, DgmError> {
    let key = json_cache_key(path);
    let signature = json_file_signature(path)?;
    if let Some(entry) = json_file_cache()
        .lock()
        .unwrap()
        .get(&key)
        .filter(|entry| entry.signature == signature)
        .cloned()
    {
        return Ok(entry);
    }
    let text = read_text_file_limited(path, "json.read_file_cached", DEFAULT_FILE_READ_LIMIT_BYTES)?;
    let root = Arc::new(parse_json_limited(&text, "json.read_file_cached")?);
    let entry = CachedJsonFile {
        signature,
        text: Arc::<str>::from(text),
        root,
    };
    json_file_cache().lock().unwrap().insert(
        key,
        entry.clone(),
    );
    Ok(entry)
}

pub(crate) fn json_to_dgm(val: &serde_json::Value) -> DgmValue {
    match val {
        serde_json::Value::Null => DgmValue::Null,
        serde_json::Value::Bool(b) => DgmValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() { DgmValue::Int(i) }
            else { DgmValue::Float(n.as_f64().unwrap_or(0.0)) }
        }
        serde_json::Value::String(s) => DgmValue::Str(s.clone()),
        serde_json::Value::Array(arr) => DgmValue::List(Rc::new(RefCell::new(arr.iter().map(json_to_dgm).collect()))),
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj { map.insert(k.clone(), json_to_dgm(v)); }
            DgmValue::Map(Rc::new(RefCell::new(map)))
        }
    }
}

struct JsonValueRef<'a>(&'a DgmValue);

impl Serialize for JsonValueRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            DgmValue::Null => serializer.serialize_unit(),
            DgmValue::Bool(value) => serializer.serialize_bool(*value),
            DgmValue::Int(value) => serializer.serialize_i64(*value),
            DgmValue::Float(value) => serializer.serialize_f64(*value),
            DgmValue::Str(value) => serializer.serialize_str(value),
            DgmValue::List(items) => {
                let items = items.borrow();
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items.iter() {
                    seq.serialize_element(&JsonValueRef(item))?;
                }
                seq.end()
            }
            DgmValue::Map(map) => {
                let map = map.borrow();
                let mut entries = map.iter().collect::<Vec<_>>();
                entries.sort_by(|a, b| a.0.cmp(b.0));
                let mut ser = serializer.serialize_map(Some(entries.len()))?;
                for (key, value) in entries {
                    ser.serialize_entry(key, &JsonValueRef(value))?;
                }
                ser.end()
            }
            DgmValue::Request(request) => {
                let mut ser = serializer.serialize_map(Some(8))?;
                ser.serialize_entry("body", request.body_text())?;
                ser.serialize_entry("headers", &StringMapRef(request.headers_values()))?;
                ser.serialize_entry("json", "<native http.request.json>")?;
                ser.serialize_entry("method", &request.method)?;
                ser.serialize_entry("params", &StringMapRef(request.params_values()))?;
                ser.serialize_entry("path", &request.path)?;
                ser.serialize_entry("query", &StringMapRef(request.query_values()))?;
                ser.serialize_entry("url", &request.url)?;
                ser.end()
            }
            DgmValue::RequestShell(request) => {
                let mut ser = serializer.serialize_map(Some(8))?;
                ser.serialize_entry("body", request.body_text())?;
                ser.serialize_entry("headers", &StringMapRef(request.headers_values()))?;
                ser.serialize_entry("json", "<native http.request.json>")?;
                ser.serialize_entry("method", &request.backing().method)?;
                ser.serialize_entry("params", &StringMapRef(request.params_values()))?;
                ser.serialize_entry("path", &request.backing().path)?;
                ser.serialize_entry("query", &StringMapRef(request.query_values()))?;
                ser.serialize_entry("url", &request.backing().url)?;
                ser.end()
            }
            DgmValue::RequestMap(map) => StringMapRef(map.values.as_ref()).serialize(serializer),
            DgmValue::RequestMapView(values) => StringMapRef(values.as_ref()).serialize(serializer),
            DgmValue::RawJson(text) => match serde_json::from_str::<serde_json::Value>(&text.to_string_lossy()) {
                Ok(value) => value.serialize(serializer),
                Err(_) => serializer.serialize_str(&text.to_string_lossy()),
            },
            DgmValue::Json(state) => json_resolved_value(state).serialize(serializer),
            _ => serializer.serialize_str(&format!("{}", self.0)),
        }
    }
}

struct StringMapRef<'a>(&'a HashMap<String, String>);

impl Serialize for StringMapRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut entries = self.0.iter().collect::<Vec<_>>();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let mut ser = serializer.serialize_map(Some(entries.len()))?;
        for (key, value) in entries {
            ser.serialize_entry(key, value)?;
        }
        ser.end()
    }
}

pub(crate) fn dgm_to_json_string(val: &DgmValue) -> Result<String, DgmError> {
    serde_json::to_string(&JsonValueRef(val))
        .map_err(|err| DgmError::RuntimeError { msg: format!("json stringify failed: {}", err) })
}

pub(crate) fn encode_dgm_json_into(val: &DgmValue, output: &mut Vec<u8>) -> Result<(), DgmError> {
    serde_json::to_writer(output, &JsonValueRef(val))
        .map_err(|err| DgmError::RuntimeError { msg: format!("json stringify failed: {}", err) })
}

pub(crate) fn dgm_to_pretty_json_string(val: &DgmValue) -> Result<String, DgmError> {
    serde_json::to_string_pretty(&JsonValueRef(val))
        .map_err(|err| DgmError::RuntimeError { msg: format!("json pretty failed: {}", err) })
}

fn json_depth(val: &serde_json::Value) -> usize {
    match val {
        serde_json::Value::Array(items) => 1 + items.iter().map(json_depth).max().unwrap_or(0),
        serde_json::Value::Object(map) => 1 + map.values().map(json_depth).max().unwrap_or(0),
        _ => 1,
    }
}

pub(crate) fn parse_json_limited(input: &str, context: &str) -> Result<serde_json::Value, DgmError> {
    if input.len() > DEFAULT_JSON_INPUT_LIMIT_BYTES {
        return Err(DgmError::RuntimeError { msg: format!("{context}: input exceeds {} bytes", DEFAULT_JSON_INPUT_LIMIT_BYTES) });
    }
    let val: serde_json::Value = serde_json::from_str(input).map_err(|e| DgmError::RuntimeError { msg: format!("{context}: {}", e) })?;
    if json_depth(&val) > DEFAULT_JSON_DEPTH_LIMIT {
        return Err(DgmError::RuntimeError { msg: format!("{context}: nesting exceeds {}", DEFAULT_JSON_DEPTH_LIMIT) });
    }
    Ok(val)
}

fn json_parse(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(s)) => {
            let val = parse_json_limited(s, "json.parse")?;
            let out = json_to_dgm(&val);
            runtime_reserve_value_labeled("json.parse", &out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "json.parse(str) required".into() }),
    }
}

fn json_stringify(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::RawJson(text)) => Ok(DgmValue::Str(text.to_string_lossy())),
        Some(val) => {
            let text = dgm_to_json_string(val)?;
            runtime_reserve_labeled("json.stringify", mem::size_of::<String>() + text.len())?;
            Ok(DgmValue::Str(text))
        }
        None => Err(DgmError::RuntimeError { msg: "json.stringify(val) required".into() }),
    }
}

fn json_pretty(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::RawJson(text)) => {
            let rendered = text.to_string_lossy();
            let value = parse_json_limited(&rendered, "json.pretty")?;
            let pretty = serde_json::to_string_pretty(&value)
                .map_err(|err| DgmError::RuntimeError { msg: format!("json pretty failed: {}", err) })?;
            runtime_reserve_labeled("json.pretty", mem::size_of::<String>() + pretty.len())?;
            Ok(DgmValue::Str(pretty))
        }
        Some(val) => {
            let text = dgm_to_pretty_json_string(val)?;
            runtime_reserve_labeled("json.pretty", mem::size_of::<String>() + text.len())?;
            Ok(DgmValue::Str(text))
        }
        None => Err(DgmError::RuntimeError { msg: "json.pretty(val) required".into() }),
    }
}

fn json_read_file_cached(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(path)) => Ok(json_value_from_root(load_cached_json_file(path)?.root)),
        _ => Err(DgmError::RuntimeError { msg: "json.read_file_cached(path) required".into() }),
    }
}

fn json_read_file_text_cached(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(path)) => Ok(DgmValue::Str(load_cached_json_file(path)?.text.to_string())),
        _ => Err(DgmError::RuntimeError { msg: "json.read_file_text_cached(path) required".into() }),
    }
}

fn raw_json_from_text_with_label(text: &str, label: &str) -> Result<DgmValue, DgmError> {
    runtime_reserve_labeled(label, std::mem::size_of::<RawJsonValue>() + text.len())?;
    let raw = Arc::new(RawJsonValue::from_text(text.to_string()));
    track_raw_json_ref(&raw);
    Ok(DgmValue::RawJson(raw))
}

fn json_read_file_raw_cached(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(path)) => {
            let raw = Arc::new(RawJsonValue::from_arc(load_cached_json_file(path)?.text));
            track_raw_json_ref(&raw);
            Ok(DgmValue::RawJson(raw))
        }
        _ => Err(DgmError::RuntimeError { msg: "json.read_file_raw_cached(path) required".into() }),
    }
}

fn json_raw(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(text)) => {
            let _ = parse_json_limited(text, "json.raw")?;
            raw_json_from_text_with_label(text, "json.raw")
        }
        Some(DgmValue::RawJson(text)) => Ok(DgmValue::RawJson(Arc::clone(text))),
        _ => Err(DgmError::RuntimeError { msg: "json.raw(text) requires json string".into() }),
    }
}

fn json_raw_trusted(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(text)) => raw_json_from_text_with_label(text, "json.raw_trusted"),
        Some(DgmValue::RawJson(text)) => Ok(DgmValue::RawJson(Arc::clone(text))),
        _ => Err(DgmError::RuntimeError { msg: "json.raw_trusted(text) requires json string".into() }),
    }
}

fn json_raw_parts(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let parts = if a.len() == 1 {
        match a.first() {
            Some(DgmValue::List(items)) => items.borrow().clone(),
            _ => a,
        }
    } else {
        a
    };
    if parts.is_empty() {
        return Err(DgmError::RuntimeError { msg: "json.raw_parts(parts...) requires at least one part".into() });
    }
    if parts.len() == 1 {
        return match parts.into_iter().next().unwrap() {
            DgmValue::Str(text) => raw_json_from_text_with_label(&text, "json.raw_parts"),
            DgmValue::RawJson(raw) => Ok(DgmValue::RawJson(raw)),
            other => Err(DgmError::RuntimeError {
                msg: format!("json.raw_parts only accepts str/raw_json, got {}", other),
            }),
        };
    }
    let mut segments = Vec::new();
    let mut reserved_bytes = std::mem::size_of::<RawJsonValue>();
    for part in parts {
        match part {
            DgmValue::Str(text) => {
                reserved_bytes = reserved_bytes.saturating_add(std::mem::size_of::<Arc<str>>() + text.len());
                segments.push(Arc::<str>::from(text));
            }
            DgmValue::RawJson(raw) => {
                segments.extend(raw.segments().iter().cloned());
            }
            other => {
                return Err(DgmError::RuntimeError {
                    msg: format!("json.raw_parts only accepts str/raw_json, got {}", other),
                })
            }
        }
    }
    reserved_bytes = reserved_bytes.saturating_add(std::mem::size_of::<Vec<Arc<str>>>());
    reserved_bytes = reserved_bytes.saturating_add(segments.len().saturating_mul(std::mem::size_of::<Arc<str>>()));
    let raw = RawJsonValue::from_parts(segments);
    runtime_reserve_labeled("json.raw_parts", reserved_bytes)?;
    let raw = Arc::new(raw);
    track_raw_json_ref(&raw);
    Ok(DgmValue::RawJson(raw))
}

fn json_materialize(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Json(state)) => {
            let value = json_to_dgm(json_resolved_value(state));
            runtime_reserve_value_labeled("json.materialize", &value)?;
            Ok(value)
        }
        Some(value) => Ok(value.clone()),
        None => Err(DgmError::RuntimeError { msg: "json.materialize(value) required".into() }),
    }
}

fn json_invalidate_file_cache(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Str(path)) => {
            json_file_cache().lock().unwrap().remove(&json_cache_key(path));
            Ok(DgmValue::Null)
        }
        None => {
            json_file_cache().lock().unwrap().clear();
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "json.invalidate_file_cache(path?) requires string path".into() }),
    }
}

fn json_append_to_array_file(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (a.first(), a.get(1)) {
        (Some(DgmValue::Str(path)), Some(value)) => {
            let key = json_cache_key(path);
            let lock = json_file_lock(&key);
            let _guard = lock.lock().unwrap();
            let current = read_text_file_limited(path, "json.append_to_array_file", DEFAULT_FILE_READ_LIMIT_BYTES)?;
            let current = current.trim();
            let entry = dgm_to_json_string(value)?;
            let updated = if current == "[]" {
                format!("[{entry}]")
            } else {
                if !current.ends_with(']') {
                    return Err(DgmError::RuntimeError { msg: "json.append_to_array_file: file must contain a json array".into() });
                }
                let prefix = &current[..current.len() - 1];
                format!("{prefix},{entry}]")
            };
            write_atomic_text(path, &updated, "json.append_to_array_file")?;
            json_file_cache().lock().unwrap().remove(&key);
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "json.append_to_array_file(path, value) required".into() }),
    }
}
