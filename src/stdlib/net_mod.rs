use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;

use crate::concurrency::schedule_named_future;
use crate::error::DgmError;
use crate::interpreter::{runtime_check_limits, runtime_close_socket, runtime_open_socket, runtime_record_io, runtime_reserve_value, DgmValue};
use crate::io_runtime;

static SOCKETS: OnceLock<Mutex<HashMap<i64, Arc<AsyncMutex<TcpStream>>>>> = OnceLock::new();
static NEXT_ID: OnceLock<Mutex<i64>> = OnceLock::new();
const DEFAULT_SOCKET_TIMEOUT_MS: u64 = 5_000;

fn get_sockets() -> &'static Mutex<HashMap<i64, Arc<AsyncMutex<TcpStream>>>> {
    SOCKETS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn next_socket_id() -> i64 {
    let m = NEXT_ID.get_or_init(|| Mutex::new(1));
    let mut id = m.lock().unwrap();
    let v = *id;
    *id += 1;
    v
}

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("connect", net_connect),
        ("send", net_send),
        ("recv", net_recv),
        ("close", net_close),
        ("listen", net_listen),
    ];
    for (name, func) in fns {
        m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("net.{}", name), func: *func });
    }
    m
}

pub(crate) fn try_schedule_async_call(callee: &DgmValue, args: Vec<DgmValue>) -> Result<Option<DgmValue>, DgmError> {
    match callee {
        DgmValue::NativeFunction { name, .. } if name == "net.connect" => match (args.first(), args.get(1)) {
            (Some(DgmValue::Str(host)), Some(DgmValue::Int(port))) => {
                let host = host.clone();
                let port = *port;
                let label = format!("net.connect {}:{}", host, port);
                schedule_named_future("net", label, move || net_connect(vec![DgmValue::Str(host), DgmValue::Int(port)])).map(Some)
            }
            _ => Err(DgmError::RuntimeError { msg: "net.connect(host, port) required".into() }),
        },
        DgmValue::NativeFunction { name, .. } if name == "net.send" => match (args.first(), args.get(1)) {
            (Some(DgmValue::Int(id)), Some(DgmValue::Str(data))) => {
                let id = *id;
                let data = data.clone();
                let label = format!("net.send {}", id);
                schedule_named_future("net", label, move || net_send(vec![DgmValue::Int(id), DgmValue::Str(data)])).map(Some)
            }
            _ => Err(DgmError::RuntimeError { msg: "net.send(socket, data) required".into() }),
        },
        DgmValue::NativeFunction { name, .. } if name == "net.recv" => {
            let id = match args.first() {
                Some(DgmValue::Int(id)) => *id,
                _ => return Err(DgmError::RuntimeError { msg: "net.recv(socket) required".into() }),
            };
            let bufsize = match args.get(1) {
                Some(DgmValue::Int(size)) => *size,
                _ => 4096,
            };
            let label = format!("net.recv {}", id);
            schedule_named_future(
                "net",
                label,
                move || net_recv(vec![DgmValue::Int(id), DgmValue::Int(bufsize)]),
            )
            .map(Some)
        }
        DgmValue::NativeFunction { name, .. } if name == "net.listen" => {
            let (host, port) = match (args.first(), args.get(1)) {
                (Some(DgmValue::Str(host)), Some(DgmValue::Int(port))) => (host.clone(), *port),
                _ => return Err(DgmError::RuntimeError { msg: "net.listen(host, port) required".into() }),
            };
            let label = format!("net.listen {}:{}", host, port);
            schedule_named_future(
                "net",
                label,
                move || net_listen(vec![DgmValue::Str(host), DgmValue::Int(port)]),
            )
            .map(Some)
        }
        _ => Ok(None),
    }
}

fn net_connect(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (a.first(), a.get(1)) {
        (Some(DgmValue::Str(host)), Some(DgmValue::Int(port))) => {
            let host = host.clone();
            let port = *port as u16;
            let started = Instant::now();
            let stream = io_runtime::block_on(async move {
                tokio::time::timeout(
                    std::time::Duration::from_millis(DEFAULT_SOCKET_TIMEOUT_MS),
                    TcpStream::connect(format!("{}:{}", host, port)),
                )
                    .await
                    .map_err(|_| DgmError::RuntimeError { msg: "net.connect: timed out".into() })?
                    .map_err(|e| DgmError::RuntimeError { msg: format!("net.connect: {}", e) })
            })?;
            runtime_record_io(started.elapsed().as_nanos());
            runtime_open_socket()?;
            let id = next_socket_id();
            get_sockets().lock().unwrap().insert(id, Arc::new(AsyncMutex::new(stream)));
            Ok(DgmValue::Int(id))
        }
        _ => Err(DgmError::RuntimeError { msg: "net.connect(host, port) required".into() }),
    }
}

fn net_send(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (a.first(), a.get(1)) {
        (Some(DgmValue::Int(id)), Some(DgmValue::Str(data))) => {
            runtime_check_limits()?;
            let socket = get_socket(*id)?;
            let data = data.clone();
            let len = data.len();
            let started = Instant::now();
            io_runtime::block_on(async move {
                let mut stream = socket.lock().await;
                tokio::time::timeout(
                    std::time::Duration::from_millis(DEFAULT_SOCKET_TIMEOUT_MS),
                    stream.write_all(data.as_bytes()),
                )
                    .await
                    .map_err(|_| DgmError::RuntimeError { msg: "net.send: timed out".into() })?
                    .map_err(|e| DgmError::RuntimeError { msg: format!("net.send: {}", e) })
            })?;
            runtime_record_io(started.elapsed().as_nanos());
            Ok(DgmValue::Int(len as i64))
        }
        _ => Err(DgmError::RuntimeError { msg: "net.send(socket, data) required".into() }),
    }
}

fn net_recv(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    let bufsize = match a.get(1) {
        Some(DgmValue::Int(n)) if *n > 0 => *n as usize,
        Some(DgmValue::Int(_)) => return Err(DgmError::RuntimeError { msg: "net.recv(socket, size?) requires size > 0".into() }),
        _ => 4096,
    };
    match a.first() {
        Some(DgmValue::Int(id)) => {
            runtime_check_limits()?;
            let socket = get_socket(*id)?;
            let started = Instant::now();
            let text = io_runtime::block_on(async move {
                let mut stream = socket.lock().await;
                let mut buf = vec![0u8; bufsize.max(1)];
                let n = tokio::time::timeout(
                    std::time::Duration::from_millis(DEFAULT_SOCKET_TIMEOUT_MS),
                    stream.read(&mut buf),
                )
                    .await
                    .map_err(|_| DgmError::RuntimeError { msg: "net.recv: timed out".into() })?
                    .map_err(|e| DgmError::RuntimeError { msg: format!("net.recv: {}", e) })?;
                Ok(String::from_utf8_lossy(&buf[..n]).to_string())
            })?;
            runtime_record_io(started.elapsed().as_nanos());
            Ok(DgmValue::Str(text))
        }
        _ => Err(DgmError::RuntimeError { msg: "net.recv(socket) required".into() }),
    }
}

fn net_close(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match a.first() {
        Some(DgmValue::Int(id)) => {
            if get_sockets().lock().unwrap().remove(id).is_some() {
                runtime_close_socket();
            }
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "net.close(socket) required".into() }),
    }
}

fn net_listen(a: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (a.first(), a.get(1)) {
        (Some(DgmValue::Str(host)), Some(DgmValue::Int(port))) => {
            let host = host.clone();
            let port = *port as u16;
            let started = Instant::now();
            let (stream, addr) = io_runtime::block_on(async move {
                let listener = TcpListener::bind(format!("{}:{}", host, port))
                    .await
                    .map_err(|e| DgmError::RuntimeError { msg: format!("net.listen: {}", e) })?;
                tokio::time::timeout(
                    std::time::Duration::from_millis(DEFAULT_SOCKET_TIMEOUT_MS),
                    listener.accept(),
                )
                    .await
                    .map_err(|_| DgmError::RuntimeError { msg: "net.listen: timed out".into() })?
                    .map_err(|e| DgmError::RuntimeError { msg: format!("net.listen: {}", e) })
            })?;
            runtime_record_io(started.elapsed().as_nanos());
            runtime_open_socket()?;
            let id = next_socket_id();
            get_sockets().lock().unwrap().insert(id, Arc::new(AsyncMutex::new(stream)));
            let mut result = HashMap::new();
            result.insert("socket".into(), DgmValue::Int(id));
            result.insert("addr".into(), DgmValue::Str(addr.to_string()));
            let value = DgmValue::Map(Rc::new(RefCell::new(result)));
            runtime_reserve_value(&value)?;
            Ok(value)
        }
        _ => Err(DgmError::RuntimeError { msg: "net.listen(host, port) required".into() }),
    }
}

fn get_socket(id: i64) -> Result<Arc<AsyncMutex<TcpStream>>, DgmError> {
    get_sockets()
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| DgmError::RuntimeError { msg: "invalid socket".into() })
}
