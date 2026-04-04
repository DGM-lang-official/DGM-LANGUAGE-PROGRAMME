use crate::ast::Span;
use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::cell::Cell;
use std::sync::{Once, OnceLock, RwLock};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StackFrame {
    pub function: Option<String>,
    pub path: String,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub enum ThrownValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
    List(Vec<ThrownValue>),
    Map(BTreeMap<String, ThrownValue>),
    Opaque(String),
}

#[derive(Debug, Clone)]
pub enum DgmError {
    LexError {
        msg: String,
        line: usize,
        column: usize,
        path: Option<String>,
    },
    ParseError {
        msg: String,
        line: usize,
        column: usize,
        path: Option<String>,
    },
    RuntimeError {
        msg: String,
    },
    ThrownError {
        value: ThrownValue,
    },
    ImportError {
        msg: String,
    },
    BundleError {
        msg: String,
    },
    PermissionError {
        msg: String,
    },
    Structured {
        kind: &'static str,
        message: String,
        thrown_value: Option<ThrownValue>,
        stack: Vec<StackFrame>,
    },
}

static SOURCE_REGISTRY: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
static PANIC_HOOK_INIT: Once = Once::new();
static DEFAULT_PANIC_HOOK: OnceLock<PanicHook> = OnceLock::new();

type PanicHook = Box<dyn for<'a> Fn(&std::panic::PanicHookInfo<'a>) + Sync + Send + 'static>;

thread_local! {
    static PANIC_TRAP_DEPTH: Cell<usize> = const { Cell::new(0) };
}

impl DgmError {
    pub fn lex(msg: impl Into<String>, line: usize, column: usize) -> Self {
        Self::LexError {
            msg: msg.into(),
            line,
            column,
            path: None,
        }
    }

    pub fn parse(msg: impl Into<String>, line: usize, column: usize) -> Self {
        Self::ParseError {
            msg: msg.into(),
            line,
            column,
            path: None,
        }
    }

    pub fn runtime(msg: impl Into<String>) -> Self {
        Self::RuntimeError { msg: msg.into() }
    }

    pub fn import(msg: impl Into<String>) -> Self {
        Self::ImportError { msg: msg.into() }
    }

    pub fn bundle(msg: impl Into<String>) -> Self {
        Self::BundleError { msg: msg.into() }
    }

    pub fn permission(msg: impl Into<String>) -> Self {
        Self::PermissionError { msg: msg.into() }
    }

    pub fn structured(kind: &'static str, message: impl Into<String>) -> Self {
        Self::Structured {
            kind,
            message: message.into(),
            thrown_value: None,
            stack: vec![],
        }
    }

    pub fn panic_runtime(context: impl Into<String>, payload: Box<dyn Any + Send>) -> Self {
        let context = context.into();
        let detail = panic_payload_string(payload);
        let message = if detail.is_empty() {
            format!("internal panic in {}", context)
        } else {
            format!("internal panic in {}: {}", context, detail)
        };
        Self::Structured {
            kind: "RuntimeError",
            message,
            thrown_value: None,
            stack: vec![],
        }
    }

    pub fn with_frame(self, frame: StackFrame) -> Self {
        match self {
            Self::Structured {
                kind,
                message,
                thrown_value,
                mut stack,
            } => {
                if stack.last() != Some(&frame) {
                    stack.push(frame);
                }
                Self::Structured {
                    kind,
                    message,
                    thrown_value,
                    stack,
                }
            }
            Self::RuntimeError { msg } => Self::Structured {
                kind: "RuntimeError",
                message: msg,
                thrown_value: None,
                stack: vec![frame],
            },
            Self::ImportError { msg } => Self::Structured {
                kind: "ImportError",
                message: msg,
                thrown_value: None,
                stack: vec![frame],
            },
            Self::BundleError { msg } => Self::Structured {
                kind: "BundleError",
                message: msg,
                thrown_value: None,
                stack: vec![frame],
            },
            Self::PermissionError { msg } => Self::Structured {
                kind: "PermissionError",
                message: msg,
                thrown_value: None,
                stack: vec![frame],
            },
            Self::ThrownError { value } => Self::Structured {
                kind: "ThrownError",
                message: format!("{}", value),
                thrown_value: Some(value),
                stack: vec![frame],
            },
            Self::LexError {
                msg,
                line,
                column,
                path,
            } => {
                let frame = if path.is_some() {
                    frame
                } else {
                    StackFrame {
                        function: None,
                        path: frame.path,
                        span: Span {
                            start_line: line,
                            start_col: column,
                            end_line: line,
                            end_col: column,
                        },
                    }
                };
                Self::Structured {
                    kind: "LexError",
                    message: msg,
                    thrown_value: None,
                    stack: vec![frame],
                }
            }
            Self::ParseError {
                msg,
                line,
                column,
                path,
            } => {
                let frame = if path.is_some() {
                    frame
                } else {
                    StackFrame {
                        function: None,
                        path: frame.path,
                        span: Span {
                            start_line: line,
                            start_col: column,
                            end_line: line,
                            end_col: column,
                        },
                    }
                };
                Self::Structured {
                    kind: "ParseError",
                    message: msg,
                    thrown_value: None,
                    stack: vec![frame],
                }
            }
        }
    }

    pub fn with_path(self, path: impl Into<String>) -> Self {
        let path = path.into();
        match self {
            Self::LexError {
                msg,
                line,
                column,
                ..
            } => Self::Structured {
                kind: "LexError",
                message: msg,
                thrown_value: None,
                stack: vec![StackFrame {
                    function: None,
                    path,
                    span: Span {
                        start_line: line,
                        start_col: column,
                        end_line: line,
                        end_col: column,
                    },
                }],
            },
            Self::ParseError {
                msg,
                line,
                column,
                ..
            } => Self::Structured {
                kind: "ParseError",
                message: msg,
                thrown_value: None,
                stack: vec![StackFrame {
                    function: None,
                    path,
                    span: Span {
                        start_line: line,
                        start_col: column,
                        end_line: line,
                        end_col: column,
                    },
                }],
            },
            other => other,
        }
    }

    pub fn prefix_message(self, prefix: &str) -> Self {
        match self {
            Self::Structured {
                kind,
                message,
                thrown_value,
                stack,
            } => Self::Structured {
                kind,
                message: format!("{}{}", prefix, message),
                thrown_value,
                stack,
            },
            Self::RuntimeError { msg } => Self::Structured {
                kind: "RuntimeError",
                message: format!("{}{}", prefix, msg),
                thrown_value: None,
                stack: vec![],
            },
            Self::ImportError { msg } => Self::Structured {
                kind: "ImportError",
                message: format!("{}{}", prefix, msg),
                thrown_value: None,
                stack: vec![],
            },
            Self::BundleError { msg } => Self::Structured {
                kind: "BundleError",
                message: format!("{}{}", prefix, msg),
                thrown_value: None,
                stack: vec![],
            },
            Self::PermissionError { msg } => Self::Structured {
                kind: "PermissionError",
                message: format!("{}{}", prefix, msg),
                thrown_value: None,
                stack: vec![],
            },
            Self::ThrownError { value } => Self::Structured {
                kind: "RuntimeError",
                message: format!("{}{}", prefix, value),
                thrown_value: Some(value),
                stack: vec![],
            },
            Self::LexError {
                msg,
                line,
                column,
                path,
            } => Self::LexError {
                msg: format!("{}{}", prefix, msg),
                line,
                column,
                path,
            },
            Self::ParseError {
                msg,
                line,
                column,
                path,
            } => Self::ParseError {
                msg: format!("{}{}", prefix, msg),
                line,
                column,
                path,
            },
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::LexError { .. } => "LexError",
            Self::ParseError { .. } => "ParseError",
            Self::RuntimeError { .. } => "RuntimeError",
            Self::ThrownError { .. } => "ThrownError",
            Self::ImportError { .. } => "ImportError",
            Self::BundleError { .. } => "BundleError",
            Self::PermissionError { .. } => "PermissionError",
            Self::Structured { kind, .. } => kind,
        }
    }

    pub fn message_string(&self) -> String {
        match self {
            Self::LexError { msg, .. } => msg.clone(),
            Self::ParseError { msg, .. } => msg.clone(),
            Self::RuntimeError { msg } => msg.clone(),
            Self::ThrownError { value } => format!("{}", value),
            Self::ImportError { msg } => msg.clone(),
            Self::BundleError { msg } => msg.clone(),
            Self::PermissionError { msg } => msg.clone(),
            Self::Structured { message, .. } => message.clone(),
        }
    }

    pub fn thrown_value(&self) -> Option<ThrownValue> {
        match self {
            Self::ThrownError { value } => Some(value.clone()),
            Self::Structured { thrown_value, .. } => thrown_value.clone(),
            _ => None,
        }
    }

    pub fn thrown_value_string(&self) -> Option<String> {
        self.thrown_value().map(|value| format!("{}", value))
    }

    pub fn stack(&self) -> &[StackFrame] {
        match self {
            Self::Structured { stack, .. } => stack,
            _ => &[],
        }
    }
}

impl fmt::Display for DgmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LexError {
                msg,
                line,
                column,
                path,
            } => {
                write!(f, "LexError: {}", msg)?;
                if let Some(path) = path {
                    let frame = StackFrame {
                        function: None,
                        path: path.clone(),
                        span: Span {
                            start_line: *line,
                            start_col: *column,
                            end_line: *line,
                            end_col: *column,
                        },
                    };
                    write_frames(f, &[frame])?;
                } else {
                    write!(f, "\n  at <input>:{}:{}", line, column)?;
                }
                Ok(())
            }
            Self::ParseError {
                msg,
                line,
                column,
                path,
            } => {
                write!(f, "ParseError: {}", msg)?;
                if let Some(path) = path {
                    let frame = StackFrame {
                        function: None,
                        path: path.clone(),
                        span: Span {
                            start_line: *line,
                            start_col: *column,
                            end_line: *line,
                            end_col: *column,
                        },
                    };
                    write_frames(f, &[frame])?;
                } else {
                    write!(f, "\n  at <input>:{}:{}", line, column)?;
                }
                Ok(())
            }
            Self::RuntimeError { msg } => write!(f, "RuntimeError: {}", msg),
            Self::ThrownError { value } => write!(f, "ThrownError: {}", value),
            Self::ImportError { msg } => write!(f, "ImportError: {}", msg),
            Self::BundleError { msg } => write!(f, "BundleError: {}", msg),
            Self::PermissionError { msg } => write!(f, "PermissionError: {}", msg),
            Self::Structured {
                kind,
                message,
                stack,
                ..
            } => {
                write!(f, "{}: {}", kind, message)?;
                write_frames(f, stack)
            }
        }
    }
}

impl std::error::Error for DgmError {}

impl fmt::Display for ThrownValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(value) => write!(f, "{}", value),
            Self::Float(value) => write!(f, "{}", value),
            Self::Str(value) => write!(f, "{}", value),
            Self::Bool(value) => write!(f, "{}", if *value { "tru" } else { "fals" }),
            Self::Null => write!(f, "nul"),
            Self::List(items) => {
                let items = items.iter().map(|item| item.to_string()).collect::<Vec<_>>().join(", ");
                write!(f, "[{}]", items)
            }
            Self::Map(entries) => {
                let items = entries
                    .iter()
                    .map(|(key, value)| format!("{}: {}", key, value))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{{{}}}", items)
            }
            Self::Opaque(value) => write!(f, "{}", value),
        }
    }
}

pub fn trap_dgm<T, F>(context: &str, f: F) -> Result<T, DgmError>
where
    F: FnOnce() -> Result<T, DgmError>,
{
    install_panic_hook();
    PANIC_TRAP_DEPTH.with(|depth| {
        let current = depth.get();
        depth.set(current + 1);
    });
    let result = catch_unwind(AssertUnwindSafe(f));
    PANIC_TRAP_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    match result {
        Ok(result) => result,
        Err(payload) => Err(DgmError::panic_runtime(context, payload)),
    }
}

pub fn register_source(path: impl Into<String>, source: impl Into<String>) {
    let path = path.into();
    let source = source.into();
    if path.is_empty() {
        return;
    }
    let registry = SOURCE_REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    if let Ok(mut registry) = registry.write() {
        registry.insert(path, source);
    }
}

fn write_frames(f: &mut fmt::Formatter<'_>, stack: &[StackFrame]) -> fmt::Result {
    for (index, frame) in stack.iter().rev().enumerate() {
        write!(f, "\n  at ")?;
        if let Some(function) = &frame.function {
            write!(f, "{}() ", function)?;
        }
        write!(
            f,
            "{}:{}:{}",
            frame.path,
            frame.span.start_line,
            frame.span.start_col
        )?;
        if index == 0 {
            write_source_snippet(f, frame)?;
        }
    }
    Ok(())
}

fn write_source_snippet(f: &mut fmt::Formatter<'_>, frame: &StackFrame) -> fmt::Result {
    let Some(source) = source_for_path(&frame.path) else {
        write!(f, "\n    source unavailable")?;
        return Ok(());
    };
    let lines = source.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        write!(f, "\n    source unavailable")?;
        return Ok(());
    }
    let current_line = frame.span.start_line.max(1).min(lines.len());
    let start_line = current_line.saturating_sub(1).max(1);
    let width = current_line.to_string().len().max(start_line.to_string().len());
    for line_no in start_line..=current_line {
        if let Some(text) = lines.get(line_no - 1) {
            write!(f, "\n {:>width$} | {}", line_no, text, width = width)?;
            if line_no == current_line {
                let caret_col = frame.span.start_col.max(1);
                let caret_width = frame
                    .span
                    .end_col
                    .saturating_sub(frame.span.start_col)
                    .max(1);
                let mut marker = String::new();
                marker.push_str(&" ".repeat(width + 3 + caret_col.saturating_sub(1)));
                marker.push('^');
                if caret_width > 1 {
                    marker.push_str(&"~".repeat(caret_width - 1));
                }
                write!(f, "\n{}", marker)?;
            }
        }
    }
    Ok(())
}

fn source_for_path(path: &str) -> Option<String> {
    if let Some(registry) = SOURCE_REGISTRY.get() {
        if let Ok(registry) = registry.read() {
            if let Some(source) = registry.get(path) {
                return Some(source.clone());
            }
        }
    }
    if path.starts_with("bundle/") || path.starts_with('<') {
        return None;
    }
    let source = fs::read_to_string(path).ok()?;
    register_source(path.to_string(), source.clone());
    Some(source)
}

fn panic_payload_string(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(msg) => *msg,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(msg) => (*msg).to_string(),
            Err(_) => String::new(),
        },
    }
}

fn install_panic_hook() {
    PANIC_HOOK_INIT.call_once(|| {
        let hook = std::panic::take_hook();
        let _ = DEFAULT_PANIC_HOOK.set(hook);
        std::panic::set_hook(Box::new(|info| {
            let suppressed = PANIC_TRAP_DEPTH.with(|depth| depth.get() > 0);
            if !suppressed {
                if let Some(hook) = DEFAULT_PANIC_HOOK.get() {
                    hook(info);
                }
            }
        }));
    });
}
