pub mod math;
pub mod io_mod;
pub mod os_mod;
pub mod json_mod;
pub mod time_mod;
pub mod http_mod;
pub mod crypto_mod;
pub mod regex_mod;
pub mod net_mod;
pub mod thread_mod;
pub mod string_utils;
pub mod collections;
pub mod debug_mod;
pub mod path_mod;
pub mod config_mod;
pub mod runtime_mod;
pub mod log_mod;

use std::cell::RefCell;
use std::rc::Rc;
use crate::interpreter::DgmValue;

pub fn load_module(name: &str) -> Option<DgmValue> {
    let map = match name {
        "math" => math::module(),
        "io" => io_mod::module(),
        "os" => os_mod::module(),
        "json" => json_mod::module(),
        "time" => time_mod::module(),
        "http" => http_mod::module(),
        "crypto" => crypto_mod::module(),
        "regex" => regex_mod::module(),
        "net" => net_mod::module(),
        "thread" => thread_mod::module(),
        "strutils" | "string_utils" | "strings" => string_utils::module(),
        "collections" | "collect" => collections::module(),
        "debug" | "dbg" => debug_mod::module(),
        "path" => path_mod::module(),
        "config" => config_mod::module(),
        "runtime" | "rt" => runtime_mod::module(),
        "log" | "logger" => log_mod::module(),
        _ => return None,
    };
    Some(DgmValue::Map(Rc::new(RefCell::new(map))))
}
