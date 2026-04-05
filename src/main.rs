mod ast;
mod environment;
mod error;
mod interpreter;
mod lexer;
mod parser;
mod token;
pub mod stdlib;

use interpreter::Interpreter;
use lexer::Lexer;
use parser::Parser;
use std::io::{self, Write};

fn run_source(source: &str) -> Result<(), error::DgmError> {
    let mut lexer = Lexer::new(source);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let stmts = parser.parse()?;
    let mut interp = Interpreter::new();
    interp.run(stmts)
}

fn run_file(path: &str) {
    match std::fs::read_to_string(path) {
        Ok(source) => {
            if let Err(e) = run_source(&source) {
                eprintln!("{}", e);
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Cannot read file '{}': {}", path, e);
            std::process::exit(1);
        }
    }
}

fn run_repl() {
    println!("DGM 0.2.0 — Interactive REPL");
    println!("Type 'exit' to quit, 'help' for commands\n");
    let mut interp = Interpreter::new();

    let config = rustyline::config::Config::builder()
        .history_ignore_space(true)
        .build();
    let mut rl = rustyline::DefaultEditor::with_config(config).unwrap();
    let history_path = dirs_home().map(|h| format!("{}/.dgm_history", h));
    if let Some(ref path) = history_path { let _ = rl.load_history(path); }

    loop {
        let readline = rl.readline(">>> ");
        match readline {
            Ok(line) => {
                let line = line.trim().to_string();
                if line.is_empty() { continue; }
                if line == "exit" || line == "quit" { break; }
                if line == "help" {
                    println!("DGM REPL commands:");
                    println!("  exit/quit  — exit REPL");
                    println!("  help       — show this help");
                    println!("  .clear     — clear screen");
                    println!("\nAvailable modules: math, io, os, fs, json, time, http, crypto, regex, net, thread");
                    println!("Use: imprt <module>\n");
                    continue;
                }
                if line == ".clear" {
                    print!("\x1B[2J\x1B[1;1H");
                    io::stdout().flush().ok();
                    continue;
                }
                let _ = rl.add_history_entry(&line);

                let mut lexer = Lexer::new(&line);
                let tokens = match lexer.tokenize() {
                    Ok(t) => t,
                    Err(e) => { eprintln!("\x1b[31m{}\x1b[0m", e); continue; }
                };
                let mut parser = Parser::new(tokens);
                let stmts = match parser.parse() {
                    Ok(s) => s,
                    Err(e) => { eprintln!("\x1b[31m{}\x1b[0m", e); continue; }
                };
                if let Err(e) = interp.run(stmts) {
                    eprintln!("\x1b[31m{}\x1b[0m", e);
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => { println!("^C"); continue; }
            Err(rustyline::error::ReadlineError::Eof) => { break; }
            Err(e) => { eprintln!("Error: {}", e); break; }
        }
    }
    if let Some(ref path) = history_path { let _ = rl.save_history(path); }
}

fn dirs_home() -> Option<String> {
    std::env::var("HOME").ok()
}

fn print_version() {
    println!("DGM Programming Language v0.2.0");
    println!("Created by Dang Gia Minh");
    println!("Built with Rust — tree-walk interpreter");
}

fn print_help() {
    println!("DGM Programming Language v0.2.0\n");
    println!("USAGE:");
    println!("  dgm run <file.dgm>    Run a DGM script");
    println!("  dgm repl              Start interactive REPL");
    println!("  dgm version           Show version info");
    println!("  dgm help              Show this help\n");
    println!("MODULES:");
    println!("  math     — math functions (sqrt, sin, cos, random, etc.)");
    println!("  io       — file I/O (read_file, write_file, mkdir, etc.)");
    println!("  fs       — sandboxed filesystem (read, write, append, delete, list)");
    println!("  os       — OS operations (exec, env, platform, sleep, etc.)");
    println!("  json     — JSON parse/stringify");
    println!("  time     — timestamps and formatting");
    println!("  http     — HTTP client/server (get, post, serve)");
    println!("  crypto   — cryptography (sha256, md5, base64)");
    println!("  regex    — regular expressions");
    println!("  net      — TCP networking");
    println!("  thread   — threading utilities\n");
    println!("EXAMPLE:");
    println!("  # hello.dgm");
    println!("  let name = input(\"Your name: \")");
    println!("  writ(f\"Hello, {{name}}!\")");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("run") => {
            if let Some(path) = args.get(2) {
                run_file(path);
            } else {
                eprintln!("Usage: dgm run <file.dgm>");
            }
        }
        Some("repl") => run_repl(),
        Some("version") | Some("--version") | Some("-v") => print_version(),
        Some("help") | Some("--help") | Some("-h") => print_help(),
        None => run_repl(),
        Some(arg) => {
            // If arg ends with .dgm, treat as file
            if arg.ends_with(".dgm") {
                run_file(arg);
            } else {
                eprintln!("Unknown command '{}'. Use 'dgm help' for usage.", arg);
            }
        }
    }
}
