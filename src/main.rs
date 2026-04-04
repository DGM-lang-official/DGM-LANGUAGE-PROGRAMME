use dgm::bundle::{self, BuildMode, BundleRuntimeStats};
use dgm::error::{trap_dgm, DgmError};
use dgm::interpreter::{runtime_profile_snapshot, Interpreter};
use dgm::lexer::Lexer;
use dgm::package_manager;
use dgm::parser::Parser;
use dgm::{parse_source, run_bundle_path, run_source_with_path};
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::Command;

fn run_file(path: &str) -> Result<(), DgmError> {
    let source = std::fs::read_to_string(path).map_err(|e| DgmError::RuntimeError { msg: format!("cannot read '{}': {}", path, e) })?;
    run_source_with_path(&source, Some(PathBuf::from(path))).map(|_| ())
}

fn run_bundle_file(path: &str) -> Result<(), DgmError> {
    run_bundle_path(PathBuf::from(path)).map(|_| ())
}

fn run_path_auto(path: &str) -> Result<(), DgmError> {
    let path_buf = PathBuf::from(path);
    if bundle::is_bundle_file(&path_buf)? {
        run_bundle_file(path)
    } else {
        run_file(path)
    }
}

fn inspect_bundle_file(path: &str) -> Result<(), DgmError> {
    let inspection = bundle::inspect_bundle(&PathBuf::from(path))?;
    println!("entry: {}", inspection.manifest.entry);
    println!("modules: {}", inspection.module_count);
    println!("bundle_format_version: {}", inspection.manifest.bundle_format_version);
    println!("vm_version: {}", inspection.manifest.vm_version);
    println!("dgm_version: {}", inspection.manifest.dgm_version);
    println!("build_mode: {}", inspection.manifest.build_mode);
    println!(
        "runtime: {{ max_steps: {}, max_call_depth: {}, max_heap: {}, max_threads: {}, max_wall_time_ms: {}, max_open_sockets: {}, max_concurrent_requests: {} }}",
        inspection.manifest.runtime.max_steps,
        inspection.manifest.runtime.max_call_depth,
        inspection.manifest.runtime.max_heap,
        inspection.manifest.runtime.max_threads,
        inspection.manifest.runtime.max_wall_time_ms,
        inspection.manifest.runtime.max_open_sockets,
        inspection.manifest.runtime.max_concurrent_requests
    );
    println!("checksum: {}", inspection.checksum);
    println!("bundle_size: {}", inspection.bundle_size);
    println!("debug_sources: {}", inspection.manifest.debug_sources.len());
    Ok(())
}

fn profile_path_auto(path: &str) -> Result<(), DgmError> {
    let path_buf = PathBuf::from(path);
    if bundle::is_bundle_file(&path_buf)? {
        let interp = run_bundle_path(path_buf)?;
        print_profile_report(interp.bundle_stats());
    } else {
        let source = std::fs::read_to_string(path)
            .map_err(|e| DgmError::RuntimeError { msg: format!("cannot read '{}': {}", path, e) })?;
        run_source_with_path(&source, Some(PathBuf::from(path)))?;
        print_profile_report(None);
    }
    Ok(())
}

fn profile_project() -> Result<(), DgmError> {
    let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) })?;
    let entry = package_manager::resolve_project_entry(&cwd)?;
    let source = std::fs::read_to_string(&entry)
        .map_err(|e| DgmError::RuntimeError { msg: format!("cannot read '{}': {}", entry.display(), e) })?;
    run_source_with_path(&source, Some(entry))?;
    print_profile_report(None);
    Ok(())
}

fn print_profile_report(bundle_stats: Option<BundleRuntimeStats>) {
    let profile = runtime_profile_snapshot();
    if let Some(bundle_stats) = bundle_stats {
        println!("bundle_load_time: {:.3} ms", bundle_stats.load_time_ns as f64 / 1_000_000.0);
        println!("bundle_entry_decode_time: {:.3} ms", bundle_stats.entry_decode_time_ns as f64 / 1_000_000.0);
        println!("bundle_loaded_modules: {}", bundle_stats.loaded_module_count);
        println!("bundle_peak_loaded_module_bytes: {}", bundle_stats.peak_loaded_module_bytes);
        println!("bundle_memory_mapped: {}", bundle_stats.memory_mapped);
    }
    println!("total_time: {:.3} ms", profile.total_time as f64 / 1_000_000.0);
    println!("allocations: {}", profile.alloc_count);
    println!("allocated_bytes: {}", profile.alloc_bytes);
    println!("vm_instruction_count: {}", profile.vm_instruction_count);
    println!("vm_frames: {}", profile.vm_frame_count);
    println!("vm_time: {:.3} ms", profile.vm_time_ns as f64 / 1_000_000.0);
    println!("vm_allocated_bytes: {}", profile.vm_alloc_bytes);
    println!("io_calls: {}", profile.io_call_count);
    println!("io_time: {:.3} ms", profile.io_time_ns as f64 / 1_000_000.0);
    println!("top_functions:");
    for line in top_profile_entries(&profile.function_time, Some(&profile.function_calls), profile.total_time, 10) {
        println!("  {}", line);
    }
    println!("top_modules:");
    for line in top_profile_entries(&profile.module_time, None, profile.total_time, 10) {
        println!("  {}", line);
    }
}

fn top_profile_entries(
    timings: &std::collections::HashMap<String, u128>,
    calls: Option<&std::collections::HashMap<String, u64>>,
    total_time: u128,
    limit: usize,
) -> Vec<String> {
    let mut items = timings.iter().collect::<Vec<_>>();
    items.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
    let mut lines = Vec::new();
    for (name, elapsed) in items.into_iter().take(limit) {
        let share = if total_time == 0 {
            0.0
        } else {
            (*elapsed as f64 / total_time as f64) * 100.0
        };
        let elapsed_ms = *elapsed as f64 / 1_000_000.0;
        let line = match calls.and_then(|calls| calls.get(name)) {
            Some(count) => format!("{name}: {share:.2}% ({elapsed_ms:.3} ms, {count} call(s))"),
            None => format!("{name}: {share:.2}% ({elapsed_ms:.3} ms)"),
        };
        lines.push(line);
    }
    if lines.is_empty() {
        lines.push("<none>".into());
    }
    lines
}

fn run_project() -> Result<(), DgmError> {
    let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) })?;
    run_project_at(&cwd)
}

fn run_project_at(root: &std::path::Path) -> Result<(), DgmError> {
    let entry = package_manager::resolve_project_entry(root)?;
    let source = std::fs::read_to_string(&entry)
        .map_err(|e| DgmError::RuntimeError { msg: format!("cannot read '{}': {}", entry.display(), e) })?;
    run_source_with_path(&source, Some(entry)).map(|_| ())
}

fn run_update_validation(root: &std::path::Path) -> Result<(), DgmError> {
    let cargo_toml = root.join("Cargo.toml");
    if cargo_toml.exists() {
        let output = Command::new("cargo")
            .arg("test")
            .arg("-q")
            .current_dir(root)
            .output()
            .map_err(|e| DgmError::RuntimeError { msg: format!("cannot run cargo test: {}", e) })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            let detail = if !stderr.trim().is_empty() { stderr.trim() } else { stdout.trim() };
            return Err(DgmError::RuntimeError { msg: format!("cargo test failed: {}", detail) });
        }
    }
    if let Ok(test_entry) = package_manager::resolve_project_test_entry(root) {
        let source = std::fs::read_to_string(&test_entry)
            .map_err(|e| DgmError::RuntimeError { msg: format!("cannot read '{}': {}", test_entry.display(), e) })?;
        run_source_with_path(&source, Some(test_entry)).map(|_| ())?;
    } else if package_manager::resolve_project_entry(root).is_ok() {
        run_project_at(root)?;
    }
    Ok(())
}

fn check_file(path: &str) -> Result<(), DgmError> {
    let source = std::fs::read_to_string(path).map_err(|e| DgmError::RuntimeError { msg: format!("cannot read '{}': {}", path, e) })?;
    parse_source(&source).map(|_| ())
}

fn run_test(path: &str) -> Result<(), DgmError> {
    run_file(path)?;
    println!("PASS {}", path);
    Ok(())
}

fn run_project_tests() -> Result<(), DgmError> {
    let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) })?;
    let entry = package_manager::resolve_project_test_entry(&cwd)?;
    let source = std::fs::read_to_string(&entry)
        .map_err(|e| DgmError::RuntimeError { msg: format!("cannot read '{}': {}", entry.display(), e) })?;
    run_source_with_path(&source, Some(entry.clone())).map(|_| ())?;
    println!("PASS {}", entry.display());
    Ok(())
}

fn run_repl() -> Result<(), DgmError> {
    println!("DGM 0.3.0 — Interactive REPL");
    println!("Type 'exit' to quit, 'help' for commands\n");
    let mut interp = Interpreter::new();
    let config = rustyline::config::Config::builder().history_ignore_space(true).build();
    let mut rl = rustyline::DefaultEditor::with_config(config)
        .map_err(|err| DgmError::RuntimeError { msg: format!("cannot start REPL: {}", err) })?;
    let history_path = std::env::var("HOME").ok().map(|h| format!("{}/.dgm_history", h));
    if let Some(path) = &history_path {
        let _ = rl.load_history(path);
    }
    let mut buffer = String::new();

    loop {
        let prompt = if buffer.is_empty() { ">>> " } else { "... " };
        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if buffer.is_empty() {
                    if trimmed.is_empty() {
                        continue;
                    }
                    if trimmed == "exit" || trimmed == "quit" {
                        break;
                    }
                    if trimmed == "help" {
                        print_repl_help();
                        continue;
                    }
                    if trimmed == ".clear" {
                        print!("\x1B[2J\x1B[1;1H");
                        io::stdout().flush().ok();
                        continue;
                    }
                }
                if !buffer.is_empty() {
                    buffer.push('\n');
                }
                buffer.push_str(&line);
                if !source_is_complete(&buffer) {
                    continue;
                }
                let source = std::mem::take(&mut buffer);
                let _ = rl.add_history_entry(source.as_str());
                let mut lexer = Lexer::new(&source);
                let tokens = match lexer.tokenize() {
                    Ok(tokens) => tokens,
                    Err(err) => {
                        eprintln!("\x1b[31m{}\x1b[0m", err);
                        continue;
                    }
                };
                let mut parser = Parser::new(tokens);
                let stmts = match parser.parse() {
                    Ok(stmts) => stmts,
                    Err(err) => {
                        eprintln!("\x1b[31m{}\x1b[0m", err);
                        continue;
                    }
                };
                if let Err(err) = interp.run(stmts) {
                    eprintln!("\x1b[31m{}\x1b[0m", err);
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                if buffer.is_empty() {
                    println!("^C");
                } else {
                    buffer.clear();
                    println!("^C");
                }
            }
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("Error: {}", err);
                break;
            }
        }
    }
    if let Some(path) = &history_path {
        let _ = rl.save_history(path);
    }
    Ok(())
}

fn source_is_complete(source: &str) -> bool {
    let mut braces = 0isize;
    let mut brackets = 0isize;
    let mut parens = 0isize;
    let mut in_string = false;
    let mut in_triple = false;
    let mut escaped = false;
    let chars: Vec<char> = source.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if in_triple {
            if ch == '"' && chars.get(i + 1) == Some(&'"') && chars.get(i + 2) == Some(&'"') {
                in_triple = false;
                i += 3;
                continue;
            }
            i += 1;
            continue;
        }
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if ch == '"' && chars.get(i + 1) == Some(&'"') && chars.get(i + 2) == Some(&'"') {
            in_triple = true;
            i += 3;
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' => braces += 1,
            '}' => braces -= 1,
            '[' => brackets += 1,
            ']' => brackets -= 1,
            '(' => parens += 1,
            ')' => parens -= 1,
            _ => {}
        }
        i += 1;
    }
    !in_string && !in_triple && braces <= 0 && brackets <= 0 && parens <= 0
}

fn print_repl_help() {
    println!("DGM REPL commands:");
    println!("  exit/quit  — exit REPL");
    println!("  help       — show this help");
    println!("  .clear     — clear screen");
    println!();
    println!("Available modules: math, io, os, json, time, http, crypto, regex, net, thread, strutils, collections, debug, path, config, runtime");
    println!("Use: imprt <module>");
    println!();
}

fn print_version() {
    println!("DGM Programming Language v0.3.0");
    println!("Created by Dang Gia Minh");
    println!("Built with Rust — tree-walk interpreter");
}

fn print_help() {
    println!("DGM Programming Language v0.3.0\n");
    println!("USAGE:");
    println!("  dgm init              Create a new DGM project");
    println!("  dgm add <pkg>         Add a package and install dependencies");
    println!("  dgm install           Install project dependencies");
    println!("  dgm install --offline Install project dependencies from cache only");
    println!("  dgm install --frozen  Fail if dgm.lock and dgm.toml differ");
    println!("  dgm clean             Remove installed packages and temp state");
    println!("  dgm cache prune       Remove unused cache entries for this project");
    println!("  dgm update [pkg]      Update dependencies and rewrite dgm.lock");
    println!("  dgm outdated          Show newer package versions");
    println!("  dgm why <pkg>         Show why a dependency is present");
    println!("  dgm doctor            Check lock, cache, and installed packages");
    println!("  dgm build             Build a standalone bundle artifact");
    println!("  dgm build -o <file>   Write bundle to a custom output path");
    println!("  dgm build --release   Build a release bundle");
    println!("  dgm run <file.dgm>    Run a DGM script");
    println!("  dgm run --bundle <f>  Run a built bundle");
    println!("  dgm inspect <bundle>  Show bundle metadata");
    println!("  dgm profile [target]  Run and print runtime profile");
    println!("  dgm run               Run the current DGM project");
    println!("  dgm test <file.dgm>   Run a DGM script as a test");
    println!("  dgm test              Run the project test entry");
    println!("  dgm check <file.dgm>  Parse-check a DGM script");
    println!("  dgm repl              Start interactive REPL");
    println!("  dgm version           Show version info");
    println!("  dgm help              Show this help\n");
    println!("MODULES:");
    println!("  math         — math functions");
    println!("  io           — file I/O");
    println!("  os           — OS operations");
    println!("  json         — JSON parse/stringify");
    println!("  time         — timestamps and formatting");
    println!("  http         — HTTP client/server");
    println!("  crypto       — hashing, base64, random bytes");
    println!("  regex        — regular expressions");
    println!("  net          — TCP networking");
    println!("  thread       — basic thread utilities");
    println!("  strutils     — string helpers");
    println!("  collections  — set/list helpers");
    println!("  debug        — debugging helpers");
    println!("  path         — path utilities");
    println!("  config       — config/env file helpers");
    println!("  runtime      — runtime limits and stats\n");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let result = trap_dgm("CLI", || match args.get(1).map(|s| s.as_str()) {
        Some("init") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            cwd.and_then(|cwd| {
                let root = package_manager::init_project(&cwd)?;
                println!("Initialized DGM project at {}", root.display());
                Ok(())
            })
        }
        Some("add") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            match (cwd, args.get(2)) {
                (Ok(cwd), Some(spec)) => (|| -> Result<(), DgmError> {
                    let (root, name, req) = package_manager::add_dependency(&cwd, spec, false)?;
                    let report = package_manager::install_project(&root, false)?;
                    println!(
                        "Added {} {} and installed {} package(s){}",
                        name,
                        req,
                        report.packages,
                        if report.reused_lock { " using lock file" } else { "" }
                    );
                    Ok(())
                })(),
                (Err(err), _) => Err(err),
                (_, None) => Err(DgmError::RuntimeError { msg: "usage: dgm add <pkg>[@version]".into() }),
            }
        }
        Some("install") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            cwd.and_then(|cwd| {
                let offline = args.iter().skip(2).any(|arg| arg == "--offline");
                let frozen = args.iter().skip(2).any(|arg| arg == "--frozen");
                for arg in args.iter().skip(2) {
                    if arg != "--offline" && arg != "--frozen" {
                        return Err(DgmError::RuntimeError { msg: format!("unknown install option '{}'", arg) });
                    }
                }
                let report = package_manager::install_project_with_options(&cwd, &package_manager::InstallOptions { offline, frozen })?;
                println!(
                    "Installed {} package(s) for {}{}",
                    report.packages,
                    report.root.display(),
                    if frozen {
                        " using frozen lock file"
                    } else if report.reused_lock {
                        " using lock file"
                    } else {
                        ""
                    }
                );
                Ok(())
            })
        }
        Some("clean") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            cwd.and_then(|cwd| {
                let report = package_manager::clean_project(&cwd)?;
                println!(
                    "Cleaned {} path(s) in {}",
                    report.removed_paths.len(),
                    report.root.display()
                );
                Ok(())
            })
        }
        Some("cache") => match args.get(2).map(|s| s.as_str()) {
            Some("prune") => {
                let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
                cwd.and_then(|cwd| {
                    let report = package_manager::prune_cache(&cwd)?;
                    println!(
                        "Pruned {} cache file(s) for {}",
                        report.removed_files.len(),
                        report.root.display()
                    );
                    Ok(())
                })
            }
            Some(arg) => Err(DgmError::RuntimeError { msg: format!("unknown cache command '{}'", arg) }),
            None => Err(DgmError::RuntimeError { msg: "usage: dgm cache prune".into() }),
        },
        Some("outdated") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            cwd.and_then(|cwd| {
                let offline = args.iter().skip(2).any(|arg| arg == "--offline");
                let outdated = package_manager::outdated_project(&cwd, offline)?;
                if outdated.is_empty() {
                    println!("All dependencies are up to date");
                } else {
                    for item in outdated {
                        println!(
                            "{} {} -> {}{}",
                            item.name,
                            item.current,
                            item.latest,
                            if item.major { " [major]" } else { "" }
                        );
                    }
                }
                Ok(())
            })
        }
        Some("why") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            match (cwd, args.get(2)) {
                (Ok(cwd), Some(name)) => (|| -> Result<(), DgmError> {
                    let chain = package_manager::why_dependency(&cwd, name)?;
                    println!("{}", chain.join(" -> "));
                    Ok(())
                })(),
                (Err(err), _) => Err(err),
                (_, None) => Err(DgmError::RuntimeError { msg: "usage: dgm why <pkg>".into() }),
            }
        }
        Some("update") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            cwd.and_then(|cwd| {
                let mut options = package_manager::UpdateOptions::default();
                for arg in args.iter().skip(2) {
                    match arg.as_str() {
                        "--dry-run" => options.dry_run = true,
                        "--offline" => options.offline = true,
                        "--allow-major" => options.allow_major = true,
                        "--test" => options.run_tests = true,
                        _ if arg.starts_with('-') => {
                            return Err(DgmError::RuntimeError { msg: format!("unknown update option '{}'", arg) });
                        }
                        _ => options.targets.push(arg.clone()),
                    }
                }
                let report = package_manager::update_project(&cwd, &options, run_update_validation)?;
                if report.changes.is_empty() {
                    println!("No dependency updates available");
                } else {
                    for change in &report.changes {
                        println!(
                            "{} {} -> {}{}",
                            change.name,
                            change.from.as_deref().unwrap_or("none"),
                            change.to,
                            if change.major_change { " [major]" } else { "" }
                        );
                    }
                    if report.dry_run {
                        println!("Dry run only: dgm.lock unchanged");
                    } else {
                        println!(
                            "Updated {} dependency entries across {} package(s){}",
                            report.changes.len(),
                            report.packages,
                            if report.tested { " with test gate" } else { "" }
                        );
                    }
                }
                Ok(())
            })
        }
        Some("doctor") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            cwd.and_then(|cwd| {
                let report = package_manager::doctor_project(&cwd)?;
                println!("Project: {}", report.root.display());
                if report.issues.is_empty() && report.warnings.is_empty() {
                    println!("Doctor OK");
                    return Ok(());
                }
                for issue in &report.issues {
                    println!("ISSUE: {}", issue);
                }
                for warning in &report.warnings {
                    println!("WARN: {}", warning);
                }
                if report.is_healthy() {
                    Ok(())
                } else {
                    Err(DgmError::RuntimeError { msg: "doctor found issues".into() })
                }
            })
        }
        Some("build") => {
            let cwd = std::env::current_dir().map_err(|e| DgmError::RuntimeError { msg: format!("cannot read current directory: {}", e) });
            cwd.and_then(|cwd| {
                let mut mode = BuildMode::Debug;
                let mut output = None;
                let mut index = 2usize;
                while index < args.len() {
                    match args[index].as_str() {
                        "--debug" => mode = BuildMode::Debug,
                        "--release" => mode = BuildMode::Release,
                        "-o" | "--output" => {
                            index += 1;
                            let path = args.get(index).ok_or_else(|| DgmError::RuntimeError { msg: "usage: dgm build [-o <file>] [--debug|--release]".into() })?;
                            output = Some(PathBuf::from(path));
                        }
                        arg => return Err(DgmError::RuntimeError { msg: format!("unknown build option '{}'", arg) }),
                    }
                    index += 1;
                }
                let output = bundle::build_project_with_options(&cwd, &bundle::BuildOptions { mode, output })?;
                println!("Built {} bundle at {}", mode.as_str(), output.display());
                Ok(())
            })
        }
        Some("inspect") => match args.get(2) {
            Some(path) => inspect_bundle_file(path),
            None => Err(DgmError::RuntimeError { msg: "usage: dgm inspect <bundle.dgm>".into() }),
        },
        Some("profile") => match args.get(2).map(|s| s.as_str()) {
            Some(path) => profile_path_auto(path),
            None => profile_project(),
        },
        Some("run") => match args.get(2).map(|s| s.as_str()) {
            Some("--bundle") => match args.get(3) {
                Some(path) => run_bundle_file(path),
                None => Err(DgmError::RuntimeError { msg: "usage: dgm run --bundle <bundle.dgm>".into() }),
            },
            Some(path) => run_path_auto(path),
            None => run_project(),
        },
        Some("test") => args.get(2).map(|path| run_test(path)).unwrap_or_else(run_project_tests),
        Some("check") => args.get(2).map(|path| check_file(path)).unwrap_or_else(|| Err(DgmError::RuntimeError { msg: "usage: dgm check <file.dgm>".into() })),
        Some("repl") => {
            run_repl()
        }
        Some("version") | Some("--version") | Some("-v") => {
            print_version();
            Ok(())
        }
        Some("help") | Some("--help") | Some("-h") => {
            print_help();
            Ok(())
        }
        None => {
            run_repl()
        }
        Some(arg) if arg.ends_with(".dgm") => run_path_auto(arg),
        Some(arg) => Err(DgmError::RuntimeError { msg: format!("unknown command '{}'", arg) }),
    });
    if let Err(err) = result {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}
