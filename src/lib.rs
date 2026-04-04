pub mod ast;
pub mod bundle;
pub mod environment;
pub mod error;
pub mod interpreter;
pub mod concurrency;
pub mod io_runtime;
pub mod lexer;
pub mod package_manager;
pub mod parser;
pub mod stdlib;
pub mod token;
pub mod vm;
pub mod memory;
pub mod request_scope;

use std::path::PathBuf;
use bundle::BundleImage;
use error::{register_source, trap_dgm, DgmError};
use interpreter::{runtime_reset_all, Interpreter};
use lexer::Lexer;
use parser::Parser;

pub fn parse_source(source: &str) -> Result<Vec<ast::Stmt>, DgmError> {
    trap_dgm("parser", || {
        let mut lexer = Lexer::new(source);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    })
}

pub fn run_source(source: &str) -> Result<Interpreter, DgmError> {
    run_source_with_path(source, None)
}

pub fn run_source_with_path(source: &str, path: Option<PathBuf>) -> Result<Interpreter, DgmError> {
    trap_dgm("runtime entry", || {
        memory::init_allocator_maintenance();
        runtime_reset_all();
        let display_path = path
            .as_ref()
            .map(|path| path.to_string_lossy().to_string())
            .unwrap_or_else(|| "<repl>".into());
        register_source(display_path.clone(), source.to_string());
        let stmts = parse_source(source).map_err(|err| err.with_path(display_path.clone()))?;
        let mut interp = Interpreter::new();
        interp.set_entry_path(path.clone());
        interp.run(stmts)?;
        Ok(interp)
    })
}

pub fn run_bundle(bundle: BundleImage) -> Result<Interpreter, DgmError> {
    trap_dgm("bundle runtime", || {
        memory::init_allocator_maintenance();
        let mut interp = Interpreter::new();
        interp.run_bundle(bundle)?;
        Ok(interp)
    })
}

pub fn run_bundle_path(path: PathBuf) -> Result<Interpreter, DgmError> {
    trap_dgm("bundle loader", || {
        let bundle = bundle::load_bundle(&path)?;
        run_bundle(bundle)
    })
}
