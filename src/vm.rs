use crate::ast::{BindingPattern, Expr, Param, Span, Stmt, StmtKind};
use crate::environment::{new_child_environment_ref, new_environment_ref, Environment};
use crate::error::DgmError;
use crate::interpreter::{
    runtime_allocation_counters, runtime_record_vm_frame, runtime_record_vm_instruction, runtime_tick, ControlFlow, DgmValue,
    Interpreter,
};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BytecodeModule {
    #[serde(default)]
    pub entry: CompiledChunk,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompiledFunctionBlob {
    pub name: Option<String>,
    #[serde(default)]
    pub params: Vec<Param>,
    #[serde(default)]
    pub body: Vec<Stmt>,
    #[serde(default)]
    pub chunk: CompiledChunk,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompiledChunk {
    pub name: Option<String>,
    #[serde(default)]
    pub local_names: Vec<String>,
    #[serde(default)]
    pub local_depths: Vec<usize>,
    #[serde(default)]
    pub instructions: Vec<Instruction>,
    #[serde(default)]
    pub spans: Vec<Span>,
    #[serde(default)]
    pub expr_pool: Vec<Expr>,
    #[serde(default)]
    pub stmt_pool: Vec<Stmt>,
    #[serde(default)]
    pub nested_functions: Vec<CompiledFunctionBlob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Constant {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Instruction {
    LoadConst(Constant),
    LoadLocal(usize),
    LoadName(String),
    StoreLocal(usize),
    DefineName(String),
    AssignName(String),
    MakeFunction(usize),
    BinOp(String),
    UnaryOp(String),
    Truthy,
    Jump(usize),
    JumpIfFalse(usize),
    JumpIfTrue(usize),
    JumpIfNotNull(usize),
    EnterScope,
    ExitScope,
    EvalExpr(usize),
    ExecStmt(usize),
    Writ,
    Pop,
    Return,
}

pub fn compile_module(stmts: &[Stmt], name: Option<String>) -> BytecodeModule {
    let mut compiler = Compiler::new(name);
    compiler.compile_stmts(stmts);
    BytecodeModule {
        entry: compiler.finish(),
    }
}

pub fn compile_function_blob(name: Option<String>, params: &[Param], body: &[Stmt]) -> Arc<CompiledFunctionBlob> {
    Arc::new(Compiler::compile_function(name, params, body))
}

pub(crate) fn execute_module(
    interp: &mut Interpreter,
    module: &BytecodeModule,
    env: Rc<RefCell<Environment>>,
) -> Result<ControlFlow, DgmError> {
    execute_chunk(interp, &module.entry, env, None)
}

pub(crate) fn execute_function(
    interp: &mut Interpreter,
    function: &CompiledFunctionBlob,
    env: Rc<RefCell<Environment>>,
) -> Result<ControlFlow, DgmError> {
    execute_chunk(interp, &function.chunk, env, Some(&function.params))
}

fn execute_chunk(
    interp: &mut Interpreter,
    chunk: &CompiledChunk,
    base_env: Rc<RefCell<Environment>>,
    params: Option<&[Param]>,
) -> Result<ControlFlow, DgmError> {
    let started = Instant::now();
    let alloc_before = runtime_allocation_counters();
    let mut state = VmState::new(chunk, base_env, params);
    let result = state.run(interp);
    let alloc_after = runtime_allocation_counters();
    runtime_record_vm_frame(
        started.elapsed().as_nanos(),
        state.executed_instructions,
        alloc_after.0.saturating_sub(alloc_before.0),
        alloc_after.1.saturating_sub(alloc_before.1),
    );
    result
}

struct VmState<'a> {
    chunk: &'a CompiledChunk,
    ip: usize,
    stack: Vec<DgmValue>,
    locals: Vec<DgmValue>,
    env_stack: Vec<Rc<RefCell<Environment>>>,
    executed_instructions: u64,
}

impl<'a> VmState<'a> {
    fn new(chunk: &'a CompiledChunk, base_env: Rc<RefCell<Environment>>, params: Option<&[Param]>) -> Self {
        let mut locals = vec![DgmValue::Null; chunk.local_names.len()];
        if let Some(params) = params {
            for (index, param) in params.iter().enumerate() {
                if index >= locals.len() {
                    break;
                }
                if let Some(value) = base_env.borrow().get_current(&param.name) {
                    locals[index] = value;
                }
            }
        } else {
            for (index, name) in chunk.local_names.iter().enumerate() {
                if chunk.local_depths.get(index).copied().unwrap_or(0) == 0 {
                    if let Some(value) = base_env.borrow().get_current(name) {
                        locals[index] = value;
                    }
                }
            }
        }
        Self {
            chunk,
            ip: 0,
            stack: Vec::new(),
            locals,
            env_stack: vec![base_env],
            executed_instructions: 0,
        }
    }

    fn run(&mut self, interp: &mut Interpreter) -> Result<ControlFlow, DgmError> {
        while self.ip < self.chunk.instructions.len() {
            runtime_tick()?;
            runtime_record_vm_instruction();
            self.executed_instructions = self.executed_instructions.saturating_add(1);
            let instruction = self.chunk.instructions[self.ip].clone();
            let span = self.chunk.spans.get(self.ip).copied().unwrap_or_default();
            self.ip += 1;
            let step = self.step(interp, instruction, span);
            match step {
                Ok(Some(flow)) => return Ok(flow),
                Ok(None) => {}
                Err(err) => return Err(interp.attach_span_frame(err, span)),
            }
        }
        Ok(ControlFlow::None)
    }

    fn step(
        &mut self,
        interp: &mut Interpreter,
        instruction: Instruction,
        span: Span,
    ) -> Result<Option<ControlFlow>, DgmError> {
        match instruction {
            Instruction::LoadConst(value) => self.stack.push(constant_to_value(&value)?),
            Instruction::LoadLocal(slot) => {
                let value = self.locals.get(slot).cloned().unwrap_or(DgmValue::Null);
                self.stack.push(value);
            }
            Instruction::LoadName(name) => {
                let value = self
                    .current_env()
                    .borrow()
                    .get(&name)
                    .ok_or_else(|| DgmError::runtime(format!("undefined variable '{}'", name)))?;
                self.stack.push(value);
            }
            Instruction::StoreLocal(slot) => {
                let value = self.pop_value("store")?;
                if slot >= self.locals.len() {
                    return Err(DgmError::runtime("invalid local slot"));
                }
                self.locals[slot] = value.clone();
                self.set_slot_env(slot, value.clone())?;
                self.stack.push(value);
            }
            Instruction::DefineName(name) => {
                let value = self.pop_value("define")?;
                self.current_env().borrow_mut().set(&name, value.clone());
                self.stack.push(value);
            }
            Instruction::AssignName(name) => {
                let value = self.pop_value("assign")?;
                if self.current_env().borrow().has(&name) {
                    self.current_env().borrow_mut().assign(&name, value.clone())?;
                } else {
                    self.current_env().borrow_mut().set(&name, value.clone());
                }
                self.stack.push(value);
            }
            Instruction::MakeFunction(index) => {
                let function = self
                    .chunk
                    .nested_functions
                    .get(index)
                    .cloned()
                    .ok_or_else(|| DgmError::runtime("invalid function constant"))?;
                self.stack.push(DgmValue::Function {
                    name: function.name.clone(),
                    params: function.params.clone(),
                    body: function.body.clone(),
                    closure: self.current_env(),
                    compiled: Some(Arc::new(function)),
                });
            }
            Instruction::BinOp(op) => {
                let right = self.pop_value("binary op")?;
                let left = self.pop_value("binary op")?;
                let value = interp.apply_binop(&op, left, right)?;
                self.stack.push(value);
            }
            Instruction::UnaryOp(op) => {
                let value = self.pop_value("unary op")?;
                let value = interp.apply_unary_op(&op, value)?;
                self.stack.push(value);
            }
            Instruction::Truthy => {
                let value = self.pop_value("truthy")?;
                self.stack.push(DgmValue::Bool(interp.is_truthy(&value)));
            }
            Instruction::Jump(target) => self.ip = target,
            Instruction::JumpIfFalse(target) => {
                if !interp.is_truthy(self.stack.last().ok_or_else(|| DgmError::runtime("vm stack underflow"))?) {
                    self.ip = target;
                }
            }
            Instruction::JumpIfTrue(target) => {
                if interp.is_truthy(self.stack.last().ok_or_else(|| DgmError::runtime("vm stack underflow"))?) {
                    self.ip = target;
                }
            }
            Instruction::JumpIfNotNull(target) => {
                if !matches!(self.stack.last(), Some(DgmValue::Null)) {
                    self.ip = target;
                }
            }
            Instruction::EnterScope => {
                let child = new_child_environment_ref(self.current_env());
                self.env_stack.push(child);
            }
            Instruction::ExitScope => {
                if self.env_stack.len() > 1 {
                    self.env_stack.pop();
                }
            }
            Instruction::EvalExpr(index) => {
                let expr = self
                    .chunk
                    .expr_pool
                    .get(index)
                    .ok_or_else(|| DgmError::runtime("invalid vm expr index"))?;
                let value = interp.eval_expr(expr, self.current_env())?;
                self.sync_slots_from_env();
                self.stack.push(value);
            }
            Instruction::ExecStmt(index) => {
                let stmt = self
                    .chunk
                    .stmt_pool
                    .get(index)
                    .ok_or_else(|| DgmError::runtime("invalid vm stmt index"))?;
                match interp.exec_stmt(stmt, self.current_env())? {
                    ControlFlow::None => self.sync_slots_from_env(),
                    flow => return Ok(Some(flow)),
                }
            }
            Instruction::Writ => {
                let value = self.pop_value("writ")?;
                println!("{}", value);
            }
            Instruction::Pop => {
                self.stack.pop();
            }
            Instruction::Return => {
                let value = self.stack.pop().unwrap_or(DgmValue::Null);
                self.env_stack.truncate(1);
                return Ok(Some(ControlFlow::Return(value)));
            }
        }
        if !span.eq(&Span::default()) {
            interp.note_vm_span(span);
        }
        Ok(None)
    }

    fn pop_value(&mut self, context: &str) -> Result<DgmValue, DgmError> {
        self.stack
            .pop()
            .ok_or_else(|| DgmError::runtime(format!("vm stack underflow during {}", context)))
    }

    fn current_env(&self) -> Rc<RefCell<Environment>> {
        self.env_stack
            .last()
            .cloned()
            .unwrap_or_else(new_environment_ref)
    }

    fn env_for_slot(&self, slot: usize) -> Option<Rc<RefCell<Environment>>> {
        let depth = self.chunk.local_depths.get(slot).copied().unwrap_or(0);
        self.env_stack.get(depth).cloned()
    }

    fn set_slot_env(&mut self, slot: usize, value: DgmValue) -> Result<(), DgmError> {
        let name = self
            .chunk
            .local_names
            .get(slot)
            .ok_or_else(|| DgmError::runtime("invalid local slot"))?
            .clone();
        if let Some(env) = self.env_for_slot(slot) {
            env.borrow_mut().set(&name, value);
        }
        Ok(())
    }

    fn sync_slots_from_env(&mut self) {
        for slot in 0..self.locals.len() {
            let Some(name) = self.chunk.local_names.get(slot) else {
                continue;
            };
            let Some(env) = self.env_for_slot(slot) else {
                continue;
            };
            let current = {
                let env_ref = env.borrow();
                env_ref.get_current(name)
            };
            if let Some(value) = current {
                self.locals[slot] = value;
            }
        }
    }
}

fn constant_to_value(value: &Constant) -> Result<DgmValue, DgmError> {
    match value {
        Constant::Int(value) => Ok(DgmValue::Int(*value)),
        Constant::Float(value) => Ok(DgmValue::Float(*value)),
        Constant::Str(value) => Ok(DgmValue::Str(value.clone())),
        Constant::Bool(value) => Ok(DgmValue::Bool(*value)),
        Constant::Null => Ok(DgmValue::Null),
    }
}

struct Compiler {
    chunk: CompiledChunk,
    scopes: Vec<HashMap<String, usize>>,
}

impl Compiler {
    fn new(name: Option<String>) -> Self {
        Self {
            chunk: CompiledChunk {
                name,
                ..CompiledChunk::default()
            },
            scopes: vec![HashMap::new()],
        }
    }

    fn compile_function(name: Option<String>, params: &[Param], body: &[Stmt]) -> CompiledFunctionBlob {
        let mut compiler = Self::new(name.clone());
        for param in params {
            compiler.define_local(&param.name);
        }
        compiler.compile_stmts(body);
        compiler.emit(Instruction::LoadConst(Constant::Null), compiler.default_span(body));
        compiler.emit(Instruction::Return, compiler.default_span(body));
        CompiledFunctionBlob {
            name,
            params: params.to_vec(),
            body: body.to_vec(),
            chunk: compiler.finish(),
        }
    }

    fn finish(self) -> CompiledChunk {
        self.chunk
    }

    fn compile_stmts(&mut self, stmts: &[Stmt]) {
        for stmt in stmts {
            self.compile_stmt(stmt);
        }
    }

    fn compile_stmt(&mut self, stmt: &Stmt) {
        match &stmt.kind {
            StmtKind::Expr(expr) => {
                self.compile_expr(expr, stmt.span);
                self.emit(Instruction::Pop, stmt.span);
            }
            StmtKind::Let { pattern: BindingPattern::Name { name, default: None }, value } => {
                self.compile_expr(value, stmt.span);
                let slot = self.define_local(name);
                self.emit(Instruction::StoreLocal(slot), stmt.span);
                self.emit(Instruction::Pop, stmt.span);
            }
            StmtKind::Writ(expr) => {
                self.compile_expr(expr, stmt.span);
                self.emit(Instruction::Writ, stmt.span);
            }
            StmtKind::If {
                condition,
                then_block,
                elseif_branches,
                else_block,
            } => {
                let mut branches = Vec::with_capacity(elseif_branches.len() + 1);
                branches.push((condition.clone(), then_block.clone()));
                for (cond, block) in elseif_branches {
                    branches.push((cond.clone(), block.clone()));
                }
                let mut end_jumps = Vec::new();
                for (cond, block) in branches {
                    self.compile_expr(&cond, stmt.span);
                    let false_jump = self.emit_placeholder(stmt.span);
                    self.emit(Instruction::Pop, stmt.span);
                    self.compile_block(&block);
                    end_jumps.push(self.emit_placeholder(stmt.span));
                    let next = self.chunk.instructions.len();
                    self.patch(false_jump, Instruction::JumpIfFalse(next));
                    self.emit(Instruction::Pop, stmt.span);
                }
                if let Some(block) = else_block {
                    self.compile_block(block);
                }
                let end = self.chunk.instructions.len();
                for jump in end_jumps {
                    self.patch(jump, Instruction::Jump(end));
                }
            }
            StmtKind::While { condition, body } if !has_loop_control(body) => {
                let loop_start = self.chunk.instructions.len();
                self.compile_expr(condition, stmt.span);
                let false_jump = self.emit_placeholder(stmt.span);
                self.emit(Instruction::Pop, stmt.span);
                self.compile_block(body);
                self.emit(Instruction::Jump(loop_start), stmt.span);
                let loop_end = self.chunk.instructions.len();
                self.patch(false_jump, Instruction::JumpIfFalse(loop_end));
                self.emit(Instruction::Pop, stmt.span);
            }
            StmtKind::FuncDef { name, params, body } => {
                let function_index = self.add_function(Some(name.clone()), params, body);
                self.emit(Instruction::MakeFunction(function_index), stmt.span);
                let slot = self.define_local(name);
                self.emit(Instruction::StoreLocal(slot), stmt.span);
                self.emit(Instruction::Pop, stmt.span);
            }
            StmtKind::Return(expr) => {
                if let Some(expr) = expr {
                    self.compile_expr(expr, stmt.span);
                } else {
                    self.emit(Instruction::LoadConst(Constant::Null), stmt.span);
                }
                self.emit(Instruction::Return, stmt.span);
            }
            _ => {
                let index = self.chunk.stmt_pool.len();
                self.chunk.stmt_pool.push(stmt.clone());
                self.emit(Instruction::ExecStmt(index), stmt.span);
            }
        }
    }

    fn compile_block(&mut self, stmts: &[Stmt]) {
        let span = self.default_span(stmts);
        self.emit(Instruction::EnterScope, span);
        self.push_scope();
        self.compile_stmts(stmts);
        self.pop_scope();
        self.emit(Instruction::ExitScope, span);
    }

    fn compile_expr(&mut self, expr: &Expr, span: Span) {
        match expr {
            Expr::IntLit(value) => self.emit(Instruction::LoadConst(Constant::Int(*value)), span),
            Expr::FloatLit(value) => self.emit(Instruction::LoadConst(Constant::Float(*value)), span),
            Expr::StringLit(value) => self.emit(Instruction::LoadConst(Constant::Str(value.clone())), span),
            Expr::BoolLit(value) => self.emit(Instruction::LoadConst(Constant::Bool(*value)), span),
            Expr::NullLit => self.emit(Instruction::LoadConst(Constant::Null), span),
            Expr::This => self.emit(Instruction::LoadName("__self__".into()), span),
            Expr::Ident(name) => {
                if let Some(slot) = self.lookup_local(name) {
                    self.emit(Instruction::LoadLocal(slot), span);
                } else {
                    self.emit(Instruction::LoadName(name.clone()), span);
                }
            }
            Expr::BinOp { op, left, right } if op == "and" => {
                self.compile_expr(left, span);
                let false_jump = self.emit_placeholder(span);
                self.emit(Instruction::Pop, span);
                self.compile_expr(right, span);
                self.emit(Instruction::Truthy, span);
                let end_jump = self.emit_placeholder(span);
                let false_label = self.chunk.instructions.len();
                self.patch(false_jump, Instruction::JumpIfFalse(false_label));
                self.emit(Instruction::Pop, span);
                self.emit(Instruction::LoadConst(Constant::Bool(false)), span);
                let end = self.chunk.instructions.len();
                self.patch(end_jump, Instruction::Jump(end));
            }
            Expr::BinOp { op, left, right } if op == "or" => {
                self.compile_expr(left, span);
                let true_jump = self.emit_placeholder(span);
                self.emit(Instruction::Pop, span);
                self.compile_expr(right, span);
                self.emit(Instruction::Truthy, span);
                let end_jump = self.emit_placeholder(span);
                let true_label = self.chunk.instructions.len();
                self.patch(true_jump, Instruction::JumpIfTrue(true_label));
                self.emit(Instruction::Pop, span);
                self.emit(Instruction::LoadConst(Constant::Bool(true)), span);
                let end = self.chunk.instructions.len();
                self.patch(end_jump, Instruction::Jump(end));
            }
            Expr::BinOp { op, left, right } => {
                self.compile_expr(left, span);
                self.compile_expr(right, span);
                self.emit(Instruction::BinOp(op.clone()), span);
            }
            Expr::NullCoalesce { left, right } => {
                self.compile_expr(left, span);
                let end_jump = self.emit_placeholder(span);
                self.emit(Instruction::Pop, span);
                self.compile_expr(right, span);
                let end = self.chunk.instructions.len();
                self.patch(end_jump, Instruction::JumpIfNotNull(end));
            }
            Expr::UnaryOp { op, operand } => {
                self.compile_expr(operand, span);
                self.emit(Instruction::UnaryOp(op.clone()), span);
            }
            Expr::Assign { target, op, value } if op == "=" => match &**target {
                Expr::Ident(name) => {
                    self.compile_expr(value, span);
                    if let Some(slot) = self.lookup_local(name) {
                        self.emit(Instruction::StoreLocal(slot), span);
                    } else {
                        self.emit(Instruction::AssignName(name.clone()), span);
                    }
                }
                _ => self.emit_fallback_expr(expr, span),
            },
            Expr::Lambda { params, body } => {
                let function_index = self.add_function(None, params, body);
                self.emit(Instruction::MakeFunction(function_index), span);
            }
            Expr::Ternary {
                condition,
                then_expr,
                else_expr,
            } => {
                self.compile_expr(condition, span);
                let false_jump = self.emit_placeholder(span);
                self.emit(Instruction::Pop, span);
                self.compile_expr(then_expr, span);
                let end_jump = self.emit_placeholder(span);
                let else_label = self.chunk.instructions.len();
                self.patch(false_jump, Instruction::JumpIfFalse(else_label));
                self.emit(Instruction::Pop, span);
                self.compile_expr(else_expr, span);
                let end = self.chunk.instructions.len();
                self.patch(end_jump, Instruction::Jump(end));
            }
            _ => self.emit_fallback_expr(expr, span),
        }
    }

    fn emit_fallback_expr(&mut self, expr: &Expr, span: Span) {
        let index = self.chunk.expr_pool.len();
        self.chunk.expr_pool.push(expr.clone());
        self.emit(Instruction::EvalExpr(index), span);
    }

    fn emit(&mut self, instruction: Instruction, span: Span) {
        self.chunk.instructions.push(instruction);
        self.chunk.spans.push(span);
    }

    fn emit_placeholder(&mut self, span: Span) -> usize {
        let index = self.chunk.instructions.len();
        self.emit(Instruction::Jump(usize::MAX), span);
        index
    }

    fn patch(&mut self, index: usize, instruction: Instruction) {
        if let Some(slot) = self.chunk.instructions.get_mut(index) {
            *slot = instruction;
        }
    }

    fn add_function(&mut self, name: Option<String>, params: &[Param], body: &[Stmt]) -> usize {
        let index = self.chunk.nested_functions.len();
        self.chunk
            .nested_functions
            .push(Self::compile_function(name, params, body));
        index
    }

    fn define_local(&mut self, name: &str) -> usize {
        if let Some(existing) = self.scopes.last().and_then(|scope| scope.get(name)).copied() {
            return existing;
        }
        let slot = self.chunk.local_names.len();
        self.chunk.local_names.push(name.to_string());
        self.chunk.local_depths.push(self.current_depth());
        self.scopes.last_mut().unwrap().insert(name.to_string(), slot);
        slot
    }

    fn lookup_local(&self, name: &str) -> Option<usize> {
        self.scopes
            .iter()
            .rev()
            .find_map(|scope| scope.get(name).copied())
    }

    fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    fn pop_scope(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }

    fn current_depth(&self) -> usize {
        self.scopes.len().saturating_sub(1)
    }

    fn default_span(&self, stmts: &[Stmt]) -> Span {
        stmts.first().map(|stmt| stmt.span).unwrap_or_default()
    }
}

fn has_loop_control(stmts: &[Stmt]) -> bool {
    for stmt in stmts {
        match &stmt.kind {
            StmtKind::Break | StmtKind::Continue => return true,
            StmtKind::If {
                then_block,
                elseif_branches,
                else_block,
                ..
            } => {
                if has_loop_control(then_block) {
                    return true;
                }
                for (_, block) in elseif_branches {
                    if has_loop_control(block) {
                        return true;
                    }
                }
                if else_block.as_ref().is_some_and(|block| has_loop_control(block)) {
                    return true;
                }
            }
            StmtKind::While { body, .. }
            | StmtKind::For { body, .. } => {
                if has_loop_control(body) {
                    return true;
                }
            }
            StmtKind::TryCatch {
                try_block,
                catch_block,
                finally_block,
                ..
            } => {
                if has_loop_control(try_block)
                    || has_loop_control(catch_block)
                    || finally_block.as_ref().is_some_and(|block| has_loop_control(block))
                {
                    return true;
                }
            }
            StmtKind::Match { arms, .. } => {
                if arms.iter().any(|arm| has_loop_control(&arm.body)) {
                    return true;
                }
            }
            StmtKind::FuncDef { .. } | StmtKind::ClassDef { .. } => {}
            _ => {}
        }
    }
    false
}
