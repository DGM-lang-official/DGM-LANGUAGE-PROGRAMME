use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Span {
    pub start_line: usize,
    pub start_col: usize,
    pub end_line: usize,
    pub end_col: usize,
}

impl Span {
    pub fn new(start_line: usize, start_col: usize, end_line: usize, end_col: usize) -> Self {
        Self {
            start_line,
            start_col,
            end_line,
            end_col,
        }
    }

    pub fn merge(self, other: Span) -> Self {
        Self {
            start_line: self.start_line,
            start_col: self.start_col,
            end_line: other.end_line,
            end_col: other.end_col,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Param {
    pub name: String,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Argument {
    pub name: Option<String>,
    pub value: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BindingPattern {
    Ignore,
    Name {
        name: String,
        default: Option<Expr>,
    },
    List(Vec<BindingPattern>),
    Map(Vec<MapBinding>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapBinding {
    pub key: String,
    pub pattern: BindingPattern,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MatchPattern {
    Wildcard,
    Binding(String),
    Expr(Expr),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchArm {
    pub pattern: MatchPattern,
    pub guard: Option<Expr>,
    pub body: Vec<Stmt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    IntLit(i64),
    FloatLit(f64),
    StringLit(String),
    BoolLit(bool),
    NullLit,
    Ident(String),
    This,
    BinOp { op: String, left: Box<Expr>, right: Box<Expr> },
    NullCoalesce { left: Box<Expr>, right: Box<Expr> },
    UnaryOp { op: String, operand: Box<Expr> },
    Call { callee: Box<Expr>, args: Vec<Argument> },
    OptionalCall { callee: Box<Expr>, args: Vec<Argument> },
    Index { object: Box<Expr>, index: Box<Expr> },
    FieldAccess { object: Box<Expr>, field: String },
    OptionalFieldAccess { object: Box<Expr>, field: String },
    List(Vec<Expr>),
    Map(Vec<(Expr, Expr)>),
    Assign { target: Box<Expr>, op: String, value: Box<Expr> },
    New { callee: Box<Expr>, args: Vec<Argument> },
    Lambda { params: Vec<Param>, body: Vec<Stmt> },
    Await(Box<Expr>),
    Ternary { condition: Box<Expr>, then_expr: Box<Expr>, else_expr: Box<Expr> },
    StringInterp(Vec<Expr>),
    Range { start: Box<Expr>, end: Box<Expr> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stmt {
    pub span: Span,
    pub kind: StmtKind,
}

impl Stmt {
    pub fn new(span: Span, kind: StmtKind) -> Self {
        Self { span, kind }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StmtKind {
    Expr(Expr),
    Let { pattern: BindingPattern, value: Expr },
    Writ(Expr),
    If {
        condition: Expr,
        then_block: Vec<Stmt>,
        elseif_branches: Vec<(Expr, Vec<Stmt>)>,
        else_block: Option<Vec<Stmt>>,
    },
    While { condition: Expr, body: Vec<Stmt> },
    For { var: String, iterable: Expr, body: Vec<Stmt> },
    FuncDef { name: String, params: Vec<Param>, body: Vec<Stmt> },
    Return(Option<Expr>),
    Break,
    Continue,
    ClassDef { name: String, parent: Option<String>, methods: Vec<Stmt> },
    Imprt(String),
    TryCatch {
        try_block: Vec<Stmt>,
        catch_var: Option<String>,
        catch_block: Vec<Stmt>,
        finally_block: Option<Vec<Stmt>>,
    },
    Throw(Expr),
    Match {
        expr: Expr,
        arms: Vec<MatchArm>,
    },
}
