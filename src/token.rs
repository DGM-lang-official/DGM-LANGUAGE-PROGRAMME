use crate::ast::Span;

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    // Literals
    IntLit, FloatLit, StringLit, Ident,
    // Keywords – original
    Imprt, Writ, Def, Retrun, Iff, Elseif, Els,
    Fr, Whl, Brk, Cont, Tru, Fals, Nul, Let,
    And, Or, Not, Cls, New, Ths, In,
    // Keywords – new
    Try, Catch, Finally, Throw,
    Match, When, Extends, Lam, Await,
    // Symbols – original
    LParen, RParen, LBrace, RBrace, LBracket, RBracket,
    Comma, Dot, Colon, Newline, EOF, Semicolon,
    // Operators – original
    Plus, Minus, Star, Slash, Percent, StarStar,
    Eq, EqEq, BangEq, Lt, Gt, LtEq, GtEq,
    PlusEq, MinusEq, StarEq, SlashEq,
    // Operators – new
    Arrow,      // =>
    DotDot,     // ..
    Question,   // ?
    QuestionDot, // ?.
    QuestionQuestion, // ??
    Ampersand,  // &
    Pipe,       // |
    Caret,      // ^
    Tilde,      // ~
    ShiftLeft,  // <<
    ShiftRight, // >>
    Bang,       // !
    PercentEq,  // %=
    FStringStart, // f"
}

#[derive(Debug, Clone)]
pub struct Token {
    pub kind: TokenKind,
    pub lexeme: String,
    pub line: usize,
    pub column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

impl Token {
    pub fn new(
        kind: TokenKind,
        lexeme: impl Into<String>,
        line: usize,
        column: usize,
        end_line: usize,
        end_column: usize,
    ) -> Self {
        Self {
            kind,
            lexeme: lexeme.into(),
            line,
            column,
            end_line,
            end_column,
        }
    }

    pub fn span(&self) -> Span {
        Span::new(self.line, self.column, self.end_line, self.end_column)
    }
}
