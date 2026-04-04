use crate::error::DgmError;
use crate::token::{Token, TokenKind};

pub struct Lexer {
    source: Vec<char>,
    pos: usize,
    line: usize,
    column: usize,
    last_line: usize,
    last_column: usize,
}

impl Lexer {
    pub fn new(source: &str) -> Self {
        Self { source: source.chars().collect(), pos: 0, line: 1, column: 1, last_line: 1, last_column: 1 }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, DgmError> {
        let mut tokens = vec![];
        while !self.is_at_end() {
            let ch = self.current();
            match ch {
                ' ' | '\t' | '\r' => { self.advance(); }
                '\n' => {
                    let (line, column) = self.position();
                    self.advance();
                    tokens.push(self.make_token(TokenKind::Newline, "\\n", line, column));
                }
                '#' => { while !self.is_at_end() && self.current() != '\n' { self.advance(); } }
                '(' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::LParen, "(", line, column)); }
                ')' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::RParen, ")", line, column)); }
                '{' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::LBrace, "{", line, column)); }
                '}' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::RBrace, "}", line, column)); }
                '[' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::LBracket, "[", line, column)); }
                ']' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::RBracket, "]", line, column)); }
                ',' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::Comma, ",", line, column)); }
                ':' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::Colon, ":", line, column)); }
                ';' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::Semicolon, ";", line, column)); }
                '?' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '.' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::QuestionDot, "?.", line, column));
                    } else if !self.is_at_end() && self.current() == '?' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::QuestionQuestion, "??", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Question, "?", line, column));
                    }
                }
                '~' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::Tilde, "~", line, column)); }
                '^' => { let (line, column) = self.position(); self.advance(); tokens.push(self.make_token(TokenKind::Caret, "^", line, column)); }
                '.' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '.' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::DotDot, "..", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Dot, ".", line, column));
                    }
                }
                '&' => {
                    let (line, column) = self.position();
                    self.advance();
                    tokens.push(self.make_token(TokenKind::Ampersand, "&", line, column));
                }
                '|' => {
                    let (line, column) = self.position();
                    self.advance();
                    tokens.push(self.make_token(TokenKind::Pipe, "|", line, column));
                }
                '+' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::PlusEq, "+=", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Plus, "+", line, column));
                    }
                }
                '-' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::MinusEq, "-=", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Minus, "-", line, column));
                    }
                }
                '*' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '*' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::StarStar, "**", line, column));
                    } else if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::StarEq, "*=", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Star, "*", line, column));
                    }
                }
                '/' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::SlashEq, "/=", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Slash, "/", line, column));
                    }
                }
                '%' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::PercentEq, "%=", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Percent, "%", line, column));
                    }
                }
                '=' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::EqEq, "==", line, column));
                    } else if !self.is_at_end() && self.current() == '>' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::Arrow, "=>", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Eq, "=", line, column));
                    }
                }
                '!' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::BangEq, "!=", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Bang, "!", line, column));
                    }
                }
                '<' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::LtEq, "<=", line, column));
                    } else if !self.is_at_end() && self.current() == '<' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::ShiftLeft, "<<", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Lt, "<", line, column));
                    }
                }
                '>' => {
                    let (line, column) = self.position();
                    self.advance();
                    if !self.is_at_end() && self.current() == '=' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::GtEq, ">=", line, column));
                    } else if !self.is_at_end() && self.current() == '>' {
                        self.advance();
                        tokens.push(self.make_token(TokenKind::ShiftRight, ">>", line, column));
                    } else {
                        tokens.push(self.make_token(TokenKind::Gt, ">", line, column));
                    }
                }
                'f' if self.peek(1) == Some('"') => {
                    let t = self.lex_fstring(self.peek(2) == Some('"') && self.peek(3) == Some('"'))?;
                    tokens.extend(t);
                }
                '"' => {
                    let t = self.lex_string(self.peek(1) == Some('"') && self.peek(2) == Some('"'))?;
                    tokens.push(t);
                }
                c if c.is_ascii_digit() => { let t = self.lex_number()?; tokens.push(t); }
                c if c.is_alphabetic() || c == '_' => { let t = self.lex_ident(); tokens.push(t); }
                other => {
                    return Err(DgmError::lex(format!("unexpected character '{}'", other), self.line, self.column));
                }
            }
        }
        let (line, column) = self.position();
        tokens.push(Token::new(TokenKind::EOF, "", line, column, line, column));
        Ok(tokens)
    }

    fn lex_string(&mut self, multiline: bool) -> Result<Token, DgmError> {
        let (line, column) = self.position();
        self.advance();
        if multiline {
            self.advance();
            self.advance();
        }
        let mut s = String::new();
        while !self.is_at_end() {
            if multiline {
                if self.current() == '"' && self.peek(1) == Some('"') && self.peek(2) == Some('"') {
                    break;
                }
            } else if self.current() == '"' {
                break;
            }
            if self.current() == '\\' {
                self.advance();
                if self.is_at_end() {
                    return Err(DgmError::lex("unterminated string escape", line, column));
                }
                match self.current() {
                    'n' => s.push('\n'),
                    't' => s.push('\t'),
                    'r' => s.push('\r'),
                    '\\' => s.push('\\'),
                    '"' => s.push('"'),
                    '0' => s.push('\0'),
                    other => s.push(other),
                }
            } else if self.current() == '\n' {
                if !multiline {
                    return Err(DgmError::lex("unterminated string", line, column));
                }
                s.push('\n');
            } else {
                s.push(self.current());
            }
            self.advance();
        }
        if self.is_at_end() {
            return Err(DgmError::lex("unterminated string", line, column));
        }
        self.advance();
        if multiline {
            self.advance();
            self.advance();
        }
        Ok(self.make_token(TokenKind::StringLit, s, line, column))
    }

    fn lex_fstring(&mut self, multiline: bool) -> Result<Vec<Token>, DgmError> {
        let (line, column) = self.position();
        self.advance();
        self.advance();
        if multiline {
            self.advance();
            self.advance();
        }
        let mut tokens = vec![];
        tokens.push(Token::new(TokenKind::FStringStart, "f\"", line, column, line, column + 1));

        let mut buf = String::new();
        while !self.is_at_end() {
            if multiline {
                if self.current() == '"' && self.peek(1) == Some('"') && self.peek(2) == Some('"') {
                    break;
                }
            } else if self.current() == '"' {
                break;
            }
            if self.current() == '{' {
                if !buf.is_empty() {
                    tokens.push(Token::new(TokenKind::StringLit, buf.clone(), line, column, self.last_line, self.last_column));
                }
                buf.clear();
                let (brace_line, brace_col) = self.position();
                self.advance();
                tokens.push(self.make_token(TokenKind::LBrace, "{", brace_line, brace_col));
                let mut depth = 1;
                let mut inner_src = String::new();
                while !self.is_at_end() && depth > 0 {
                    if self.current() == '{' { depth += 1; }
                    if self.current() == '}' {
                        depth -= 1;
                        if depth == 0 { break; }
                    }
                    inner_src.push(self.current());
                    self.advance();
                }
                let mut inner_lexer = Lexer::new(&inner_src);
                let inner_tokens = inner_lexer.tokenize()?;
                for t in inner_tokens {
                    if t.kind != TokenKind::EOF {
                        tokens.push(Token::new(
                            t.kind,
                            t.lexeme,
                            self.line,
                            self.column,
                            self.line,
                            self.column,
                        ));
                    }
                }
                if !self.is_at_end() && self.current() == '}' {
                    let (brace_line, brace_col) = self.position();
                    self.advance();
                    tokens.push(self.make_token(TokenKind::RBrace, "}", brace_line, brace_col));
                }
            } else if self.current() == '\\' {
                self.advance();
                if self.is_at_end() {
                    return Err(DgmError::lex("unterminated f-string escape", line, column));
                }
                match self.current() {
                    'n' => buf.push('\n'),
                    't' => buf.push('\t'),
                    '\\' => buf.push('\\'),
                    '"' => buf.push('"'),
                    '{' => buf.push('{'),
                    '}' => buf.push('}'),
                    other => buf.push(other),
                }
                self.advance();
            } else if self.current() == '\n' {
                if !multiline {
                    return Err(DgmError::lex("unterminated f-string", line, column));
                }
                buf.push('\n');
                self.advance();
            } else {
                buf.push(self.current());
                self.advance();
            }
        }
        if !buf.is_empty() {
            tokens.push(Token::new(TokenKind::StringLit, buf, line, column, self.last_line, self.last_column));
        }
        if self.is_at_end() {
            return Err(DgmError::lex("unterminated f-string", line, column));
        }
        self.advance();
        if multiline {
            self.advance();
            self.advance();
        }
        tokens.push(Token::new(TokenKind::RParen, ")", line, column, self.last_line, self.last_column));
        Ok(tokens)
    }

    fn lex_number(&mut self) -> Result<Token, DgmError> {
        let (line, column) = self.position();
        let mut s = String::new();

        // Hex: 0x...
        if self.current() == '0' && self.peek_next().map(|c| c == 'x' || c == 'X').unwrap_or(false) {
            s.push(self.current()); self.advance(); // '0'
            s.push(self.current()); self.advance(); // 'x'
            while !self.is_at_end() && self.current().is_ascii_hexdigit() {
                s.push(self.current());
                self.advance();
            }
            let val = i64::from_str_radix(&s[2..], 16).map_err(|_| DgmError::lex(format!("invalid hex '{}'", s), line, column))?;
            return Ok(self.make_token(TokenKind::IntLit, val.to_string(), line, column));
        }

        // Binary: 0b...
        if self.current() == '0' && self.peek_next().map(|c| c == 'b' || c == 'B').unwrap_or(false) {
            s.push(self.current()); self.advance();
            s.push(self.current()); self.advance();
            while !self.is_at_end() && (self.current() == '0' || self.current() == '1') {
                s.push(self.current());
                self.advance();
            }
            let val = i64::from_str_radix(&s[2..], 2).map_err(|_| DgmError::lex(format!("invalid binary '{}'", s), line, column))?;
            return Ok(self.make_token(TokenKind::IntLit, val.to_string(), line, column));
        }

        // Octal: 0o...
        if self.current() == '0' && self.peek_next().map(|c| c == 'o' || c == 'O').unwrap_or(false) {
            s.push(self.current()); self.advance();
            s.push(self.current()); self.advance();
            while !self.is_at_end() && self.current().is_digit(8) {
                s.push(self.current());
                self.advance();
            }
            let val = i64::from_str_radix(&s[2..], 8).map_err(|_| DgmError::lex(format!("invalid octal '{}'", s), line, column))?;
            return Ok(self.make_token(TokenKind::IntLit, val.to_string(), line, column));
        }

        // Decimal
        while !self.is_at_end() && (self.current().is_ascii_digit() || self.current() == '_') {
            if self.current() != '_' { s.push(self.current()); }
            self.advance();
        }
        if !self.is_at_end() && self.current() == '.' && self.peek_next().map(|c| c.is_ascii_digit()).unwrap_or(false) {
            s.push('.');
            self.advance();
            while !self.is_at_end() && (self.current().is_ascii_digit() || self.current() == '_') {
                if self.current() != '_' { s.push(self.current()); }
                self.advance();
            }
            // Scientific notation
            if !self.is_at_end() && (self.current() == 'e' || self.current() == 'E') {
                s.push('e');
                self.advance();
                if !self.is_at_end() && (self.current() == '+' || self.current() == '-') {
                    s.push(self.current());
                    self.advance();
                }
                while !self.is_at_end() && self.current().is_ascii_digit() {
                    s.push(self.current());
                    self.advance();
                }
            }
            Ok(self.make_token(TokenKind::FloatLit, s, line, column))
        } else {
            // Scientific notation on integer
            if !self.is_at_end() && (self.current() == 'e' || self.current() == 'E') {
                s.push('e');
                self.advance();
                if !self.is_at_end() && (self.current() == '+' || self.current() == '-') {
                    s.push(self.current());
                    self.advance();
                }
                while !self.is_at_end() && self.current().is_ascii_digit() {
                    s.push(self.current());
                    self.advance();
                }
                Ok(self.make_token(TokenKind::FloatLit, s, line, column))
            } else {
                Ok(self.make_token(TokenKind::IntLit, s, line, column))
            }
        }
    }

    fn lex_ident(&mut self) -> Token {
        let (line, column) = self.position();
        let mut s = String::new();
        while !self.is_at_end() && (self.current().is_alphanumeric() || self.current() == '_') {
            s.push(self.current());
            self.advance();
        }
        let kind = match s.as_str() {
            "imprt" | "import" => TokenKind::Imprt,
            "writ" | "print" => TokenKind::Writ,
            "def" | "func" | "function" => TokenKind::Def,
            "retrun" | "return" => TokenKind::Retrun,
            "iff" | "if" => TokenKind::Iff,
            "elseif" | "elif" => TokenKind::Elseif,
            "els" | "else" => TokenKind::Els,
            "fr" | "for" => TokenKind::Fr,
            "whl" | "while" => TokenKind::Whl,
            "brk" | "break" => TokenKind::Brk,
            "cont" | "continue" => TokenKind::Cont,
            "tru" | "true" => TokenKind::Tru,
            "fals" | "false" => TokenKind::Fals,
            "nul" | "null" => TokenKind::Nul,
            "let" => TokenKind::Let,
            "and" => TokenKind::And,
            "or" => TokenKind::Or,
            "not" => TokenKind::Not,
            "cls" | "class" => TokenKind::Cls,
            "new" => TokenKind::New,
            "ths" | "this" => TokenKind::Ths,
            "in" => TokenKind::In,
            "try" => TokenKind::Try,
            "ctch" | "catch" => TokenKind::Catch,
            "finl" | "finally" => TokenKind::Finally,
            "thro" | "throw" => TokenKind::Throw,
            "mtch" | "match" => TokenKind::Match,
            "whn" | "when" => TokenKind::When,
            "ext" | "extends" => TokenKind::Extends,
            "lam" | "lambda" | "fn" => TokenKind::Lam,
            "awt" | "await" => TokenKind::Await,
            _ => TokenKind::Ident,
        };
        self.make_token(kind, s, line, column)
    }

    fn current(&self) -> char { self.source[self.pos] }
    fn advance(&mut self) {
        let ch = self.source[self.pos];
        self.last_line = self.line;
        self.last_column = self.column;
        self.pos += 1;
        if ch == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
    }
    fn is_at_end(&self) -> bool { self.pos >= self.source.len() }
    fn peek_next(&self) -> Option<char> { self.source.get(self.pos + 1).copied() }
    fn peek(&self, offset: usize) -> Option<char> { self.source.get(self.pos + offset).copied() }
    fn position(&self) -> (usize, usize) { (self.line, self.column) }

    fn make_token(&self, kind: TokenKind, lexeme: impl Into<String>, line: usize, column: usize) -> Token {
        Token::new(kind, lexeme, line, column, self.last_line, self.last_column)
    }
}
