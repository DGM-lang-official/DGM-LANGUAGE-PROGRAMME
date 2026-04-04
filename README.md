# DGM Language

A dynamically typed interpreted language written in Rust.
Named after **Dang Gia Minh** — built from scratch, no parser generators.

## Build

```bash
cargo build --release
```

## Usage

```bash
# Run a script
./dgm run script.dgm

# Interactive REPL
./dgm repl
```

## Example Project

`examples/api-server` là một backend mẫu dùng routing, middleware, bearer auth và JSON file store.

```bash
cd examples/api-server
AUTH_TOKEN=dev-token PORT=3000 ../../target/debug/dgm run
```

## Syntax

```dgm
# Variables
let x = 42
let name = "DGM"

# Functions
def add(a, b) {
    retrun a + b
}

# Loops
fr i in range(10) { writ(i) }
whl x > 0 { x -= 1 }

# Conditionals
iff x > 0 { writ("pos") } elseif x == 0 { writ("zero") } els { writ("neg") }

# Classes
cls Animal {
    def init(name) { ths.name = name }
    def speak() { writ(ths.name) }
}
let dog = new Animal("Rex")
dog.speak()
```

## Keywords

| DGM | Meaning |
|-----|---------|
| `writ` | print |
| `def` | function |
| `retrun` | return |
| `iff` | if |
| `elseif` | else if |
| `els` | else |
| `fr` | for |
| `whl` | while |
| `brk` | break |
| `cont` | continue |
| `tru` / `fals` | true / false |
| `nul` | null |
| `cls` | class |
| `new` | instantiate |
| `ths` | this/self |

## Architecture

```
src/
├── main.rs         # CLI + REPL
├── lexer.rs        # Tokenizer (hand-written)
├── token.rs        # Token definitions
├── parser.rs       # Recursive descent parser
├── ast.rs          # AST node types
├── interpreter.rs  # Tree-walk interpreter + DgmValue
└── environment.rs  # Lexical scoping with Rc<RefCell>
```
