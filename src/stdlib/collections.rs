use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;
use crate::error::DgmError;
use crate::interpreter::{call_native_hof, reserve_list_growth, reserve_map_growth, runtime_reserve_value, value_key, DgmValue};

pub fn module() -> HashMap<String, DgmValue> {
    let mut m = HashMap::new();
    let fns: &[(&str, fn(Vec<DgmValue>) -> Result<DgmValue, DgmError>)] = &[
        ("new_set", collections_new_set),
        ("set_add", collections_set_add),
        ("set_has", collections_set_has),
        ("set_delete", collections_set_delete),
        ("set_values", collections_set_values),
        ("new_stack", collections_new_stack),
        ("stack_push", collections_stack_push),
        ("stack_pop", collections_stack_pop),
        ("stack_peek", collections_stack_peek),
        ("new_queue", collections_new_queue),
        ("queue_push", collections_queue_push),
        ("queue_pop", collections_queue_pop),
        ("queue_peek", collections_queue_peek),
        ("sort", collections_sort),
        ("sort_by", collections_sort_by),
        ("reverse", collections_reverse),
        ("filter", collections_filter),
        ("map", collections_map),
        ("reduce", collections_reduce),
        ("zip", collections_zip),
    ];
    for (name, func) in fns {
        m.insert(name.to_string(), DgmValue::NativeFunction { name: format!("collections.{}", name), func: *func });
    }
    m
}

fn collections_new_set(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    reserve_map_growth(0, 0)?;
    Ok(DgmValue::Map(Rc::new(RefCell::new(HashMap::new()))))
}

fn collections_set_add(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Map(set)), Some(value)) => {
            let key = value_key(value);
            if !set.borrow().contains_key(&key) {
                reserve_map_growth(1, key.len())?;
            }
            set.borrow_mut().insert(key, value.clone());
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.set_add(set, value) required".into() }),
    }
}

fn collections_set_has(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Map(set)), Some(value)) => Ok(DgmValue::Bool(set.borrow().contains_key(&value_key(value)))),
        _ => Err(DgmError::RuntimeError { msg: "collections.set_has(set, value) required".into() }),
    }
}

fn collections_set_delete(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::Map(set)), Some(value)) => Ok(DgmValue::Bool(set.borrow_mut().remove(&value_key(value)).is_some())),
        _ => Err(DgmError::RuntimeError { msg: "collections.set_delete(set, value) required".into() }),
    }
}

fn collections_set_values(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::Map(set)) => {
            let out = DgmValue::List(Rc::new(RefCell::new(set.borrow().values().cloned().collect())));
            runtime_reserve_value(&out)?;
            Ok(out)
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.set_values(set) required".into() }),
    }
}

fn collections_new_stack(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    reserve_list_growth(0)?;
    Ok(DgmValue::List(Rc::new(RefCell::new(vec![]))))
}

fn collections_stack_push(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(stack)), Some(value)) => {
            reserve_list_growth(1)?;
            stack.borrow_mut().push(value.clone());
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.stack_push(stack, value) required".into() }),
    }
}

fn collections_stack_pop(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(stack)) => stack.borrow_mut().pop().ok_or_else(|| DgmError::RuntimeError { msg: "stack is empty".into() }),
        _ => Err(DgmError::RuntimeError { msg: "collections.stack_pop(stack) required".into() }),
    }
}

fn collections_stack_peek(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(stack)) => stack.borrow().last().cloned().ok_or_else(|| DgmError::RuntimeError { msg: "stack is empty".into() }),
        _ => Err(DgmError::RuntimeError { msg: "collections.stack_peek(stack) required".into() }),
    }
}

fn collections_new_queue(_args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    reserve_list_growth(0)?;
    Ok(DgmValue::List(Rc::new(RefCell::new(vec![]))))
}

fn collections_queue_push(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(queue)), Some(value)) => {
            reserve_list_growth(1)?;
            queue.borrow_mut().push(value.clone());
            Ok(DgmValue::Null)
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.queue_push(queue, value) required".into() }),
    }
}

fn collections_queue_pop(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(queue)) => {
            if queue.borrow().is_empty() {
                return Err(DgmError::RuntimeError { msg: "queue is empty".into() });
            }
            Ok(queue.borrow_mut().remove(0))
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.queue_pop(queue) required".into() }),
    }
}

fn collections_queue_peek(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(queue)) => queue.borrow().first().cloned().ok_or_else(|| DgmError::RuntimeError { msg: "queue is empty".into() }),
        _ => Err(DgmError::RuntimeError { msg: "collections.queue_peek(queue) required".into() }),
    }
}

fn collections_sort(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(items)) => {
            let mut values = items.borrow().clone();
            values.sort_by(compare_values);
            reserve_list_growth(values.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(values))))
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.sort(list) required".into() }),
    }
}

fn collections_sort_by(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(items)), Some(func)) => {
            let mut values = items.borrow().clone();
            let err = Rc::new(RefCell::new(None::<DgmError>));
            let err_ref = Rc::clone(&err);
            values.sort_by(|a, b| match call_native_hof(func, vec![a.clone(), b.clone()]) {
                Ok(DgmValue::Int(n)) if n < 0 => Ordering::Less,
                Ok(DgmValue::Int(n)) if n > 0 => Ordering::Greater,
                Ok(DgmValue::Float(f)) if f < 0.0 => Ordering::Less,
                Ok(DgmValue::Float(f)) if f > 0.0 => Ordering::Greater,
                Ok(DgmValue::Bool(true)) => Ordering::Less,
                Ok(DgmValue::Bool(false)) => Ordering::Greater,
                Ok(_) => Ordering::Equal,
                Err(error) => {
                    *err_ref.borrow_mut() = Some(error);
                    Ordering::Equal
                }
            });
            if let Some(error) = err.borrow_mut().take() {
                return Err(error);
            }
            reserve_list_growth(values.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(values))))
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.sort_by(list, fn) required".into() }),
    }
}

fn collections_reverse(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match args.first() {
        Some(DgmValue::List(items)) => {
            let mut values = items.borrow().clone();
            values.reverse();
            reserve_list_growth(values.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(values))))
        }
        Some(DgmValue::Str(s)) => Ok(DgmValue::Str(s.chars().rev().collect())),
        _ => Err(DgmError::RuntimeError { msg: "collections.reverse(list/str) required".into() }),
    }
}

fn collections_filter(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(items)), Some(func)) => {
            let mut output = vec![];
            for item in items.borrow().iter() {
                if matches!(call_native_hof(func, vec![item.clone()])?, DgmValue::Bool(true)) {
                    output.push(item.clone());
                }
            }
            reserve_list_growth(output.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(output))))
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.filter(list, fn) required".into() }),
    }
}

fn collections_map(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(items)), Some(func)) => {
            let mut output = vec![];
            for item in items.borrow().iter() {
                output.push(call_native_hof(func, vec![item.clone()])?);
            }
            reserve_list_growth(output.len())?;
            Ok(DgmValue::List(Rc::new(RefCell::new(output))))
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.map(list, fn) required".into() }),
    }
}

fn collections_reduce(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1), args.get(2)) {
        (Some(DgmValue::List(items)), Some(init), Some(func)) => {
            let mut acc = init.clone();
            for item in items.borrow().iter() {
                acc = call_native_hof(func, vec![acc, item.clone()])?;
            }
            Ok(acc)
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.reduce(list, init, fn) required".into() }),
    }
}

fn collections_zip(args: Vec<DgmValue>) -> Result<DgmValue, DgmError> {
    match (args.get(0), args.get(1)) {
        (Some(DgmValue::List(a)), Some(DgmValue::List(b))) => {
            let result: Vec<DgmValue> = a.borrow().iter().zip(b.borrow().iter()).map(|(x, y)| DgmValue::List(Rc::new(RefCell::new(vec![x.clone(), y.clone()])))).collect();
            runtime_reserve_value(&DgmValue::List(Rc::new(RefCell::new(result.clone()))))?;
            Ok(DgmValue::List(Rc::new(RefCell::new(result))))
        }
        _ => Err(DgmError::RuntimeError { msg: "collections.zip(list, list) required".into() }),
    }
}

fn compare_values(a: &DgmValue, b: &DgmValue) -> Ordering {
    match (a, b) {
        (DgmValue::Int(x), DgmValue::Int(y)) => x.cmp(y),
        (DgmValue::Float(x), DgmValue::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (DgmValue::Int(x), DgmValue::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (DgmValue::Float(x), DgmValue::Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (DgmValue::Str(x), DgmValue::Str(y)) => x.cmp(y),
        (DgmValue::Bool(x), DgmValue::Bool(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}
