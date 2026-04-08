//! Transform engine: declarative JSON extraction with filter pipelines.
//!
//! Replaces external `jq` dependency for common JSON-to-JSON mapping tasks.

use indexmap::IndexMap;
use serde_json::Value;

use crate::error::{Result, RoutineError};

/// Apply a transform: select a subtree from `input`, then map fields via filter pipelines.
pub fn apply(
    input: &Value,
    select: Option<&str>,
    mapping: Option<&IndexMap<String, String>>,
) -> Result<Value> {
    // Step 1: select (supports filter pipeline via `|`)
    let selected = match select {
        Some(expr) if expr.contains('|') => evaluate_expr(input, expr)?,
        Some(path) => navigate(input, path)?,
        None => input.clone(),
    };

    // Step 2: mapping
    let Some(mapping) = mapping else {
        return Ok(selected);
    };

    match &selected {
        Value::Array(arr) => {
            let mut results = Vec::with_capacity(arr.len());
            for (index, item) in arr.iter().enumerate() {
                // Inject synthetic .item_index for positional access in mappings
                let item_with_index = if let Value::Object(mut map) = item.clone() {
                    map.insert(
                        "item_index".to_string(),
                        Value::Number(serde_json::Number::from(index)),
                    );
                    Value::Object(map)
                } else {
                    item.clone()
                };
                results.push(apply_mapping(&item_with_index, mapping)?);
            }
            Ok(Value::Array(results))
        }
        _ => apply_mapping(&selected, mapping),
    }
}

/// Apply a mapping to a single JSON value, producing an object.
fn apply_mapping(value: &Value, mapping: &IndexMap<String, String>) -> Result<Value> {
    let mut obj = serde_json::Map::new();
    for (key, expr) in mapping {
        let result = evaluate_expr(value, expr)?;
        obj.insert(key.clone(), result);
    }
    Ok(Value::Object(obj))
}

/// Public wrapper for evaluate_expr (used by executor for template mode).
pub fn evaluate_expr_pub(value: &Value, expr: &str) -> Result<Value> {
    evaluate_expr(value, expr)
}

/// Public wrapper for navigate (used by executor for template mode).
pub fn navigate_pub(value: &Value, path: &str) -> Result<Value> {
    navigate(value, path)
}

/// Apply a multi-field template to a JSON value, replacing `{{ .path | filter }}` placeholders.
/// Returns a plain text string (not JSON).
pub fn apply_template(input: &Value, template: &str) -> Result<String> {
    let mut result = String::new();
    let mut rest = template;

    while let Some(start) = rest.find("{{") {
        // Text before the placeholder
        result.push_str(&rest[..start]);
        rest = &rest[start + 2..];

        if let Some(end) = rest.find("}}") {
            let expr = rest[..end].trim();
            let value = evaluate_expr(input, expr)?;
            // Render value as plain text (not JSON-quoted)
            match &value {
                Value::String(s) => result.push_str(s),
                Value::Null => {}
                other => result.push_str(&other.to_string()),
            }
            rest = &rest[end + 2..];
        } else {
            // No closing }}, output literally
            result.push_str("{{");
        }
    }
    result.push_str(rest);

    // Handle escape sequences
    let result = result.replace("\\n", "\n").replace("\\t", "\t");

    Ok(result)
}

/// Navigate a JSON value by a dot-path like `.data.items[0].name`.
/// Handles `[*]` wildcard: applies remaining path to each array element.
fn navigate(value: &Value, path: &str) -> Result<Value> {
    let segments = parse_path(path)?;
    navigate_segments(value, &segments)
}

fn navigate_segments(value: &Value, segments: &[PathSegment]) -> Result<Value> {
    let mut current = value.clone();
    for (i, seg) in segments.iter().enumerate() {
        match seg {
            PathSegment::Wildcard => {
                // Apply remaining segments to each element of the array
                let remaining = &segments[i + 1..];
                return match &current {
                    Value::Array(arr) => {
                        let results: Result<Vec<Value>> = arr
                            .iter()
                            .map(|item| navigate_segments(item, remaining))
                            .collect();
                        Ok(Value::Array(results?))
                    }
                    _ => Ok(Value::Null),
                };
            }
            other => {
                current = apply_segment(&current, other)?;
            }
        }
    }
    Ok(current)
}

/// Evaluate a full expression: path + filter pipeline.
/// Format: `.path.to.field | filter1 | filter2(arg)`
fn evaluate_expr(value: &Value, expr: &str) -> Result<Value> {
    let parts: Vec<&str> = split_pipeline(expr);
    if parts.is_empty() {
        return Ok(value.clone());
    }

    // First part is the path
    let mut current = navigate(value, parts[0].trim())?;

    // Remaining parts are filters
    for &filter_str in &parts[1..] {
        current = apply_filter(&current, filter_str.trim())?;
    }

    Ok(current)
}

/// Split an expression by `|` respecting parenthesized arguments.
fn split_pipeline(expr: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0u32;
    let mut start = 0;
    let bytes = expr.as_bytes();

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => depth = depth.saturating_sub(1),
            b'|' if depth == 0 => {
                parts.push(&expr[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&expr[start..]);
    parts
}

// --- Path parsing ---

#[derive(Debug, Clone)]
enum PathSegment {
    Field(String),
    Index(i64),
    Slice(i64, i64), // [start:end]
    Wildcard,        // [*]
}

fn parse_path(path: &str) -> Result<Vec<PathSegment>> {
    let path = path.trim();
    if path == "." || path.is_empty() {
        return Ok(vec![]);
    }

    let mut segments = Vec::new();
    let mut chars = path.chars().peekable();

    // Skip leading dot
    if chars.peek() == Some(&'.') {
        chars.next();
    }

    let mut buf = String::new();

    while let Some(&ch) = chars.peek() {
        match ch {
            '.' => {
                if !buf.is_empty() {
                    segments.push(PathSegment::Field(buf.clone()));
                    buf.clear();
                }
                chars.next();
            }
            '[' => {
                if !buf.is_empty() {
                    segments.push(PathSegment::Field(buf.clone()));
                    buf.clear();
                }
                chars.next(); // consume '['
                let mut idx_buf = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        chars.next();
                        break;
                    }
                    idx_buf.push(c);
                    chars.next();
                }
                if idx_buf == "*" {
                    segments.push(PathSegment::Wildcard);
                } else if let Some((start_str, end_str)) = idx_buf.split_once(':') {
                    let start: i64 = if start_str.is_empty() {
                        0
                    } else {
                        start_str.parse().map_err(|_| RoutineError::Transform {
                            step_id: String::new(),
                            message: format!("invalid slice start: {start_str}"),
                        })?
                    };
                    let end: i64 = if end_str.is_empty() {
                        i64::MAX
                    } else {
                        end_str.parse().map_err(|_| RoutineError::Transform {
                            step_id: String::new(),
                            message: format!("invalid slice end: {end_str}"),
                        })?
                    };
                    segments.push(PathSegment::Slice(start, end));
                } else {
                    let idx: i64 = idx_buf.parse().map_err(|_| RoutineError::Transform {
                        step_id: String::new(),
                        message: format!("invalid array index: {idx_buf}"),
                    })?;
                    segments.push(PathSegment::Index(idx));
                }
            }
            _ => {
                buf.push(ch);
                chars.next();
            }
        }
    }

    if !buf.is_empty() {
        segments.push(PathSegment::Field(buf));
    }

    Ok(segments)
}

fn apply_segment(value: &Value, segment: &PathSegment) -> Result<Value> {
    match segment {
        PathSegment::Field(key) => match value.get(key.as_str()) {
            Some(v) => Ok(v.clone()),
            None => Ok(Value::Null),
        },
        PathSegment::Index(idx) => match value {
            Value::Array(arr) => {
                let actual_idx = if *idx < 0 {
                    (arr.len() as i64 + idx) as usize
                } else {
                    *idx as usize
                };
                Ok(arr.get(actual_idx).cloned().unwrap_or(Value::Null))
            }
            _ => Ok(Value::Null),
        },
        PathSegment::Slice(start, end) => match value {
            Value::Array(arr) => {
                let len = arr.len() as i64;
                let s = if *start < 0 {
                    (len + start).max(0) as usize
                } else {
                    (*start as usize).min(arr.len())
                };
                let e = if *end == i64::MAX {
                    arr.len()
                } else if *end < 0 {
                    (len + end).max(0) as usize
                } else {
                    (*end as usize).min(arr.len())
                };
                Ok(Value::Array(arr[s..e].to_vec()))
            }
            _ => Ok(Value::Null),
        },
        PathSegment::Wildcard => match value {
            Value::Array(arr) => Ok(Value::Array(arr.clone())),
            _ => Ok(Value::Null),
        },
    }
}

// --- Filters ---

fn apply_filter(value: &Value, filter: &str) -> Result<Value> {
    let filter = filter.trim();

    // Parse filter name and optional args
    let (name, args) = if let Some(paren_start) = filter.find('(') {
        let name = filter[..paren_start].trim();
        let args_str = filter[paren_start + 1..].trim_end_matches(')');
        (name, Some(args_str))
    } else {
        (filter, None)
    };

    // Null safety: specific filters coerce null to zero-values, others propagate null
    if value.is_null() {
        return match name {
            "to_int" | "ceil_div" => Ok(Value::Number(serde_json::Number::from(0i64))),
            "to_float" => Ok(serde_json::to_value(0.0f64).unwrap_or(Value::Null)),
            "to_string" | "trim" | "slice" => Ok(Value::String(String::new())),
            "range" => Ok(Value::Array(Vec::new())),
            "flatten" | "take" => Ok(Value::Array(Vec::new())),
            "default" => {
                let default_val = parse_string_arg(args.unwrap_or(""));
                Ok(Value::String(default_val))
            }
            _ => Ok(Value::Null),
        };
    }

    match name {
        // Type conversion
        "to_int" => {
            let n = value_to_f64(value)?;
            Ok(Value::Number(serde_json::Number::from(n as i64)))
        }
        "to_float" => {
            let n = value_to_f64(value)?;
            Ok(serde_json::to_value(n).unwrap_or(Value::Null))
        }
        "to_string" => Ok(Value::String(value_to_string(value))),

        // String ops
        "trim" => Ok(Value::String(value_to_string(value).trim().to_string())),
        "slice" => {
            let args = args.ok_or_else(|| RoutineError::Transform {
                step_id: String::new(),
                message: "slice requires (start, end) arguments".into(),
            })?;
            let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
            if parts.len() != 2 {
                return Err(RoutineError::Transform {
                    step_id: String::new(),
                    message: "slice requires exactly 2 arguments".into(),
                });
            }
            let start: usize = parts[0].parse().unwrap_or(0);
            let end: usize = parts[1].parse().unwrap_or(0);
            let s = value_to_string(value);
            let sliced: String = s
                .chars()
                .skip(start)
                .take(end.saturating_sub(start))
                .collect();
            Ok(Value::String(sliced))
        }
        "split" => {
            let sep = parse_string_arg(args.unwrap_or(""));
            let s = value_to_string(value);
            let parts: Vec<Value> = s
                .split(&sep)
                .map(|p| Value::String(p.to_string()))
                .collect();
            Ok(Value::Array(parts))
        }
        "join" => {
            let sep = parse_string_arg(args.unwrap_or(""));
            match value {
                Value::Array(arr) => {
                    let joined: String = arr
                        .iter()
                        .map(value_to_string)
                        .collect::<Vec<_>>()
                        .join(&sep);
                    Ok(Value::String(joined))
                }
                _ => Ok(value.clone()),
            }
        }
        "replace" => {
            let args = args.ok_or_else(|| RoutineError::Transform {
                step_id: String::new(),
                message: "replace requires (old, new) arguments".into(),
            })?;
            let (old, new) = parse_two_string_args(args)?;
            let s = value_to_string(value);
            Ok(Value::String(s.replace(&old, &new)))
        }

        // Math
        "math" => {
            let expr = args.ok_or_else(|| RoutineError::Transform {
                step_id: String::new(),
                message: "math requires an expression argument".into(),
            })?;
            let current = value_to_f64(value)?;
            let result = eval_math(expr, current)?;
            // Preserve integer type when result has no fractional part
            if result.fract() == 0.0 && result >= i64::MIN as f64 && result <= i64::MAX as f64 {
                Ok(Value::Number(serde_json::Number::from(result as i64)))
            } else {
                Ok(serde_json::to_value(result).unwrap_or(Value::Null))
            }
        }
        "round" => {
            let n = value_to_f64(value)?;
            Ok(Value::Number(serde_json::Number::from(n.round() as i64)))
        }
        "floor" => {
            let n = value_to_f64(value)?;
            Ok(Value::Number(serde_json::Number::from(n.floor() as i64)))
        }
        "ceil" => {
            let n = value_to_f64(value)?;
            Ok(Value::Number(serde_json::Number::from(n.ceil() as i64)))
        }

        // Array ops
        "flatten" => match value {
            Value::Array(arr) => {
                let mut flat = Vec::new();
                for item in arr {
                    if let Value::Array(inner) = item {
                        flat.extend(inner.iter().cloned());
                    } else {
                        flat.push(item.clone());
                    }
                }
                Ok(Value::Array(flat))
            }
            _ => Ok(value.clone()),
        },

        "take" => {
            let n_str = args.ok_or_else(|| RoutineError::Transform {
                step_id: String::new(),
                message: "take requires a count argument".into(),
            })?;
            let n = n_str
                .trim()
                .parse::<usize>()
                .map_err(|_| RoutineError::Transform {
                    step_id: String::new(),
                    message: format!("take: cannot parse '{n_str}' as integer"),
                })?;
            match value {
                Value::Array(arr) => Ok(Value::Array(arr.iter().take(n).cloned().collect())),
                _ => Ok(value.clone()),
            }
        }

        // Integer math
        "ceil_div" => {
            let divisor_str = args.ok_or_else(|| RoutineError::Transform {
                step_id: String::new(),
                message: "ceil_div requires a divisor argument".into(),
            })?;
            let divisor =
                divisor_str
                    .trim()
                    .parse::<i64>()
                    .map_err(|_| RoutineError::Transform {
                        step_id: String::new(),
                        message: format!("ceil_div: cannot parse '{divisor_str}' as integer"),
                    })?;
            if divisor == 0 {
                return Err(RoutineError::Transform {
                    step_id: String::new(),
                    message: "ceil_div: division by zero".into(),
                });
            }
            let n = value_to_f64(value)? as i64;
            let result = (n + divisor - 1) / divisor;
            Ok(Value::Number(serde_json::Number::from(result)))
        }
        "range" => {
            let n = value_to_f64(value)? as i64;
            let arr: Vec<Value> = (1..=n)
                .map(|i| Value::Number(serde_json::Number::from(i)))
                .collect();
            Ok(Value::Array(arr))
        }

        // Formatting
        "duration_fmt" => {
            let minutes = value_to_f64(value)? as i64;
            let h = minutes / 60;
            let m = minutes % 60;
            Ok(Value::String(format!("{h}h{m}m")))
        }
        "default" => {
            let default_val = parse_string_arg(args.unwrap_or(""));
            if value.is_null() {
                Ok(Value::String(default_val))
            } else {
                Ok(value.clone())
            }
        }
        "fmt" => {
            let template = parse_string_arg(args.unwrap_or("{}"));
            let s = value_to_string(value);
            Ok(Value::String(template.replace("{}", &s)))
        }

        _ => {
            const AVAILABLE: &[&str] = &[
                "default", "to_int", "to_float", "slice", "split", "join",
                "replace", "math", "round", "floor", "ceil", "take",
                "ceil_div", "range", "duration_fmt", "fmt",
            ];
            Err(RoutineError::Transform {
                step_id: String::new(),
                message: format!(
                    "unknown filter '{}'. Available: {}",
                    name,
                    AVAILABLE.join(", ")
                ),
            })
        }
    }
}

/// Convert a JSON value to f64.
fn value_to_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Number(n) => Ok(n.as_f64().unwrap_or(0.0)),
        Value::String(s) => s.parse::<f64>().map_err(|_| RoutineError::Transform {
            step_id: String::new(),
            message: format!("cannot convert '{s}' to number"),
        }),
        _ => Err(RoutineError::Transform {
            step_id: String::new(),
            message: format!("cannot convert {value} to number"),
        }),
    }
}

/// Convert a JSON value to string for display.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Parse a single-quoted string argument: `'hello'` → `hello`.
fn parse_string_arg(s: &str) -> String {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'')) || (s.starts_with('"') && s.ends_with('"')) {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Parse two string args: `'old', 'new'` → `("old", "new")`.
fn parse_two_string_args(s: &str) -> Result<(String, String)> {
    // Find the comma that separates two arguments (respecting quotes)
    let mut depth = 0u32;
    let mut in_quote = false;
    let mut quote_char = ' ';

    for (i, ch) in s.char_indices() {
        match ch {
            '\'' | '"' if !in_quote => {
                in_quote = true;
                quote_char = ch;
            }
            c if c == quote_char && in_quote => {
                in_quote = false;
            }
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 && !in_quote => {
                let a = parse_string_arg(&s[..i]);
                let b = parse_string_arg(&s[i + 1..]);
                return Ok((a, b));
            }
            _ => {}
        }
    }

    Err(RoutineError::Transform {
        step_id: String::new(),
        message: format!("expected two arguments separated by comma, got: {s}"),
    })
}

/// Evaluate a simple math expression with `_` as current value placeholder.
/// Supports: +, -, *, /, %
fn eval_math(expr: &str, current: f64) -> Result<f64> {
    let expr = expr.trim().replace('_', &current.to_string());

    // Simple two-operand math: "value op value"
    // Try each operator (order matters: check two-char ops first isn't needed here)
    for &op in &['+', '-', '*', '/', '%'] {
        // Find the operator (skip if it's part of a negative number at the start)
        if let Some(pos) = find_operator(&expr, op) {
            let left: f64 = expr[..pos]
                .trim()
                .parse()
                .map_err(|_| RoutineError::Transform {
                    step_id: String::new(),
                    message: format!("invalid math left operand: {}", &expr[..pos]),
                })?;
            let right: f64 =
                expr[pos + 1..]
                    .trim()
                    .parse()
                    .map_err(|_| RoutineError::Transform {
                        step_id: String::new(),
                        message: format!("invalid math right operand: {}", &expr[pos + 1..]),
                    })?;
            return match op {
                '+' => Ok(left + right),
                '-' => Ok(left - right),
                '*' => Ok(left * right),
                '/' => {
                    if right == 0.0 {
                        Err(RoutineError::Transform {
                            step_id: String::new(),
                            message: "division by zero".into(),
                        })
                    } else {
                        Ok(left / right)
                    }
                }
                '%' => Ok(left % right),
                _ => unreachable!(),
            };
        }
    }

    // If no operator found, try to parse as a number (identity)
    expr.trim()
        .parse::<f64>()
        .map_err(|_| RoutineError::Transform {
            step_id: String::new(),
            message: format!("invalid math expression: {expr}"),
        })
}

/// Find operator position, skipping leading sign.
fn find_operator(expr: &str, op: char) -> Option<usize> {
    let bytes = expr.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b == op as u8 && i > 0 {
            // Look backward past whitespace for a digit/dot/paren
            let prev_non_ws = bytes[..i].iter().rev().find(|&&c| c != b' ');
            if let Some(&p) = prev_non_ws
                && (p.is_ascii_digit() || p == b'.' || p == b')')
            {
                return Some(i);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn select_nested_field() {
        let input = json(r#"{"data": {"items": [1, 2, 3]}}"#);
        let result = apply(&input, Some(".data.items"), None).unwrap();
        assert_eq!(result, json("[1, 2, 3]"));
    }

    #[test]
    fn select_array_index() {
        let input = json(r#"{"arr": [10, 20, 30]}"#);
        let result = apply(&input, Some(".arr[1]"), None).unwrap();
        assert_eq!(result, json("20"));
    }

    #[test]
    fn select_negative_index() {
        let input = json(r#"{"arr": [10, 20, 30]}"#);
        let result = apply(&input, Some(".arr[-1]"), None).unwrap();
        assert_eq!(result, json("30"));
    }

    #[test]
    fn mapping_simple_fields() {
        let input = json(r#"[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]"#);
        let mut mapping = IndexMap::new();
        mapping.insert("n".to_string(), ".name".to_string());
        mapping.insert("a".to_string(), ".age".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(
            result,
            json(r#"[{"n": "Alice", "a": 30}, {"n": "Bob", "a": 25}]"#)
        );
    }

    #[test]
    fn mapping_with_select() {
        let input = json(r#"{"data": {"items": [{"x": 1}, {"x": 2}]}}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("val".to_string(), ".x".to_string());
        let result = apply(&input, Some(".data.items"), Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"[{"val": 1}, {"val": 2}]"#));
    }

    #[test]
    fn select_only_no_mapping() {
        let input = json(r#"{"data": "hello"}"#);
        let result = apply(&input, Some(".data"), None).unwrap();
        assert_eq!(result, json(r#""hello""#));
    }

    #[test]
    fn identity_transform() {
        let input = json(r#"{"a": 1}"#);
        let result = apply(&input, None, None).unwrap();
        assert_eq!(result, input);
    }

    #[test]
    fn filter_to_int() {
        let input = json(r#"{"val": "42"}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("n".to_string(), ".val | to_int".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"n": 42}"#));
    }

    #[test]
    fn filter_slice() {
        let input = json(r#"{"ts": "2026-04-06T10:30:00"}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("time".to_string(), ".ts | slice(11, 16)".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"time": "10:30"}"#));
    }

    #[test]
    fn filter_join() {
        let input = json(r#"{"tags": ["a", "b", "c"]}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("all".to_string(), ".tags | join('/')".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"all": "a/b/c"}"#));
    }

    #[test]
    fn filter_wildcard_join() {
        let input = json(r#"{"segments": [{"code": "CZ3086"}, {"code": "MU5678"}]}"#);
        let mut mapping = IndexMap::new();
        mapping.insert(
            "flights".to_string(),
            ".segments[*].code | join('/')".to_string(),
        );
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"flights": "CZ3086/MU5678"}"#));
    }

    #[test]
    fn filter_math_and_floor() {
        let input = json(r#"{"minutes": 285}"#);
        let mut mapping = IndexMap::new();
        mapping.insert(
            "hours".to_string(),
            ".minutes | math(_ / 60) | floor".to_string(),
        );
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"hours": 4}"#));
    }

    #[test]
    fn filter_duration_fmt() {
        let input = json(r#"{"dur": "285"}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("d".to_string(), ".dur | to_int | duration_fmt".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"d": "4h45m"}"#));
    }

    #[test]
    fn filter_default_on_null() {
        let input = json(r#"{"a": null}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | default('N/A')".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": "N/A"}"#));
    }

    #[test]
    fn filter_default_preserves_value() {
        let input = json(r#"{"a": "hello"}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | default('N/A')".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": "hello"}"#));
    }

    #[test]
    fn filter_replace() {
        let input = json(r#"{"s": "hello world"}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("r".to_string(), ".s | replace('world', 'rust')".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"r": "hello rust"}"#));
    }

    #[test]
    fn filter_fmt() {
        let input = json(r#"{"n": 42}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("s".to_string(), ".n | fmt('value={}')".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"s": "value=42"}"#));
    }

    #[test]
    fn filter_trim() {
        let input = json(r#"{"s": "  hello  "}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("t".to_string(), ".s | trim".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"t": "hello"}"#));
    }

    #[test]
    fn filter_split() {
        let input = json(r#"{"s": "a,b,c"}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("parts".to_string(), ".s | split(',')".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"parts": ["a", "b", "c"]}"#));
    }

    #[test]
    fn unknown_filter_errors() {
        let input = json(r#"{"a": 1}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | nonexistent".to_string());
        let result = apply(&input, None, Some(&mapping));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("unknown filter 'nonexistent'"), "got: {err}");
        assert!(err.contains("Available:"), "should list available filters, got: {err}");
        assert!(err.contains("slice"), "should include 'slice' in available list, got: {err}");
    }

    #[test]
    fn single_object_mapping() {
        let input = json(r#"{"name": "test", "value": 42}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("n".to_string(), ".name".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"n": "test"}"#));
    }

    #[test]
    fn missing_field_returns_null() {
        let input = json(r#"{"a": 1}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".nonexistent".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": null}"#));
    }

    #[test]
    fn filter_chain_to_float_round() {
        let input = json(r#"{"v": "3.7"}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("r".to_string(), ".v | to_float | round".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"r": 4}"#));
    }

    #[test]
    fn search_flight_scenario() {
        let input = json(
            r#"{
            "data": {
                "itemList": [
                    {
                        "ticketPrice": 3466.00,
                        "journeys": [{
                            "journeyType": "直达",
                            "totalDuration": "285",
                            "segments": [
                                {"marketingTransportNo": "CZ3086", "depDateTime": "2026-09-20T10:45:00", "arrDateTime": "2026-09-20T14:30:00"}
                            ]
                        }]
                    }
                ]
            }
        }"#,
        );

        let mut mapping = IndexMap::new();
        mapping.insert("price".to_string(), ".ticketPrice".to_string());
        mapping.insert("type".to_string(), ".journeys[0].journeyType".to_string());
        mapping.insert(
            "duration".to_string(),
            ".journeys[0].totalDuration | to_int | duration_fmt".to_string(),
        );
        mapping.insert(
            "flights".to_string(),
            ".journeys[0].segments[*].marketingTransportNo | join('/')".to_string(),
        );
        mapping.insert(
            "dep".to_string(),
            ".journeys[0].segments[0].depDateTime | slice(11, 16)".to_string(),
        );
        mapping.insert(
            "arr".to_string(),
            ".journeys[0].segments[-1].arrDateTime | slice(11, 16)".to_string(),
        );

        let result = apply(&input, Some(".data.itemList"), Some(&mapping)).unwrap();
        let expected = json(
            r#"[{
            "price": 3466.0,
            "type": "直达",
            "duration": "4h45m",
            "flights": "CZ3086",
            "dep": "10:45",
            "arr": "14:30"
        }]"#,
        );
        assert_eq!(result, expected);
    }

    // B1: null safety tests
    #[test]
    fn null_to_int_returns_zero() {
        let input = json(r#"{"a": null}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | to_int".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": 0}"#));
    }

    #[test]
    fn null_to_float_returns_zero() {
        let input = json(r#"{"a": null}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | to_float".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": 0.0}"#));
    }

    #[test]
    fn null_to_string_returns_empty() {
        let input = json(r#"{"a": null}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | to_string".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": ""}"#));
    }

    #[test]
    fn null_slice_returns_empty() {
        let input = json(r#"{"a": null}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | slice(0, 5)".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": ""}"#));
    }

    #[test]
    fn null_propagates_through_unknown_filters() {
        let input = json(r#"{"a": null}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | join(',')".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": null}"#));
    }

    #[test]
    fn null_pipeline_to_int_no_error() {
        // Real scenario: .comments | to_int where comments is null
        let input = json(r#"{"comments": null}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("count".to_string(), ".comments | to_int".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"count": 0}"#));
    }

    #[test]
    fn null_default_still_works() {
        let input = json(r#"{"a": null}"#);
        let mut mapping = IndexMap::new();
        mapping.insert("v".to_string(), ".a | default('fallback')".to_string());
        let result = apply(&input, None, Some(&mapping)).unwrap();
        assert_eq!(result, json(r#"{"v": "fallback"}"#));
    }

    // B2: flatten tests
    #[test]
    fn flatten_nested_arrays() {
        let input = json(r#"[[1, 2], [3, 4], [5]]"#);
        let result = apply(&input, Some(". | flatten"), None).unwrap();
        assert_eq!(result, json("[1, 2, 3, 4, 5]"));
    }

    #[test]
    fn flatten_mixed_array() {
        let input = json(r#"[[1, 2], 3, [4, 5]]"#);
        let result = apply(&input, Some(". | flatten"), None).unwrap();
        assert_eq!(result, json("[1, 2, 3, 4, 5]"));
    }

    #[test]
    fn flatten_non_array_passthrough() {
        let input = json(r#"{"a": 1}"#);
        let result = apply(&input, Some(". | flatten"), None).unwrap();
        assert_eq!(result, json(r#"{"a": 1}"#));
    }

    // B3: ceil_div + range tests
    #[test]
    fn ceil_div_exact() {
        let input = json("200");
        let result = apply(&input, Some(". | ceil_div(100)"), None).unwrap();
        assert_eq!(result, json("2"));
    }

    #[test]
    fn ceil_div_rounds_up() {
        let input = json("150");
        let result = apply(&input, Some(". | ceil_div(100)"), None).unwrap();
        assert_eq!(result, json("2"));
    }

    #[test]
    fn range_generates_sequence() {
        let input = json("3");
        let result = apply(&input, Some(". | range"), None).unwrap();
        assert_eq!(result, json("[1, 2, 3]"));
    }

    #[test]
    fn ceil_div_then_range_pagination() {
        // Real scenario: NUM=150, pages = ceil(150/100) = 2, range = [1, 2]
        let input = json("150");
        let result = apply(&input, Some(". | ceil_div(100) | range"), None).unwrap();
        assert_eq!(result, json("[1, 2]"));
    }

    #[test]
    fn range_zero_returns_empty() {
        let input = json("0");
        let result = apply(&input, Some(". | range"), None).unwrap();
        assert_eq!(result, json("[]"));
    }

    #[test]
    fn select_array_slice() {
        let input = json(r#"[10, 20, 30, 40, 50]"#);
        let result = apply(&input, Some(".[0:3]"), None).unwrap();
        assert_eq!(result, json("[10, 20, 30]"));
    }

    #[test]
    fn select_array_slice_open_end() {
        let input = json(r#"[10, 20, 30, 40, 50]"#);
        let result = apply(&input, Some(".[2:]"), None).unwrap();
        assert_eq!(result, json("[30, 40, 50]"));
    }

    #[test]
    fn select_array_slice_negative() {
        let input = json(r#"[10, 20, 30, 40, 50]"#);
        let result = apply(&input, Some(".[-2:]"), None).unwrap();
        assert_eq!(result, json("[40, 50]"));
    }

    // Template tests
    #[test]
    fn template_simple_fields() {
        let input = json(r#"{"title": "Hello", "body": "World"}"#);
        let result = apply_template(&input, "# {{ .title }}\n\n{{ .body }}").unwrap();
        assert_eq!(result, "# Hello\n\nWorld");
    }

    #[test]
    fn template_with_filters() {
        let input = json(r#"{"name": "test", "count": "42"}"#);
        let result = apply_template(&input, "{{ .name }}: {{ .count | to_int }}").unwrap();
        assert_eq!(result, "test: 42");
    }

    #[test]
    fn template_nested_fields() {
        let input = json(r#"{"pr": {"title": "Fix bug", "additions": 10, "deletions": 3}}"#);
        let result = apply_template(
            &input,
            "{{ .pr.title }} (+{{ .pr.additions }}/-{{ .pr.deletions }})",
        )
        .unwrap();
        assert_eq!(result, "Fix bug (+10/-3)");
    }

    #[test]
    fn template_null_field_renders_empty() {
        let input = json(r#"{"title": "Hello", "body": null}"#);
        let result = apply_template(&input, "# {{ .title }}\n\n{{ .body }}").unwrap();
        assert_eq!(result, "# Hello\n\n");
    }

    #[test]
    fn template_escape_sequences() {
        let input = json(r#"{"a": "x", "b": "y"}"#);
        let result = apply_template(&input, "{{ .a }}\\n{{ .b }}").unwrap();
        assert_eq!(result, "x\ny");
    }

    // take filter test
    #[test]
    fn take_array() {
        let input = json("[1, 2, 3, 4, 5]");
        let result = apply(&input, Some(". | take(3)"), None).unwrap();
        assert_eq!(result, json("[1, 2, 3]"));
    }
}
