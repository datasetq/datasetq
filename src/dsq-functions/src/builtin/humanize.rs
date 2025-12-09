use crate::inventory;
use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;

pub fn builtin_humanize(args: &[Value]) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(dsq_shared::error::operation_error(
            "humanize() expects 1 or 2 arguments",
        ));
    }

    let value = &args[0];
    let format = if args.len() == 2 {
        match &args[1] {
            Value::String(s) => Some(s.as_str()),
            _ => {
                return Err(dsq_shared::error::operation_error(
                    "humanize() format must be a string",
                ));
            }
        }
    } else {
        None
    };

    match format {
        Some("number") => format_number(value),
        Some("currency") => format_currency(value),
        Some("date") => format_date(value),
        Some("bytes") => format_bytes(value),
        Some("percentage") => format_percentage(value),
        Some(fmt) => Err(dsq_shared::error::operation_error(format!(
            "humanize() unknown format: {}",
            fmt
        ))),
        None => auto_format(value),
    }
}

fn format_number(value: &Value) -> Result<Value> {
    match value {
        Value::Int(i) => Ok(Value::String(format_number_i64(*i))),
        Value::Float(f) => Ok(Value::String(format_number_f64(*f))),
        _ => Ok(Value::String(value.to_string())),
    }
}

fn format_number_i64(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let mut count = 0;
    for c in s.chars().rev() {
        if count == 3 {
            result.push(',');
            count = 0;
        }
        result.push(c);
        if c != '-' {
            count += 1;
        }
    }
    result.chars().rev().collect()
}

fn format_number_f64(n: f64) -> String {
    if n.fract() == 0.0 {
        format_number_i64(n as i64)
    } else {
        // Format with commas for the integer part
        let integer_part = n.trunc() as i64;
        let decimal_part = n.fract();
        let formatted_int = format_number_i64(integer_part);
        format!(
            "{}.{}",
            formatted_int,
            format!("{:.2}", decimal_part)
                .split('.')
                .nth(1)
                .unwrap_or("00")
        )
    }
}

fn format_currency(value: &Value) -> Result<Value> {
    let amount = match value {
        Value::Int(i) => *i as f64 / 100.0,
        Value::Float(f) => *f,
        _ => return Ok(Value::String(value.to_string())),
    };

    // Format as currency with commas
    let int_part = amount.trunc() as i64;
    let decimal_part = ((amount - int_part as f64) * 100.0).round() as i32;
    let formatted_int = format_number_i64(int_part);
    Ok(Value::String(format!(
        "${}.{:02}",
        formatted_int, decimal_part
    )))
}

fn format_date(value: &Value) -> Result<Value> {
    match value {
        Value::String(s) => {
            // Try to parse as date
            if let Ok(dt) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                Ok(Value::String(dt.format("%B %e, %Y").to_string()))
            } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                Ok(Value::String(dt.format("%B %e, %Y").to_string()))
            } else {
                Ok(Value::String(s.clone()))
            }
        }
        Value::Int(i) => {
            // Assume timestamp
            let dt = chrono::DateTime::from_timestamp(*i, 0)
                .unwrap_or_default()
                .naive_utc();
            Ok(Value::String(dt.format("%B %e, %Y").to_string()))
        }
        _ => Ok(Value::String(value.to_string())),
    }
}

fn format_bytes(value: &Value) -> Result<Value> {
    let bytes = match value {
        Value::Int(i) => *i as f64,
        Value::Float(f) => *f,
        _ => return Ok(Value::String(value.to_string())),
    };

    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    let mut size = bytes;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        Ok(Value::String(format!("{:.0} {}", size, UNITS[unit_idx])))
    } else {
        Ok(Value::String(format!("{:.1} {}", size, UNITS[unit_idx])))
    }
}

fn format_percentage(value: &Value) -> Result<Value> {
    match value {
        Value::Int(i) => Ok(Value::String(format!("{}%", i))),
        Value::Float(f) => Ok(Value::String(format!("{:.1}%", f * 100.0))),
        _ => Ok(Value::String(value.to_string())),
    }
}

fn auto_format(value: &Value) -> Result<Value> {
    match value {
        Value::Int(i) => {
            // Check if it's a power of 1024 (likely bytes)
            if *i > 0 && (*i as f64).log2() / 1024.0_f64.log2() % 1.0 < 1e-10 {
                format_bytes(value)
            } else {
                format_number(value)
            }
        }
        Value::Float(f) => {
            if f.fract() == 0.0 {
                let int_val = *f as i64;
                // Check if it's a power of 1024 (likely bytes)
                if int_val > 0 && (int_val as f64).log2() / 1024.0_f64.log2() % 1.0 < 1e-10 {
                    format_bytes(value)
                } else {
                    format_number(value)
                }
            } else {
                Ok(Value::String(f.to_string()))
            }
        }
        Value::String(s) => {
            // Try to detect if it's a date string
            if s.contains('-') && s.len() >= 8 {
                format_date(value)
            } else {
                Ok(Value::String(s.clone()))
            }
        }
        _ => Ok(Value::String(value.to_string())),
    }
}

inventory::submit! {
    FunctionRegistration {
        name: "humanize",
        func: builtin_humanize,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_humanize_number() {
        let result =
            builtin_humanize(&[Value::Int(1234567), Value::String("number".to_string())]).unwrap();
        assert_eq!(result, Value::String("1,234,567".to_string()));

        let result =
            builtin_humanize(&[Value::Int(123456), Value::String("currency".to_string())]).unwrap();
        assert_eq!(result, Value::String("$1,234.56".to_string()));

        let result =
            builtin_humanize(&[Value::Int(1048576), Value::String("bytes".to_string())]).unwrap();
        assert_eq!(result, Value::String("1.0 MB".to_string()));

        let result =
            builtin_humanize(&[Value::Float(0.85), Value::String("percentage".to_string())])
                .unwrap();
        assert_eq!(result, Value::String("85.0%".to_string()));

        let result = builtin_humanize(&[
            Value::String("2023-12-25".to_string()),
            Value::String("date".to_string()),
        ])
        .unwrap();
        assert_eq!(result, Value::String("December 25, 2023".to_string()));
    }

    #[test]
    fn test_builtin_humanize_auto() {
        let result = builtin_humanize(&[Value::Int(1234567)]).unwrap();
        assert_eq!(result, Value::String("1,234,567".to_string()));

        let result = builtin_humanize(&[Value::Int(1048576)]).unwrap();
        assert_eq!(result, Value::String("1.0 MB".to_string()));

        let result = builtin_humanize(&[Value::String("2023-12-25".to_string())]).unwrap();
        assert_eq!(result, Value::String("December 25, 2023".to_string()));
    }

    #[test]
    fn test_builtin_humanize_invalid_args() {
        let result = builtin_humanize(&[]);
        assert!(result.is_err());

        let result = builtin_humanize(&[
            Value::Int(1),
            Value::String("number".to_string()),
            Value::Int(2),
        ]);
        assert!(result.is_err());

        let result = builtin_humanize(&[Value::Int(1), Value::String("invalid".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_humanize_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("humanize"));
    }
}
