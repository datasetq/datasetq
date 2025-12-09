use chrono::{DateTime, Duration, TimeZone, Utc};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_time_series_range(args: &[Value]) -> Result<Value> {
    if args.len() != 3 {
        return Err(dsq_shared::error::operation_error(
            "time_series_range() expects 3 arguments: start, end, interval",
        ));
    }

    let start = extract_timestamp(&args[0])?;
    let end = extract_timestamp(&args[1])?;
    let interval = parse_interval(&args[2])?;

    if start >= end {
        return Ok(Value::Array(Vec::new()));
    }

    let mut timestamps = Vec::new();
    let mut current = start;

    while current <= end {
        timestamps.push(Value::Int(current.timestamp()));
        current += interval;
    }

    Ok(Value::Array(timestamps))
}

fn extract_timestamp(value: &Value) -> Result<DateTime<Utc>> {
    match value {
        Value::Int(i) => Utc
            .timestamp_opt(*i, 0)
            .single()
            .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp")),
        Value::Float(f) => {
            let secs = f.trunc() as i64;
            let nanos = (f.fract() * 1_000_000_000.0) as u32;
            Utc.timestamp_opt(secs, nanos)
                .single()
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid timestamp"))
        }
        Value::String(s) => {
            // Try parsing as RFC3339 first
            if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                Ok(dt.with_timezone(&Utc))
            } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                Ok(dt.and_utc())
            } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                Ok(dt.and_utc())
            } else if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                let dt = date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| dsq_shared::error::operation_error("Invalid time components"))?;
                Ok(dt.and_utc())
            } else {
                Err(dsq_shared::error::operation_error(
                    "Unable to parse date/time string",
                ))
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "Unsupported type for timestamp",
        )),
    }
}

fn parse_interval(value: &Value) -> Result<Duration> {
    match value {
        Value::String(s) => {
            // Parse interval like "1s", "2m", "3h", "4d", "5w"
            let len = s.len();
            if len < 2 {
                return Err(dsq_shared::error::operation_error(
                    "Invalid interval format",
                ));
            }
            let (num_str, unit) = s.split_at(len - 1);
            let num: i64 = num_str
                .parse()
                .map_err(|_| dsq_shared::error::operation_error("Invalid interval number"))?;
            match unit {
                "s" => Ok(Duration::seconds(num)),
                "m" => Ok(Duration::minutes(num)),
                "h" => Ok(Duration::hours(num)),
                "d" => Ok(Duration::days(num)),
                "w" => Ok(Duration::weeks(num)),
                _ => Err(dsq_shared::error::operation_error(
                    "Invalid interval unit. Use s, m, h, d, or w",
                )),
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "Interval must be a string",
        )),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "time_series_range",
        func: builtin_time_series_range,
    }
}
