use chrono::{Datelike, NaiveDate, TimeZone, Timelike, Utc};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_truncate_time(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "truncate_time() expects 2 arguments",
        ));
    }

    let dt = crate::extract_timestamp(&args[0])?;
    let unit = match &args[1] {
        Value::String(s) => s.as_str(),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "truncate_time() unit must be string",
            ))
        }
    };

    let truncated = match unit {
        "second" => dt,
        "minute" => Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day()).unwrap().and_hms_opt(dt.hour(), dt.minute(), 0).unwrap()),
        "hour" => Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day()).unwrap().and_hms_opt(dt.hour(), 0, 0).unwrap()),
        "day" => Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day()).unwrap().and_hms_opt(0, 0, 0).unwrap()),
        "week" => {
            let weekday = dt.weekday();
            let days_to_subtract = weekday.num_days_from_monday() as i64;
            let week_start = dt.date_naive() - chrono::Duration::days(days_to_subtract);
            Utc.from_utc_datetime(&week_start.and_hms_opt(0, 0, 0).unwrap())
        }
        "month" => Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).unwrap().and_hms_opt(0, 0, 0).unwrap()),
        "year" => Utc.from_utc_datetime(&NaiveDate::from_ymd_opt(dt.year(), 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()),
        _ => return Err(dsq_shared::error::operation_error("truncate_time() invalid unit. Valid units: second, minute, hour, day, week, month, year")),
    };
    Ok(Value::String(truncated.to_rfc3339()))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "truncate_time",
        func: builtin_truncate_time,
    }
}
