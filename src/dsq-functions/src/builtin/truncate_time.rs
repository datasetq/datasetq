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
            ));
        }
    };

    let truncated = match unit {
        "second" => dt,
        "minute" => {
            let date = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid date"))?;
            let datetime = date
                .and_hms_opt(dt.hour(), dt.minute(), 0)
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid time"))?;
            Utc.from_utc_datetime(&datetime)
        }
        "hour" => {
            let date = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid date"))?;
            let datetime = date
                .and_hms_opt(dt.hour(), 0, 0)
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid time"))?;
            Utc.from_utc_datetime(&datetime)
        }
        "day" => {
            let date = NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid date"))?;
            let datetime = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid time"))?;
            Utc.from_utc_datetime(&datetime)
        }
        "week" => {
            let weekday = dt.weekday();
            let days_to_subtract = weekday.num_days_from_monday() as i64;
            let week_start = dt.date_naive() - chrono::Duration::days(days_to_subtract);
            let datetime = week_start
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid time"))?;
            Utc.from_utc_datetime(&datetime)
        }
        "month" => {
            let date = NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid date"))?;
            let datetime = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid time"))?;
            Utc.from_utc_datetime(&datetime)
        }
        "year" => {
            let date = NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid date"))?;
            let datetime = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| dsq_shared::error::operation_error("Invalid time"))?;
            Utc.from_utc_datetime(&datetime)
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "truncate_time() invalid unit. Valid units: second, minute, hour, day, week, month, year",
            ));
        }
    };
    Ok(Value::String(truncated.to_rfc3339()))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "truncate_time",
        func: builtin_truncate_time,
    }
}
