use chrono::{Datelike, NaiveDate, TimeZone, Timelike, Utc};
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

pub fn builtin_truncate_date(args: &[Value]) -> Result<Value> {
    if args.len() != 2 {
        return Err(dsq_shared::error::operation_error(
            "truncate_date() expects 2 arguments",
        ));
    }

    let dt = crate::extract_timestamp(&args[0])?;
    let unit = match &args[1] {
        Value::String(s) => s.as_str(),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "truncate_date() unit must be string",
            ))
        }
    };

    let truncated = match unit {
        "year" => Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(dt.year(), 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        "month" => Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        "day" => Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ),
        "hour" => Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                .unwrap()
                .and_hms_opt(dt.hour(), 0, 0)
                .unwrap(),
        ),
        "minute" => Utc.from_utc_datetime(
            &NaiveDate::from_ymd_opt(dt.year(), dt.month(), dt.day())
                .unwrap()
                .and_hms_opt(dt.hour(), dt.minute(), 0)
                .unwrap(),
        ),
        _ => {
            return Err(dsq_shared::error::operation_error(
                "truncate_date() invalid unit",
            ))
        }
    };
    Ok(Value::Int(truncated.timestamp()))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "truncate_date",
        func: builtin_truncate_date,
    }
}
