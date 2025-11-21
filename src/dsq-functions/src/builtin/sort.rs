use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

use crate::compare_values_for_sorting;

pub fn builtin_sort(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "sort() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            let mut sorted = arr.clone();
            sorted.sort_by(compare_values_for_sorting);
            Ok(Value::Array(sorted))
        }
        Value::DataFrame(df) => {
            // Sort by first column by default
            if let Some(first_col) = df.get_column_names().first() {
                match df.sort([first_col], false, false) {
                    Ok(sorted_df) => Ok(Value::DataFrame(sorted_df)),
                    Err(e) => Err(dsq_shared::error::operation_error(format!(
                        "sort() failed: {}",
                        e
                    ))),
                }
            } else {
                Ok(args[0].clone())
            }
        }
        Value::Series(series) => {
            // Sort the series
            let sorted_series = series.sort(false);
            Ok(Value::Series(sorted_series))
        }
        _ => Ok(args[0].clone()),
    }
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "sort",
        func: builtin_sort,
    }
}
