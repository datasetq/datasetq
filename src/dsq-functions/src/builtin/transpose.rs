use crate::FunctionRegistration;
use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;

inventory::submit! {
    FunctionRegistration {
        name: "transpose",
        func: builtin_transpose,
    }
}

pub fn builtin_transpose(args: &[Value]) -> Result<Value> {
    if args.len() != 1 {
        return Err(dsq_shared::error::operation_error(
            "transpose() expects 1 argument",
        ));
    }

    match &args[0] {
        Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Value::Array(Vec::new()));
            }

            // Check if all elements are arrays (rows)
            let rows: Vec<&Vec<Value>> = arr
                .iter()
                .filter_map(|row| match row {
                    Value::Array(arr) => Some(arr),
                    _ => None,
                })
                .collect();

            if rows.len() != arr.len() {
                return Err(dsq_shared::error::operation_error(
                    "transpose() argument must be a 2D array (all elements must be arrays)",
                ));
            }

            if rows.is_empty() {
                return Ok(Value::Array(Vec::new()));
            }

            let row_len = rows[0].len();
            // Check if all rows have the same length
            if !rows.iter().all(|row| row.len() == row_len) {
                return Err(dsq_shared::error::operation_error(
                    "transpose() all rows must have the same length",
                ));
            }

            let mut result = Vec::with_capacity(row_len);
            for col in 0..row_len {
                let new_row: Vec<Value> = rows.iter().map(|row| row[col].clone()).collect();
                result.push(Value::Array(new_row));
            }

            Ok(Value::Array(result))
        }
        Value::DataFrame(df) => {
            // Transpose DataFrame - need to clone since transpose takes &mut self
            let mut df_clone = df.clone();
            match df_clone.transpose(None, None) {
                Ok(transposed_df) => Ok(Value::DataFrame(transposed_df)),
                Err(e) => Err(dsq_shared::error::operation_error(format!(
                    "transpose() failed: {}",
                    e
                ))),
            }
        }
        _ => Err(dsq_shared::error::operation_error(
            "transpose() requires array or DataFrame",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use polars::prelude::{DataFrame, NamedFrom, Series};

    #[test]
    fn test_builtin_transpose_array() {
        // Test with 2x3 matrix
        let row1 = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let row2 = vec![Value::Int(4), Value::Int(5), Value::Int(6)];
        let matrix = vec![Value::Array(row1), Value::Array(row2)];
        let result = builtin_transpose(&[Value::Array(matrix)]).unwrap();

        if let Value::Array(transposed) = result {
            assert_eq!(transposed.len(), 3);
            // Check first column
            if let Value::Array(col1) = &transposed[0] {
                assert_eq!(col1.len(), 2);
                assert_eq!(col1[0], Value::Int(1));
                assert_eq!(col1[1], Value::Int(4));
            } else {
                panic!("Expected array");
            }
            // Check second column
            if let Value::Array(col2) = &transposed[1] {
                assert_eq!(col2.len(), 2);
                assert_eq!(col2[0], Value::Int(2));
                assert_eq!(col2[1], Value::Int(5));
            } else {
                panic!("Expected array");
            }
            // Check third column
            if let Value::Array(col3) = &transposed[2] {
                assert_eq!(col3.len(), 2);
                assert_eq!(col3[0], Value::Int(3));
                assert_eq!(col3[1], Value::Int(6));
            } else {
                panic!("Expected array");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[test]
    fn test_builtin_transpose_empty_array() {
        let arr = vec![];
        let result = builtin_transpose(&[Value::Array(arr)]).unwrap();
        assert_eq!(result, Value::Array(Vec::new()));
    }

    #[test]
    fn test_builtin_transpose_invalid_args() {
        // No arguments
        let result = builtin_transpose(&[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));

        // Too many arguments
        let result = builtin_transpose(&[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }

    #[test]
    fn test_builtin_transpose_non_2d_array() {
        // Array with non-array elements
        let arr = vec![Value::Int(1), Value::Int(2), Value::Int(3)];
        let result = builtin_transpose(&[Value::Array(arr)]).unwrap_err();
        assert!(result.to_string().contains("must be a 2D array"));
    }

    #[test]
    fn test_builtin_transpose_uneven_rows() {
        // Rows with different lengths
        let row1 = vec![Value::Int(1), Value::Int(2)];
        let row2 = vec![Value::Int(3)];
        let matrix = vec![Value::Array(row1), Value::Array(row2)];
        let result = builtin_transpose(&[Value::Array(matrix)]).unwrap_err();
        assert!(result.to_string().contains("same length"));
    }

    #[test]
    fn test_builtin_transpose_dataframe() {
        // Create a simple DataFrame
        let series1 = Series::new("col1".into().into(), vec![1i64, 2]);
        let series2 = Series::new("col2".into().into(), vec![3i64, 4]);
        let df = DataFrame::new(vec![series1, series2]).unwrap();
        let result = builtin_transpose(&[Value::DataFrame(df)]).unwrap();

        if let Value::DataFrame(transposed_df) = result {
            // After transpose, should have 2 rows and 2 columns
            assert_eq!(transposed_df.height(), 2);
            assert_eq!(transposed_df.width(), 2);
        } else {
            panic!("Expected DataFrame");
        }
    }

    #[test]
    fn test_builtin_transpose_invalid_type() {
        let result = builtin_transpose(&[Value::Int(42)]).unwrap_err();
        assert!(result.to_string().contains("requires array or DataFrame"));
    }

    #[test]
    fn test_transpose_registered_via_inventory() {
        use crate::BuiltinRegistry;
        let registry = BuiltinRegistry::new();
        assert!(registry.functions.contains_key("transpose"));
    }
}
