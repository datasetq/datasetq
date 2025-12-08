use crate::Value;

/// Check if a value supports a specific type of operation
///
/// This function provides a way to determine compatibility before
/// attempting to apply operations.
#[must_use]
pub fn supports_operation(value: &Value, operation_type: OperationType) -> bool {
    matches!(
        (value, operation_type),
        (Value::DataFrame(_) | Value::LazyFrame(_), _)
            | (
                Value::Array(_),
                OperationType::Basic
                    | OperationType::Aggregate
                    | OperationType::Transform
                    | OperationType::Filter
                    | OperationType::Join,
            )
            | (
                Value::Object(_),
                OperationType::Basic | OperationType::Transform | OperationType::Filter,
            )
            | (
                Value::Series(_),
                OperationType::Basic | OperationType::Transform
            )
    )
}

/// Types of operations supported by dsq
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Basic operations like select, filter, sort
    Basic,
    /// Aggregation operations like group by, sum, mean
    Aggregate,
    /// Join operations for combining datasets
    Join,
    /// Transformation operations like pivot, transpose
    Transform,
    /// Filter operations for jq-style expressions
    Filter,
}

/// Get the recommended batch size for operations on large datasets
///
/// This function provides guidance on how to chunk large datasets
/// for efficient processing.
#[must_use]
pub fn recommended_batch_size(value: &Value, operation_type: OperationType) -> Option<usize> {
    match value {
        Value::DataFrame(df) => {
            let rows = df.height();
            match operation_type {
                OperationType::Aggregate => {
                    if rows > 500_000 {
                        Some(50_000)
                    } else {
                        None
                    }
                }
                OperationType::Join => {
                    if rows > 100_000 {
                        Some(10_000)
                    } else {
                        None
                    }
                }
                OperationType::Basic | OperationType::Filter | OperationType::Transform => {
                    if rows > 1_000_000 {
                        Some(100_000)
                    } else {
                        None
                    }
                }
            }
        }
        Value::Array(arr) => {
            let len = arr.len();
            match operation_type {
                OperationType::Aggregate => {
                    if len > 50_000 {
                        Some(5_000)
                    } else {
                        None
                    }
                }
                OperationType::Join => {
                    if len > 10_000 {
                        Some(1_000)
                    } else {
                        None
                    }
                }
                OperationType::Basic | OperationType::Filter | OperationType::Transform => {
                    if len > 100_000 {
                        Some(10_000)
                    } else {
                        None
                    }
                }
            }
        }
        _ => None,
    }
}
