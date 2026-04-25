use dsq_shared::value::Value;
use dsq_shared::Result;
use inventory;
use std::collections::HashMap;

// Extract a point (Vec<f64>) from a Value::Array of numerics
fn extract_point(val: &Value) -> Option<Vec<f64>> {
    match val {
        Value::Array(inner) => {
            let coords: Vec<f64> = inner
                .iter()
                .filter_map(|v| match v {
                    Value::Int(i) => Some(*i as f64),
                    Value::Float(f) => Some(*f),
                    _ => None,
                })
                .collect();
            if coords.len() == inner.len() && !coords.is_empty() {
                Some(coords)
            } else {
                None
            }
        }
        Value::Object(map) => {
            // Sort keys for deterministic ordering
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let coords: Vec<f64> = keys
                .iter()
                .filter_map(|k| match map.get(*k) {
                    Some(Value::Int(i)) => Some(*i as f64),
                    Some(Value::Float(f)) => Some(*f),
                    _ => None,
                })
                .collect();
            if coords.len() == map.len() && !coords.is_empty() {
                Some(coords)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn squared_distance(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum()
}

fn assign_clusters(points: &[Vec<f64>], centroids: &[Vec<f64>]) -> Vec<usize> {
    points
        .iter()
        .map(|p| {
            centroids
                .iter()
                .enumerate()
                .min_by(|(_, a), (_, b)| {
                    squared_distance(p, a)
                        .partial_cmp(&squared_distance(p, b))
                        .unwrap()
                })
                .map(|(i, _)| i)
                .unwrap_or(0)
        })
        .collect()
}

fn recompute_centroids(points: &[Vec<f64>], assignments: &[usize], k: usize) -> Vec<Vec<f64>> {
    let dims = points[0].len();
    let mut sums = vec![vec![0.0f64; dims]; k];
    let mut counts = vec![0usize; k];

    for (point, &cluster) in points.iter().zip(assignments.iter()) {
        for (d, &v) in point.iter().enumerate() {
            sums[cluster][d] += v;
        }
        counts[cluster] += 1;
    }

    sums.iter()
        .zip(counts.iter())
        .map(|(sum, &count)| {
            if count == 0 {
                sum.clone()
            } else {
                sum.iter().map(|&s| s / count as f64).collect()
            }
        })
        .collect()
}

fn inertia(points: &[Vec<f64>], assignments: &[usize], centroids: &[Vec<f64>]) -> f64 {
    points
        .iter()
        .zip(assignments.iter())
        .map(|(p, &c)| squared_distance(p, &centroids[c]))
        .sum()
}

/// kmeans(data, k [, max_iter])
///
/// data: Array of arrays (points) or Array of objects (named features)
/// k: number of clusters (Int)
/// max_iter: maximum iterations (default 300)
///
/// Returns Object with keys:
///   centroids   - Array of centroid coordinate arrays
///   assignments - Array of cluster indices (one per input point)
///   inertia     - Sum of squared distances to nearest centroid
///   n_iter      - Number of iterations until convergence
pub fn builtin_kmeans(args: &[Value]) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(dsq_shared::error::operation_error(
            "kmeans() expects 2 or 3 arguments: kmeans(data, k [, max_iter])",
        ));
    }

    let k = match &args[1] {
        Value::Int(i) if *i > 0 => *i as usize,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "kmeans(): k must be a positive integer",
            ))
        }
    };

    let max_iter: usize = match args.get(2) {
        Some(Value::Int(i)) if *i > 0 => *i as usize,
        None => 300,
        _ => {
            return Err(dsq_shared::error::operation_error(
                "kmeans(): max_iter must be a positive integer",
            ))
        }
    };

    // Extract data points from the first argument
    let points: Vec<Vec<f64>> = match &args[0] {
        Value::Array(arr) => {
            let pts: Vec<Vec<f64>> = arr.iter().filter_map(extract_point).collect();
            if pts.len() != arr.len() {
                return Err(dsq_shared::error::operation_error(
                    "kmeans(): all data points must be arrays or objects of numeric values with the same dimension",
                ));
            }
            pts
        }
        Value::DataFrame(df) => {
            let numeric_cols: Vec<String> = df
                .get_column_names()
                .into_iter()
                .filter(|name| {
                    df.column(name.as_str())
                        .map(|c| c.as_materialized_series().dtype().is_numeric())
                        .unwrap_or(false)
                })
                .map(|name| name.to_string())
                .collect();

            if numeric_cols.is_empty() {
                return Err(dsq_shared::error::operation_error(
                    "kmeans(): DataFrame has no numeric columns",
                ));
            }

            let n_rows = df.height();
            (0..n_rows)
                .map(|row_idx| {
                    numeric_cols
                        .iter()
                        .map(|col_name| {
                            df.column(col_name.as_str())
                                .ok()
                                .and_then(|col| {
                                    let s = col.as_materialized_series();
                                    s.cast(&polars::prelude::DataType::Float64).ok().and_then(
                                        |cast| cast.f64().ok().and_then(|ca| ca.get(row_idx)),
                                    )
                                })
                                .unwrap_or(f64::NAN)
                        })
                        .collect()
                })
                .collect()
        }
        Value::LazyFrame(lf) => {
            let df = lf.clone().collect().map_err(|e| {
                dsq_shared::error::operation_error(format!("Failed to collect LazyFrame: {}", e))
            })?;
            return builtin_kmeans(&[Value::DataFrame(df), args[1].clone()]);
        }
        _ => {
            return Err(dsq_shared::error::operation_error(
                "kmeans(): first argument must be an array of points or a DataFrame",
            ))
        }
    };

    let n = points.len();
    if n == 0 {
        return Err(dsq_shared::error::operation_error(
            "kmeans(): data is empty",
        ));
    }
    if k > n {
        return Err(dsq_shared::error::operation_error(format!(
            "kmeans(): k ({}) cannot be greater than number of data points ({})",
            k, n
        )));
    }

    let dims = points[0].len();
    if points.iter().any(|p| p.len() != dims) {
        return Err(dsq_shared::error::operation_error(
            "kmeans(): all data points must have the same number of dimensions",
        ));
    }

    // Kmeans++ initialization: pick first centroid uniformly, then each
    // subsequent centroid with probability proportional to D^2.
    let mut centroids: Vec<Vec<f64>> = Vec::with_capacity(k);
    centroids.push(points[0].clone()); // deterministic seed

    for _ in 1..k {
        let distances: Vec<f64> = points
            .iter()
            .map(|p| {
                centroids
                    .iter()
                    .map(|c| squared_distance(p, c))
                    .fold(f64::INFINITY, f64::min)
            })
            .collect();

        let total: f64 = distances.iter().sum();
        // Pick the point with maximum distance for determinism (no rng dependency)
        let next = distances
            .iter()
            .enumerate()
            .max_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap())
            .map(|(i, _)| i)
            .unwrap_or(0);

        // Avoid duplicate centroids when total distance is 0
        if total == 0.0 {
            centroids.push(points[centroids.len()].clone());
        } else {
            centroids.push(points[next].clone());
        }
    }

    // Iterate
    let mut assignments = assign_clusters(&points, &centroids);
    let mut n_iter = 0;

    for iter in 0..max_iter {
        n_iter = iter + 1;
        let new_centroids = recompute_centroids(&points, &assignments, k);
        let new_assignments = assign_clusters(&points, &new_centroids);

        centroids = new_centroids;
        if new_assignments == assignments {
            break;
        }
        assignments = new_assignments;
    }

    let final_inertia = inertia(&points, &assignments, &centroids);

    let centroid_values: Vec<Value> = centroids
        .iter()
        .map(|c| Value::Array(c.iter().map(|&v| Value::Float(v)).collect()))
        .collect();

    let assignment_values: Vec<Value> = assignments.iter().map(|&a| Value::Int(a as i64)).collect();

    let mut result: HashMap<String, Value> = HashMap::new();
    result.insert("centroids".to_string(), Value::Array(centroid_values));
    result.insert("assignments".to_string(), Value::Array(assignment_values));
    result.insert("inertia".to_string(), Value::Float(final_inertia));
    result.insert("n_iter".to_string(), Value::Int(n_iter as i64));

    Ok(Value::Object(result))
}

inventory::submit! {
    crate::FunctionRegistration {
        name: "kmeans",
        func: builtin_kmeans,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(coords: &[f64]) -> Value {
        Value::Array(coords.iter().map(|&v| Value::Float(v)).collect())
    }

    fn make_data(points: &[&[f64]]) -> Value {
        Value::Array(points.iter().map(|p| pt(p)).collect())
    }

    #[test]
    fn test_kmeans_two_clear_clusters() {
        // Two well-separated groups
        let data = make_data(&[
            &[0.0, 0.0],
            &[0.1, 0.1],
            &[0.0, 0.1],
            &[10.0, 10.0],
            &[10.1, 9.9],
            &[9.9, 10.1],
        ]);

        let result = builtin_kmeans(&[data, Value::Int(2)]).unwrap();

        if let Value::Object(map) = result {
            // Should converge quickly
            let n_iter = match map.get("n_iter").unwrap() {
                Value::Int(i) => *i,
                _ => panic!("n_iter is not Int"),
            };
            assert!(n_iter <= 300);

            // Inertia should be small given well-separated clusters
            let inertia = match map.get("inertia").unwrap() {
                Value::Float(f) => *f,
                _ => panic!("inertia is not Float"),
            };
            assert!(inertia < 1.0, "inertia={}", inertia);

            // Two centroids
            let centroids = match map.get("centroids").unwrap() {
                Value::Array(arr) => arr,
                _ => panic!("centroids is not Array"),
            };
            assert_eq!(centroids.len(), 2);

            // Six assignments
            let assignments = match map.get("assignments").unwrap() {
                Value::Array(arr) => arr,
                _ => panic!("assignments is not Array"),
            };
            assert_eq!(assignments.len(), 6);

            // First three points should share a cluster; last three should share a cluster
            let a0 = match assignments[0] {
                Value::Int(i) => i,
                _ => panic!(),
            };
            let a3 = match assignments[3] {
                Value::Int(i) => i,
                _ => panic!(),
            };
            assert_ne!(a0, a3);
            for i in 0..3 {
                assert_eq!(
                    match assignments[i] {
                        Value::Int(x) => x,
                        _ => panic!(),
                    },
                    a0
                );
            }
            for i in 3..6 {
                assert_eq!(
                    match assignments[i] {
                        Value::Int(x) => x,
                        _ => panic!(),
                    },
                    a3
                );
            }
        } else {
            panic!("expected Object");
        }
    }

    #[test]
    fn test_kmeans_k_equals_n() {
        let data = make_data(&[&[1.0], &[2.0], &[3.0]]);
        let result = builtin_kmeans(&[data, Value::Int(3)]).unwrap();
        if let Value::Object(map) = result {
            let inertia = match map.get("inertia").unwrap() {
                Value::Float(f) => *f,
                _ => panic!(),
            };
            assert_eq!(inertia, 0.0);
        }
    }

    #[test]
    fn test_kmeans_max_iter_respected() {
        let data = make_data(&[&[0.0, 0.0], &[1.0, 1.0], &[2.0, 2.0], &[10.0, 10.0]]);
        let result = builtin_kmeans(&[data, Value::Int(2), Value::Int(1)]).unwrap();
        if let Value::Object(map) = result {
            let n_iter = match map.get("n_iter").unwrap() {
                Value::Int(i) => *i,
                _ => panic!(),
            };
            assert!(n_iter <= 1);
        }
    }

    #[test]
    fn test_kmeans_object_points() {
        // Points as objects with named features
        let make_obj = |x: f64, y: f64| -> Value {
            let mut m = HashMap::new();
            m.insert("x".to_string(), Value::Float(x));
            m.insert("y".to_string(), Value::Float(y));
            Value::Object(m)
        };
        let data = Value::Array(vec![
            make_obj(0.0, 0.0),
            make_obj(0.0, 1.0),
            make_obj(10.0, 10.0),
            make_obj(10.0, 11.0),
        ]);
        let result = builtin_kmeans(&[data, Value::Int(2)]).unwrap();
        if let Value::Object(map) = result {
            let centroids = match map.get("centroids").unwrap() {
                Value::Array(arr) => arr,
                _ => panic!(),
            };
            assert_eq!(centroids.len(), 2);
        } else {
            panic!("expected Object");
        }
    }

    #[test]
    fn test_kmeans_error_k_greater_than_n() {
        let data = make_data(&[&[1.0], &[2.0]]);
        let err = builtin_kmeans(&[data, Value::Int(5)]).unwrap_err();
        assert!(err.to_string().contains("cannot be greater"));
    }

    #[test]
    fn test_kmeans_error_empty_data() {
        let data = Value::Array(vec![]);
        let err = builtin_kmeans(&[data, Value::Int(2)]).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_kmeans_error_non_numeric() {
        let data = Value::Array(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]);
        let err = builtin_kmeans(&[data, Value::Int(1)]).unwrap_err();
        assert!(err.to_string().contains("numeric"));
    }

    #[test]
    fn test_kmeans_registered() {
        let registry = crate::BuiltinRegistry::new();
        assert!(registry.functions.contains_key("kmeans"));
    }
}
