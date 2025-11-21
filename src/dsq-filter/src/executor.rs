//! Filter execution engine for dsq
//!
//! This module provides the execution engine that runs compiled filters against
//! data values, managing the execution lifecycle, error handling, and performance
//! monitoring.

use crate::compiler::{CompiledFilter, ErrorMode, FilterCompiler, FilterContext};
use dsq_shared::value::Value;
use dsq_shared::Result;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
#[cfg(target_arch = "wasm32")]
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{Duration, Instant};

/// Execution configuration options
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Maximum execution time in milliseconds
    pub timeout_ms: Option<u64>,
    /// Error handling mode
    pub error_mode: ErrorMode,
    /// Whether to collect execution statistics
    pub collect_stats: bool,
    /// Maximum recursion depth
    pub max_recursion_depth: usize,
    /// Whether to enable debug mode
    pub debug_mode: bool,
    /// Batch size for DataFrame operations
    pub batch_size: usize,
    /// Variables available during execution
    pub variables: std::collections::HashMap<String, Value>,
    /// Maximum number of compiled filters to cache
    pub filter_cache_size: usize,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            timeout_ms: None,
            error_mode: ErrorMode::Strict,
            collect_stats: false,
            max_recursion_depth: 1000,
            debug_mode: false,
            batch_size: 10000,
            variables: std::collections::HashMap::new(),
            filter_cache_size: 1000, // Cache up to 1000 compiled filters
        }
    }
}

/// Execution mode for different types of operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// Standard execution
    Standard,
    /// Lazy evaluation mode
    Lazy,
    /// Streaming mode for large datasets
    Streaming,
}

/// Result of filter execution
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    /// The output value
    pub value: Value,
    /// Execution statistics (if collected)
    pub stats: Option<ExecutionStats>,
    /// Any warnings generated during execution
    pub warnings: Vec<String>,
}

/// Execution statistics
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    /// Total execution time
    pub execution_time: Duration,
    /// Number of operations executed
    pub operations_executed: usize,
    /// Peak memory usage in bytes
    pub peak_memory_bytes: usize,
    /// Number of function calls
    pub function_calls: usize,
    /// Number of DataFrame operations
    pub dataframe_operations: usize,
    /// Cache hit rate (0.0 to 1.0)
    pub cache_hit_rate: f64,
}

/// Filter executor that runs compiled filters against data
pub struct FilterExecutor {
    /// Compiler for filters
    compiler: FilterCompiler,
    /// Execution configuration
    config: ExecutorConfig,
    /// LRU cache for compiled filters (provides O(1) get/put operations)
    /// Arc allows cheap cloning for concurrent access
    filter_cache: LruCache<String, Arc<CompiledFilter>>,
    /// Cache hit/miss counters
    cache_hits: usize,
    cache_misses: usize,
    /// Statistics accumulator
    stats_accumulator: Option<ExecutionStats>,
}

impl FilterExecutor {
    /// Create a new filter executor with default configuration
    pub fn new() -> Self {
        Self::with_config(ExecutorConfig::default())
    }

    /// Create a new filter executor with custom configuration
    pub fn with_config(config: ExecutorConfig) -> Self {
        let collect_stats = config.collect_stats;
        let cache_size = config.filter_cache_size;
        // Use LruCache for O(1) get/put operations instead of manual LRU tracking
        let cache_capacity =
            NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(1000).unwrap());
        Self {
            compiler: FilterCompiler::new(),
            config,
            filter_cache: LruCache::new(cache_capacity),
            cache_hits: 0,
            cache_misses: 0,
            stats_accumulator: if collect_stats {
                Some(ExecutionStats {
                    execution_time: Duration::ZERO,
                    operations_executed: 0,
                    peak_memory_bytes: 0,
                    function_calls: 0,
                    dataframe_operations: 0,
                    cache_hit_rate: 0.0,
                })
            } else {
                None
            },
        }
    }

    /// Execute a filter string against a value
    pub fn execute_str(&mut self, filter: &str, input: Value) -> Result<ExecutionResult> {
        #[cfg(not(target_arch = "wasm32"))]
        let start_time = Instant::now();
        let collect_stats = self.config.collect_stats;

        // Check cache and get Arc clone (cheap - just increments reference count)
        let compiled = if let Some(cached) = self.filter_cache.get(filter) {
            self.cache_hits += 1;
            Arc::clone(cached)
        } else {
            self.cache_misses += 1;

            #[cfg(feature = "profiling")]
            coz::progress!("filter_compilation");

            // Compile the filter
            let compiled = self.compiler.compile_str(filter)?;
            let arc_compiled = Arc::new(compiled);

            // Insert into cache - LRU eviction handled automatically by LruCache
            self.filter_cache
                .put(filter.to_string(), Arc::clone(&arc_compiled));

            arc_compiled
        };

        // Update cache hit rate in stats
        if collect_stats {
            if let Some(ref mut stats) = self.stats_accumulator {
                let total_requests = self.cache_hits + self.cache_misses;
                stats.cache_hit_rate = if total_requests > 0 {
                    self.cache_hits as f64 / total_requests as f64
                } else {
                    0.0
                };
            }
        }

        let operations_count = compiled.operations.len();

        #[cfg(feature = "profiling")]
        coz::progress!("filter_execute_start");

        let mut result = self.execute_compiled(&compiled, input)?;

        #[cfg(feature = "profiling")]
        coz::progress!("filter_execute_end");

        if collect_stats {
            if let Some(ref mut stats) = self.stats_accumulator {
                #[cfg(not(target_arch = "wasm32"))]
                {
                    stats.execution_time += start_time.elapsed();
                }
                stats.operations_executed += operations_count;
            }
            result.stats = self.stats_accumulator.clone();
        }

        Ok(result)
    }

    /// Execute a compiled filter against a value
    pub fn execute_compiled(
        &self,
        filter: &CompiledFilter,
        input: Value,
    ) -> Result<ExecutionResult> {
        #[cfg(not(target_arch = "wasm32"))]
        let start_time = Instant::now();

        // Create execution context
        let mut context = FilterContext::new();
        context.set_error_mode(self.config.error_mode);
        context.set_debug_mode(self.config.debug_mode);
        context.set_input(input);
        context.set_functions(filter.functions.clone());

        // Set variables from config
        for (name, value) in &self.config.variables {
            context.set_variable(name, value.clone());
        }

        // Execute operations
        let mut current_value = context.get_input().cloned().unwrap_or(Value::Null);
        let mut warnings = Vec::new();

        for operation in &filter.operations {
            #[cfg(feature = "profiling")]
            coz::progress!("operation_exec");

            let mut ctx = Some(&mut context as &mut dyn dsq_shared::ops::Context);
            match operation.apply_with_context(&current_value, &mut ctx) {
                Ok(new_value) => {
                    current_value = new_value;
                }
                Err(e) => {
                    match self.config.error_mode {
                        ErrorMode::Strict => return Err(e),
                        ErrorMode::Collect => {
                            warnings.push(format!("Operation failed: {}", e));
                            // Continue with null value
                            current_value = Value::Null;
                        }
                        ErrorMode::Ignore => {
                            // Continue with null value
                            current_value = Value::Null;
                        }
                    }
                }
            }

            // Check timeout
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(timeout_ms) = self.config.timeout_ms {
                if start_time.elapsed() > Duration::from_millis(timeout_ms) {
                    return Err(dsq_shared::error::operation_error("Execution timeout"));
                }
            }
        }

        let stats = if self.config.collect_stats {
            self.stats_accumulator.clone()
        } else {
            None
        };

        Ok(ExecutionResult {
            value: current_value,
            stats,
            warnings,
        })
    }

    /// Execute a filter in streaming mode for large datasets
    pub fn execute_streaming(
        &mut self,
        filter: &str,
        input_stream: impl Iterator<Item = Result<Value>>,
    ) -> Result<Vec<ExecutionResult>> {
        let compiled = self.compiler.compile_str(filter)?;
        let mut results = Vec::new();

        for item_result in input_stream {
            let input = item_result?;
            let result = self.execute_compiled(&compiled, input)?;
            results.push(result);

            // TODO: Check if we should yield control (for async/streaming).
            //       placeholder for future async implementation
        }

        Ok(results)
    }

    /// Validate a filter string without executing it
    pub fn validate_filter(&self, filter: &str) -> Result<()> {
        self.compiler.compile_str(filter)?;
        Ok(())
    }

    /// Get execution statistics
    pub fn get_stats(&self) -> Option<&ExecutionStats> {
        self.stats_accumulator.as_ref()
    }

    /// Clear the filter cache
    pub fn clear_cache(&mut self) {
        self.filter_cache.clear();
    }

    /// Get cache size
    pub fn cache_size(&self) -> usize {
        self.filter_cache.len()
    }

    /// Precompile and cache a filter
    pub fn precompile(&mut self, filter: &str) -> Result<()> {
        let compiled = self.compiler.compile_str(filter)?;
        self.filter_cache
            .put(filter.to_string(), Arc::new(compiled));
        Ok(())
    }

    /// Set execution configuration
    pub fn set_config(&mut self, config: ExecutorConfig) {
        let collect_stats = config.collect_stats;
        self.config = config;
        if collect_stats && self.stats_accumulator.is_none() {
            self.stats_accumulator = Some(ExecutionStats {
                execution_time: Duration::ZERO,
                operations_executed: 0,
                peak_memory_bytes: 0,
                function_calls: 0,
                dataframe_operations: 0,
                cache_hit_rate: 0.0,
            });
        } else if !collect_stats {
            self.stats_accumulator = None;
        }
    }

    /// Get current configuration
    pub fn get_config(&self) -> &ExecutorConfig {
        &self.config
    }
}

impl Default for FilterExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;

    #[test]
    fn test_execute_identity_filter() {
        let mut executor = FilterExecutor::new();
        let input = Value::int(42);
        let result = executor.execute_str(".", input.clone()).unwrap();

        assert_eq!(result.value, input);
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_execute_field_access() {
        let mut executor = FilterExecutor::new();
        let input = Value::object(std::collections::HashMap::from([
            ("name".to_string(), Value::string("Alice")),
            ("age".to_string(), Value::int(30)),
        ]));

        let result = executor.execute_str(".name", input).unwrap();
        assert_eq!(result.value, Value::string("Alice"));
    }

    #[test]
    fn test_filter_validation() {
        let executor = FilterExecutor::new();

        // Valid filter
        assert!(executor.validate_filter(".").is_ok());
        assert!(executor.validate_filter(".name").is_ok());

        // Invalid filter
        assert!(executor.validate_filter("invalid syntax +++").is_err());
    }

    #[test]
    fn test_cache_functionality() {
        let mut executor = FilterExecutor::new();

        // First execution should compile
        let input = Value::int(42);
        let result1 = executor.execute_str(". + 1", input.clone()).unwrap();
        assert_eq!(result1.value, Value::int(43));

        // Second execution should use cache
        let result2 = executor.execute_str(". + 1", input).unwrap();
        assert_eq!(result2.value, Value::int(43));

        assert_eq!(executor.cache_size(), 1);
    }

    #[test]
    fn test_error_handling() {
        let mut executor = FilterExecutor::new();

        // Test with strict error mode (default)
        let input = Value::int(42);
        let result = executor.execute_str(".invalid_field", input);
        assert!(result.is_err());

        // Test with ignore error mode
        let mut config = ExecutorConfig::default();
        config.error_mode = ErrorMode::Ignore;
        executor.set_config(config);

        let input = Value::int(42);
        let result = executor.execute_str(".invalid_field", input).unwrap();
        assert_eq!(result.value, Value::Null);
    }

    #[test]
    fn test_assignment_operation() {
        let mut executor = FilterExecutor::new();

        // Test field assignment on object
        let mut obj = std::collections::HashMap::new();
        obj.insert("salary".to_string(), Value::int(75000));
        obj.insert("name".to_string(), Value::string("Alice"));
        let input = Value::object(obj);

        let result = executor.execute_str(".salary += 5000", input).unwrap();

        if let Value::Object(result_obj) = result.value {
            assert_eq!(result_obj.get("salary"), Some(&Value::int(80000)));
            assert_eq!(result_obj.get("name"), Some(&Value::string("Alice")));
        } else {
            panic!("Expected object result");
        }
    }

    #[test]
    fn test_assignment_in_map_pipeline() {
        let mut executor = FilterExecutor::new();

        // Test the query from example_095: map(.salary += 5000) | map({name, new_salary: .salary, department})
        let mut obj = std::collections::HashMap::new();
        obj.insert("id".to_string(), Value::int(1));
        obj.insert("name".to_string(), Value::string("Alice Johnson"));
        obj.insert("age".to_string(), Value::int(28));
        obj.insert("city".to_string(), Value::string("New York"));
        obj.insert("salary".to_string(), Value::int(75000));
        obj.insert("department".to_string(), Value::string("Engineering"));
        let input = Value::Array(vec![Value::Object(obj)]);

        let result = executor
            .execute_str(
                r#"map(.salary += 5000) | map({name, new_salary: .salary, department})"#,
                input,
            )
            .unwrap();

        if let Value::Array(arr) = result.value {
            assert_eq!(arr.len(), 1);
            if let Value::Object(obj) = &arr[0] {
                assert_eq!(obj.get("name"), Some(&Value::string("Alice Johnson")));
                assert_eq!(obj.get("new_salary"), Some(&Value::int(80000)));
                assert_eq!(obj.get("department"), Some(&Value::string("Engineering")));
            } else {
                panic!("Expected object in array");
            }
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_stats_collection() {
        let mut config = ExecutorConfig::default();
        config.collect_stats = true;
        let mut executor = FilterExecutor::with_config(config);

        let input = Value::int(42);
        let result = executor.execute_str(".", input).unwrap();

        let stats = result.stats.unwrap();
        assert!(stats.execution_time > Duration::ZERO);
        assert!(stats.operations_executed > 0);
        // Other stats are initialized to 0 and not updated yet
        assert_eq!(stats.peak_memory_bytes, 0);
        assert_eq!(stats.function_calls, 0);
        assert_eq!(stats.dataframe_operations, 0);
        assert_eq!(stats.cache_hit_rate, 0.0);
    }

    #[test]
    fn test_streaming_execution() {
        let mut executor = FilterExecutor::new();
        let inputs = vec![Ok(Value::int(1)), Ok(Value::int(2)), Ok(Value::int(3))];

        let results = executor.execute_streaming(".", inputs.into_iter()).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].value, Value::int(1));
        assert_eq!(results[1].value, Value::int(2));
        assert_eq!(results[2].value, Value::int(3));
    }

    #[test]
    fn test_precompile() {
        let mut executor = FilterExecutor::new();
        executor.precompile(". + 1").unwrap();
        assert_eq!(executor.cache_size(), 1);

        let input = Value::int(42);
        let result = executor.execute_str(". + 1", input).unwrap();
        assert_eq!(result.value, Value::int(43));
    }

    #[test]
    fn test_clear_cache() {
        let mut executor = FilterExecutor::new();
        executor.execute_str(".", Value::int(1)).unwrap();
        assert_eq!(executor.cache_size(), 1);

        executor.clear_cache();
        assert_eq!(executor.cache_size(), 0);
    }

    #[test]
    fn test_config_management() {
        let mut executor = FilterExecutor::new();
        let config = executor.get_config();
        assert_eq!(config.timeout_ms, None);

        let mut new_config = ExecutorConfig::default();
        new_config.timeout_ms = Some(1000);
        executor.set_config(new_config);

        let config = executor.get_config();
        assert_eq!(config.timeout_ms, Some(1000));
    }

    #[test]
    fn test_error_collect_mode() {
        let mut executor = FilterExecutor::new();
        let mut config = ExecutorConfig::default();
        config.error_mode = ErrorMode::Collect;
        executor.set_config(config);

        let input = Value::int(42);
        let result = executor.execute_str(".invalid_field", input).unwrap();
        assert_eq!(result.value, Value::Null);
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].contains("Operation failed"));
    }

    #[test]
    fn test_timeout_configuration() {
        let mut executor = FilterExecutor::new();
        let mut config = ExecutorConfig::default();
        config.timeout_ms = Some(1000); // 1 second timeout
        executor.set_config(config);

        let input = Value::int(42);
        let result = executor.execute_str(".", input).unwrap();
        // Should succeed since operation is fast
        assert_eq!(result.value, Value::int(42));
    }
}
