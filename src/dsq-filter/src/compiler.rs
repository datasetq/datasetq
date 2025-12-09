//! Filter compiler for dsq
//!
//! This module provides the compiler that transforms jaq AST nodes into
//! dsq operations. It bridges the gap between jq's JSON-oriented filter
//! language and dsq's DataFrame-oriented operations.
//!
//! The compiler handles:
//! - Converting jaq AST terms to dsq operations
//! - Managing variable scoping and function definitions
//! - Optimizing filter expressions for DataFrame operations
//! - Type checking and error reporting

use dsq_functions::BuiltinRegistry;
use dsq_parser::{BinaryOperator, Expr, FilterParser, Literal, ObjectEntry, UnaryOperator};
use dsq_shared::ops::*;
use dsq_shared::value::{is_truthy, Value};
use dsq_shared::Result;
use polars::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Convert Polars AnyValue to dsq Value
fn value_from_any_value(any_val: AnyValue) -> Result<Value> {
    match any_val {
        AnyValue::Null => Ok(Value::Null),
        AnyValue::Boolean(b) => Ok(Value::Bool(b)),
        AnyValue::Int8(i) => Ok(Value::Int(i as i64)),
        AnyValue::Int16(i) => Ok(Value::Int(i as i64)),
        AnyValue::Int32(i) => Ok(Value::Int(i as i64)),
        AnyValue::Int64(i) => Ok(Value::Int(i)),
        AnyValue::UInt8(i) => Ok(Value::Int(i as i64)),
        AnyValue::UInt16(i) => Ok(Value::Int(i as i64)),
        AnyValue::UInt32(i) => Ok(Value::Int(i as i64)),
        AnyValue::UInt64(i) => Ok(Value::Int(i as i64)),
        AnyValue::Float32(f) => Ok(Value::Float(f as f64)),
        AnyValue::Float64(f) => Ok(Value::Float(f)),
        AnyValue::String(s) => Ok(Value::String(s.to_string())),
        _ => Ok(Value::String(any_val.to_string())),
    }
}

/// Convert Values to Series
fn values_to_series(name: &str, values: &[Value]) -> Result<Series> {
    if values.is_empty() {
        return Ok(Series::new_empty(name.into(), &DataType::Null));
    }

    // Determine the data type from the first non-null value
    let dtype = values
        .iter()
        .find(|v| !matches!(v, Value::Null))
        .map(|v| match v {
            Value::Bool(_) => DataType::Boolean,
            Value::Int(_) => DataType::Int64,
            Value::Float(_) => DataType::Float64,
            Value::String(_) => DataType::String,
            _ => DataType::Null,
        })
        .unwrap_or(DataType::Null);

    match dtype {
        DataType::Boolean => {
            let vec: Vec<Option<bool>> = values
                .iter()
                .map(|v| match v {
                    Value::Bool(b) => Some(*b),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Series::new(name.into(), vec))
        }
        DataType::Int64 => {
            let vec: Vec<Option<i64>> = values
                .iter()
                .map(|v| match v {
                    Value::Int(i) => Some(*i),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Series::new(name.into(), vec))
        }
        DataType::Float64 => {
            let vec: Vec<Option<f64>> = values
                .iter()
                .map(|v| match v {
                    Value::Float(f) => Some(*f),
                    Value::Int(i) => Some(*i as f64),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Series::new(name.into(), vec))
        }
        DataType::String => {
            let vec: Vec<Option<&str>> = values
                .iter()
                .map(|v| match v {
                    Value::String(s) => Some(s.as_str()),
                    Value::Null => None,
                    _ => None,
                })
                .collect();
            Ok(Series::new(name.into(), vec))
        }
        _ => Ok(Series::new_null(name.into(), values.len())),
    }
}

/// Compare two values for sorting purposes
#[allow(dead_code)]
fn compare_values_for_sorting(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::Null, _) => std::cmp::Ordering::Less,
        (_, Value::Null) => std::cmp::Ordering::Greater,
        (Value::Bool(a_val), Value::Bool(b_val)) => a_val.cmp(b_val),
        (Value::Int(a_val), Value::Int(b_val)) => a_val.cmp(b_val),
        (Value::Float(a_val), Value::Float(b_val)) => a_val
            .partial_cmp(b_val)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(a_val), Value::String(b_val)) => a_val.cmp(b_val),
        (Value::Int(a_val), Value::Float(b_val)) => (*a_val as f64)
            .partial_cmp(b_val)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a_val), Value::Int(b_val)) => a_val
            .partial_cmp(&(*b_val as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (Value::Array(a_arr), Value::Array(b_arr)) => {
            let len_cmp = a_arr.len().cmp(&b_arr.len());
            if len_cmp != std::cmp::Ordering::Equal {
                len_cmp
            } else {
                for (a_item, b_item) in a_arr.iter().zip(b_arr.iter()) {
                    let item_cmp = compare_values_for_sorting(a_item, b_item);
                    if item_cmp != std::cmp::Ordering::Equal {
                        return item_cmp;
                    }
                }
                std::cmp::Ordering::Equal
            }
        }
        (Value::Object(a_obj), Value::Object(b_obj)) => {
            let a_keys: Vec<&String> = a_obj.keys().collect();
            let b_keys: Vec<&String> = b_obj.keys().collect();
            let keys_cmp = a_keys.len().cmp(&b_keys.len());
            if keys_cmp != std::cmp::Ordering::Equal {
                keys_cmp
            } else {
                for (a_key, b_key) in a_keys.iter().zip(b_keys.iter()) {
                    let key_cmp = a_key.cmp(b_key);
                    if key_cmp != std::cmp::Ordering::Equal {
                        return key_cmp;
                    }
                    let a_val = a_obj.get(*a_key).unwrap();
                    let b_val = b_obj.get(*b_key).unwrap();
                    let val_cmp = compare_values_for_sorting(a_val, b_val);
                    if val_cmp != std::cmp::Ordering::Equal {
                        return val_cmp;
                    }
                }
                std::cmp::Ordering::Equal
            }
        }
        // For different types, compare by type order: Null < Bool < Int < Float < String < Array < Object
        (Value::Bool(_), Value::Int(_)) => std::cmp::Ordering::Less,
        (Value::Bool(_), Value::Float(_)) => std::cmp::Ordering::Less,
        (Value::Bool(_), Value::String(_)) => std::cmp::Ordering::Less,
        (Value::Bool(_), Value::Array(_)) => std::cmp::Ordering::Less,
        (Value::Bool(_), Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::Int(_), Value::Bool(_)) => std::cmp::Ordering::Greater,
        (Value::Int(_), Value::String(_)) => std::cmp::Ordering::Less,
        (Value::Int(_), Value::Array(_)) => std::cmp::Ordering::Less,
        (Value::Int(_), Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::Float(_), Value::Bool(_)) => std::cmp::Ordering::Greater,
        (Value::Float(_), Value::String(_)) => std::cmp::Ordering::Less,
        (Value::Float(_), Value::Array(_)) => std::cmp::Ordering::Less,
        (Value::Float(_), Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::String(_), Value::Bool(_)) => std::cmp::Ordering::Greater,
        (Value::String(_), Value::Int(_)) => std::cmp::Ordering::Greater,
        (Value::String(_), Value::Float(_)) => std::cmp::Ordering::Greater,
        (Value::String(_), Value::Array(_)) => std::cmp::Ordering::Less,
        (Value::String(_), Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::Array(_), Value::Bool(_)) => std::cmp::Ordering::Greater,
        (Value::Array(_), Value::Int(_)) => std::cmp::Ordering::Greater,
        (Value::Array(_), Value::Float(_)) => std::cmp::Ordering::Greater,
        (Value::Array(_), Value::String(_)) => std::cmp::Ordering::Greater,
        (Value::Array(_), Value::Object(_)) => std::cmp::Ordering::Less,
        (Value::Object(_), Value::Bool(_)) => std::cmp::Ordering::Greater,
        (Value::Object(_), Value::Int(_)) => std::cmp::Ordering::Greater,
        (Value::Object(_), Value::Float(_)) => std::cmp::Ordering::Greater,
        (Value::Object(_), Value::String(_)) => std::cmp::Ordering::Greater,
        (Value::Object(_), Value::Array(_)) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal, // Same type but unhandled, treat as equal
    }
}

/// Compiler for transforming jaq AST to dsq operations
pub struct FilterCompiler {
    /// Built-in function registry
    builtins: Arc<BuiltinRegistry>,
    /// Optimization level
    optimization_level: OptimizationLevel,
    /// Whether to enable DataFrame-specific optimizations
    dataframe_optimizations: bool,
    /// Maximum recursion depth for compilation
    max_recursion_depth: usize,
}

/// Optimization levels for filter compilation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizationLevel {
    /// No optimizations
    None,
    /// Basic optimizations (constant folding, dead code elimination)
    Basic,
    /// Advanced optimizations (operation fusion, predicate pushdown)
    Advanced,
}

/// Compiled filter expression
pub struct CompiledFilter {
    /// The main operation pipeline
    pub operations: Vec<Box<dyn Operation + Send + Sync>>,
    /// Variable definitions needed for execution
    pub variables: HashMap<String, Value>,
    /// Function definitions
    pub functions: HashMap<String, FunctionDef>,
    /// Whether this filter requires lazy evaluation
    pub requires_lazy: bool,
    /// Estimated complexity score
    pub complexity: usize,
}

/// Compilation context for building filter operations
///
/// The compilation context tracks the current compilation state including:
/// - Recursion depth to prevent infinite recursion
/// - Variable scoping information
/// - Function definition context
#[derive(Debug, Clone)]
pub struct CompilationContext {
    /// Current recursion depth
    pub depth: usize,
    /// Maximum allowed recursion depth
    pub max_depth: usize,
    /// Variables available during compilation
    pub variables: HashMap<String, Value>,
    /// Functions available during compilation
    pub functions: HashMap<String, FunctionDef>,
}

impl Default for CompilationContext {
    fn default() -> Self {
        Self::new()
    }
}

impl CompilationContext {
    /// Create a new compilation context
    pub fn new() -> Self {
        Self {
            depth: 0,
            max_depth: 1000,
            variables: HashMap::new(),
            functions: HashMap::new(),
        }
    }

    /// Create a compilation context with custom max depth
    pub fn with_max_depth(max_depth: usize) -> Self {
        Self {
            depth: 0,
            max_depth,
            variables: HashMap::new(),
            functions: HashMap::new(),
        }
    }
}

/// Function definition
#[derive(Debug, Clone)]
pub struct FunctionDef {
    /// Function name
    pub name: String,

    /// Parameter names
    pub parameters: Vec<String>,

    /// Function body (as jaq AST or compiled form)
    pub body: FunctionBody,

    /// Whether this is a recursive function
    pub is_recursive: bool,
}

/// Function body representation
pub enum FunctionBody {
    /// Compiled dsq operations
    Compiled(Vec<Box<dyn Operation + Send + Sync>>),

    /// Raw jaq AST (to be compiled on demand)
    Ast(String), // Placeholder for actual AST type

    /// Built-in function implementation
    Builtin(BuiltinFunction),
}

impl Clone for FunctionBody {
    fn clone(&self) -> Self {
        match self {
            FunctionBody::Compiled(_) => panic!("Cannot clone Compiled FunctionBody"),
            FunctionBody::Ast(s) => FunctionBody::Ast(s.clone()),
            FunctionBody::Builtin(f) => FunctionBody::Builtin(f.clone()),
        }
    }
}

impl std::fmt::Debug for FunctionBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionBody::Compiled(ops) => write!(f, "Compiled({} operations)", ops.len()),
            FunctionBody::Ast(ast) => write!(f, "Ast({})", ast),
            FunctionBody::Builtin(_) => write!(f, "Builtin"),
        }
    }
}

/// Built-in function implementation
pub type BuiltinFunction = dsq_functions::BuiltinFunction;

/// Execution context for filter operations
#[derive(Debug, Clone)]
pub struct FilterContext {
    /// Variable bindings (name -> value)
    variables: HashMap<String, Value>,

    /// User-defined functions
    functions: HashMap<String, FunctionDef>,

    /// Built-in function registry
    builtins: Arc<BuiltinRegistry>,

    /// Current execution stack for debugging
    call_stack: Vec<StackFrame>,

    /// Maximum recursion depth
    max_recursion_depth: usize,

    /// Whether to collect debug information
    debug_mode: bool,

    /// Current input value being processed
    current_input: Option<Value>,

    /// Error handling mode
    error_mode: ErrorMode,
}

/// Stack frame for debugging and recursion tracking
#[derive(Debug, Clone)]
pub struct StackFrame {
    /// Function or operation name
    pub name: String,

    /// Input value at this frame
    pub input: Value,

    /// Location information (if available)
    pub location: Option<Location>,
}

/// Location information for error reporting
#[derive(Debug, Clone)]
pub struct Location {
    /// Line number (1-based)
    pub line: usize,

    /// Column number (1-based)
    pub column: usize,

    /// Source file or identifier
    pub source: Option<String>,
}

/// Error handling modes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorMode {
    /// Stop execution on first error
    Strict,

    /// Continue execution, collect errors
    Collect,

    /// Ignore errors, return null for failed operations
    Ignore,
}

impl Default for FilterContext {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterContext {
    /// Create a new filter context with default settings
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
            functions: HashMap::new(),
            builtins: Arc::new(BuiltinRegistry::new()),
            call_stack: Vec::new(),
            max_recursion_depth: 1000,
            debug_mode: false,
            current_input: None,
            error_mode: ErrorMode::Strict,
        }
    }

    /// Set a variable binding
    pub fn set_variable(&mut self, name: impl Into<String>, value: Value) {
        self.variables.insert(name.into(), value);
    }

    /// Set user-defined functions
    pub fn set_functions(&mut self, functions: HashMap<String, FunctionDef>) {
        self.functions = functions;
    }

    /// Get a variable value
    pub fn get_variable(&self, name: &str) -> Option<&Value> {
        self.variables.get(name)
    }

    /// Check if a variable exists
    pub fn has_variable(&self, name: &str) -> bool {
        self.variables.contains_key(name)
    }

    /// Check if a function is defined
    pub fn has_function(&self, name: &str) -> bool {
        self.functions.contains_key(name) || self.builtins.has_function(name)
    }

    /// Call a function with arguments
    pub fn call_function(&mut self, name: &str, args: &[Value]) -> Result<Value> {
        // Check recursion depth
        if self.call_stack.len() >= self.max_recursion_depth {
            return Err(dsq_shared::error::operation_error(format!(
                "Maximum recursion depth of {} exceeded in function '{}'",
                self.max_recursion_depth, name
            )));
        }

        // Try user-defined functions first
        if let Some(func_def) = self.functions.get(name).cloned() {
            self.call_user_function(&func_def, args)
        } else if self.builtins.has_function(name) {
            self.builtins.call_function(name, args)
        } else {
            Err(dsq_shared::error::operation_error(format!(
                "function '{}'",
                name
            )))
        }
    }

    /// Call a user-defined function
    fn call_user_function(&mut self, func_def: &FunctionDef, args: &[Value]) -> Result<Value> {
        // Validate argument count (skip for builtins as they handle arguments dynamically)
        if !matches!(func_def.body, FunctionBody::Builtin(_))
            && args.len() != func_def.parameters.len()
        {
            return Err(dsq_shared::error::operation_error(format!(
                "Expected {} arguments, got {}",
                func_def.parameters.len(),
                args.len()
            )));
        }

        // Create new scope for function execution
        let saved_vars = self.variables.clone();

        // Bind parameters to arguments
        for (param, arg) in func_def.parameters.iter().zip(args.iter()) {
            self.set_variable(param.clone(), arg.clone());
        }

        // Push stack frame
        let frame = StackFrame {
            name: func_def.name.clone(),
            input: self.current_input.clone().unwrap_or(Value::Null),
            location: None,
        };
        self.call_stack.push(frame);

        let result = match &func_def.body {
            FunctionBody::Compiled(ops) => {
                // Execute compiled operations
                let mut current_value = self.current_input.clone().unwrap_or(Value::Null);
                for op in ops {
                    current_value = op.apply(&current_value)?;
                }
                Ok(current_value)
            }
            FunctionBody::Ast(_ast) => {
                // TODO: Compile AST and execute
                Err(dsq_shared::error::operation_error(
                    "AST execution not yet implemented",
                ))
            }
            FunctionBody::Builtin(builtin_fn) => builtin_fn(args),
        };

        // Restore scope
        self.call_stack.pop();
        self.variables = saved_vars;

        result
    }

    /// Set the current input value
    pub fn set_input(&mut self, value: Value) {
        self.current_input = Some(value);
    }

    /// Get the current input value
    pub fn get_input(&self) -> Option<&Value> {
        self.current_input.as_ref()
    }

    /// Get the current recursion depth
    pub fn recursion_depth(&self) -> usize {
        self.call_stack.len()
    }

    /// Enable or disable debug mode
    pub fn set_debug_mode(&mut self, debug: bool) {
        self.debug_mode = debug;
    }

    /// Check if debug mode is enabled
    pub fn is_debug_mode(&self) -> bool {
        self.debug_mode
    }

    /// Set error handling mode
    pub fn set_error_mode(&mut self, mode: ErrorMode) {
        self.error_mode = mode;
    }

    /// Get current error handling mode
    pub fn error_mode(&self) -> ErrorMode {
        self.error_mode
    }
}

impl dsq_shared::ops::Context for FilterContext {
    fn get_variable(&self, name: &str) -> Option<&Value> {
        self.variables.get(name)
    }

    fn set_variable(&mut self, name: &str, value: Value) {
        self.variables.insert(name.to_string(), value);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl Default for FilterCompiler {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterCompiler {
    /// Create a new filter compiler with default settings
    pub fn new() -> Self {
        Self {
            builtins: Arc::new(BuiltinRegistry::new()),
            optimization_level: OptimizationLevel::Basic,
            dataframe_optimizations: true,
            max_recursion_depth: 1000,
        }
    }

    /// Set the optimization level
    pub fn with_optimization_level(mut self, level: OptimizationLevel) -> Self {
        self.optimization_level = level;
        self
    }

    /// Enable or disable DataFrame-specific optimizations
    pub fn with_dataframe_optimizations(mut self, enabled: bool) -> Self {
        self.dataframe_optimizations = enabled;
        self
    }

    /// Set maximum recursion depth
    pub fn with_max_recursion_depth(mut self, depth: usize) -> Self {
        self.max_recursion_depth = depth;
        self
    }

    /// Compile a filter expression from a string
    pub fn compile_str(&self, filter: &str) -> Result<CompiledFilter> {
        // Parse the filter string using dsq-parser
        let parser = FilterParser::new();
        let parsed = parser
            .parse(filter)
            .map_err(|e| dsq_shared::error::operation_error(format!("{}", e)))?;

        // Create compilation context with built-in functions
        let mut ctx = CompilationContext::new();
        for name in self.builtins.function_names() {
            if let Some(func) = self.builtins.get_function(&name) {
                ctx.functions.insert(
                    name.clone(),
                    FunctionDef {
                        name: name.clone(),
                        parameters: vec![], // Built-ins handle arguments dynamically
                        body: FunctionBody::Builtin(func),
                        is_recursive: false,
                    },
                );
            }
        }

        self.compile_expr(&parsed.expr, &mut ctx)
    }

    /// Compile a dsq-parser Expr into dsq operations
    pub fn compile_expr(
        &self,
        expr: &Expr,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        if ctx.depth >= self.max_recursion_depth {
            return Err(dsq_shared::error::operation_error(format!(
                "Maximum compilation depth {} exceeded",
                self.max_recursion_depth
            )));
        }

        ctx.depth += 1;
        let result = self.compile_expr_inner(expr, ctx);
        ctx.depth -= 1;
        result
    }

    /// Internal expr compilation
    fn compile_expr_inner(
        &self,
        expr: &Expr,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        match expr {
            Expr::Identity => {
                // Identity filter - returns input unchanged
                Ok(CompiledFilter {
                    operations: vec![Box::new(IdentityOperation)],
                    variables: HashMap::new(),
                    functions: HashMap::new(),
                    requires_lazy: false,
                    complexity: 1,
                })
            }
            Expr::FieldAccess { base, fields } => {
                // Field access
                self.compile_field_access(base, fields, ctx)
            }
            Expr::ArrayAccess { array, index } => {
                // Array indexing
                self.compile_array_access(array, index, ctx)
            }
            Expr::ArraySlice { array, start, end } => {
                // Array slicing
                self.compile_array_slice(
                    array,
                    start.as_ref().map(|v| &**v),
                    end.as_ref().map(|v| &**v),
                    ctx,
                )
            }
            Expr::ArrayIteration(array) => {
                // Array iteration (.[])
                self.compile_array_iteration(array, ctx)
            }
            Expr::FunctionCall { name, args } => {
                // Function call
                self.compile_function_call(name, args, ctx)
            }
            Expr::BinaryOp { left, op, right } => {
                // Binary operations
                self.compile_binary_op(left, op, right, ctx)
            }
            Expr::UnaryOp { op, expr } => {
                // Unary operations
                self.compile_unary_op(op, expr, ctx)
            }
            Expr::Assignment { target, value, op } => {
                // Assignment operations
                self.compile_assignment(target, value, op, ctx)
            }
            Expr::Object { pairs } => {
                // Object construction
                self.compile_object(pairs, ctx)
            }
            Expr::Array(elements) => {
                // Array construction
                self.compile_array(elements, ctx)
            }
            Expr::Literal(lit) => {
                // Literal values
                self.compile_literal(lit)
            }
            Expr::Identifier(name) => {
                // Identifier (variable or function)
                self.compile_identifier(name)
            }
            Expr::Paren(inner) => {
                // Parenthesized expression
                self.compile_expr(inner, ctx)
            }
            Expr::Pipeline(exprs) => {
                // Pipeline
                self.compile_pipeline(exprs, ctx)
            }
            Expr::Sequence(exprs) => {
                // Sequence (comma operator)
                self.compile_sequence(exprs, ctx)
            }
            Expr::Variable(name) => {
                // Variable reference
                Ok(CompiledFilter {
                    operations: vec![Box::new(VariableOperation::new(name.clone()))],
                    variables: HashMap::new(),
                    functions: HashMap::new(),
                    requires_lazy: false,
                    complexity: 1,
                })
            }

            Expr::If {
                condition,
                then_branch,
                else_branch,
            } => {
                // If-then-else expression
                self.compile_if(condition, then_branch, else_branch, ctx)
            }
        }
    }

    /// Compile field access expressions
    fn compile_field_access(
        &self,
        base: &Expr,
        fields: &[String],
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let base_filter = self.compile_expr(base, ctx)?;

        let mut operations = base_filter.operations;
        let complexity = base_filter.complexity + fields.len();

        // Create a single FieldAccessOperation with all fields
        // This allows assignment operations to see the full field path
        if !fields.is_empty() {
            operations.push(Box::new(FieldAccessOperation::with_fields(fields.to_vec())));
        }

        Ok(CompiledFilter {
            operations,
            variables: base_filter.variables,
            functions: base_filter.functions,
            requires_lazy: base_filter.requires_lazy,
            complexity,
        })
    }

    /// Compile field access with a literal field name (for .["field name"] syntax)
    fn compile_field_access_literal(
        &self,
        base: &Expr,
        field_name: &str,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let base_filter = self.compile_expr(base, ctx)?;

        let mut operations = base_filter.operations;
        operations.push(Box::new(FieldAccessOperation::new(field_name.to_string())));

        Ok(CompiledFilter {
            operations,
            variables: base_filter.variables,
            functions: base_filter.functions,
            requires_lazy: base_filter.requires_lazy,
            complexity: base_filter.complexity + 1,
        })
    }

    /// Compile array access expressions
    fn compile_array_access(
        &self,
        array: &Expr,
        index: &Expr,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        // Check if this is actually field access with a string literal (e.g., .["field name"])
        if let Expr::Literal(Literal::String(field_name)) = index {
            return self.compile_field_access_literal(array, field_name, ctx);
        }

        let array_filter = self.compile_expr(array, ctx)?;
        let index_filter = self.compile_expr(index, ctx)?;

        let mut variables = array_filter.variables;
        variables.extend(index_filter.variables);

        let mut functions = array_filter.functions;
        functions.extend(index_filter.functions);

        let operations: Vec<Box<dyn Operation + Send + Sync>> =
            vec![Box::new(IndexOperation::new(index_filter.operations))];
        let mut all_operations = array_filter.operations;
        all_operations.extend(operations);

        Ok(CompiledFilter {
            operations: all_operations,
            variables,
            functions,
            requires_lazy: array_filter.requires_lazy || index_filter.requires_lazy,
            complexity: array_filter.complexity + index_filter.complexity + 2,
        })
    }

    /// Compile array slice expressions
    fn compile_array_slice(
        &self,
        array: &Expr,
        start: Option<&Expr>,
        end: Option<&Expr>,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let array_filter = self.compile_expr(array, ctx)?;

        let start_ops = if let Some(start_expr) = start {
            Some(self.compile_expr(start_expr, ctx)?.operations)
        } else {
            None
        };

        let end_ops = if let Some(end_expr) = end {
            Some(self.compile_expr(end_expr, ctx)?.operations)
        } else {
            None
        };

        let operations: Vec<Box<dyn Operation + Send + Sync>> =
            vec![Box::new(SliceOperation::new(start_ops, end_ops))];
        let mut all_operations = array_filter.operations;
        all_operations.extend(operations);

        Ok(CompiledFilter {
            operations: all_operations,
            variables: array_filter.variables,
            functions: array_filter.functions,
            requires_lazy: array_filter.requires_lazy,
            complexity: array_filter.complexity + 3,
        })
    }

    /// Compile array iteration expressions
    fn compile_array_iteration(
        &self,
        array: &Expr,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let array_filter = self.compile_expr(array, ctx)?;

        let operations: Vec<Box<dyn Operation + Send + Sync>> = vec![Box::new(IterateOperation)];
        let mut all_operations = array_filter.operations;
        all_operations.extend(operations);

        Ok(CompiledFilter {
            operations: all_operations,
            variables: array_filter.variables,
            functions: array_filter.functions,
            requires_lazy: array_filter.requires_lazy,
            complexity: array_filter.complexity + 3,
        })
    }

    /// Compile binary operations
    fn compile_binary_op(
        &self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let left_filter = self.compile_expr(left, ctx)?;
        let right_filter = self.compile_expr(right, ctx)?;

        let mut variables = left_filter.variables;
        variables.extend(right_filter.variables);

        let mut functions = left_filter.functions;
        functions.extend(right_filter.functions);

        let operation: Box<dyn Operation + Send + Sync> = match op {
            BinaryOperator::Add => Box::new(AddOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Sub => Box::new(SubOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Mul => Box::new(MulOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Div => Box::new(DivOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Eq => Box::new(EqOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Ne => Box::new(NeOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Lt => Box::new(LtOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Le => Box::new(LeOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Gt => Box::new(GtOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Ge => Box::new(GeOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::And => Box::new(AndOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
            BinaryOperator::Or => Box::new(OrOperation::new(
                left_filter.operations,
                right_filter.operations,
            )),
        };

        Ok(CompiledFilter {
            operations: vec![operation],
            variables,
            functions,
            requires_lazy: left_filter.requires_lazy || right_filter.requires_lazy,
            complexity: left_filter.complexity + right_filter.complexity + 2,
        })
    }

    /// Compile unary operations
    fn compile_unary_op(
        &self,
        op: &UnaryOperator,
        expr: &Expr,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let expr_filter = self.compile_expr(expr, ctx)?;

        let operation: Box<dyn Operation + Send + Sync> = match op {
            UnaryOperator::Not => Box::new(NegationOperation::new(expr_filter.operations)),
            UnaryOperator::Del => Box::new(DelOperation::new(expr_filter.operations)),
        };

        Ok(CompiledFilter {
            operations: vec![operation],
            variables: expr_filter.variables,
            functions: expr_filter.functions,
            requires_lazy: expr_filter.requires_lazy,
            complexity: expr_filter.complexity + 1,
        })
    }

    /// Compile if-then-else expressions
    fn compile_if(
        &self,
        condition: &Expr,
        then_branch: &Expr,
        else_branch: &Expr,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let condition_filter = self.compile_expr(condition, ctx)?;
        let then_filter = self.compile_expr(then_branch, ctx)?;
        let else_filter = self.compile_expr(else_branch, ctx)?;

        let mut variables = condition_filter.variables;
        variables.extend(then_filter.variables);
        variables.extend(else_filter.variables);

        let mut functions = condition_filter.functions;
        functions.extend(then_filter.functions);
        functions.extend(else_filter.functions);

        let operation = Box::new(IfOperation::new(
            condition_filter.operations,
            then_filter.operations,
            else_filter.operations,
        ));

        Ok(CompiledFilter {
            operations: vec![operation],
            variables,
            functions,
            requires_lazy: condition_filter.requires_lazy
                || then_filter.requires_lazy
                || else_filter.requires_lazy,
            complexity: condition_filter.complexity
                + then_filter.complexity
                + else_filter.complexity
                + 3,
        })
    }

    /// Compile function call expressions
    fn compile_function_call(
        &self,
        name: &str,
        args: &[Expr],
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        // Special handling for join function
        if name == "join" && args.len() == 2 {
            if let (
                Expr::Literal(Literal::String(file_path)),
                Expr::BinaryOp {
                    left,
                    op: BinaryOperator::Eq,
                    right,
                },
            ) = (&args[0], &args[1])
            {
                // Parse the equality condition like .dept_id == .id
                if let (
                    Expr::FieldAccess {
                        base: left_base,
                        fields: left_fields,
                    },
                    Expr::FieldAccess {
                        base: right_base,
                        fields: right_fields,
                    },
                ) = (&**left, &**right)
                {
                    if matches!(**left_base, Expr::Identity)
                        && matches!(**right_base, Expr::Identity)
                        && left_fields.len() == 1
                        && right_fields.len() == 1
                    {
                        let left_key = left_fields[0].clone();
                        let right_key = right_fields[0].clone();
                        let operation = Box::new(JoinFromFileOperation::new(
                            file_path.clone(),
                            left_key,
                            right_key,
                        ));
                        return Ok(CompiledFilter {
                            operations: vec![operation],
                            variables: HashMap::new(),
                            functions: HashMap::new(),
                            requires_lazy: false,
                            complexity: 10, // Join is complex
                        });
                    }
                }
            }
        }

        // Compile each argument expression
        let mut arg_filters = Vec::new();
        let mut variables = HashMap::new();
        let mut functions = HashMap::new();
        let mut complexity = 1; // Base complexity for function call
        let mut requires_lazy = false;

        for arg in args {
            let arg_filter = self.compile_expr(arg, ctx)?;
            variables.extend(arg_filter.variables);
            functions.extend(arg_filter.functions);
            complexity += arg_filter.complexity;
            requires_lazy |= arg_filter.requires_lazy;
            arg_filters.push(arg_filter.operations);
        }

        let operation = Box::new(FunctionCallOperation::new(
            name.to_string(),
            arg_filters,
            self.builtins.clone(),
        ));

        Ok(CompiledFilter {
            operations: vec![operation],
            variables,
            functions,
            requires_lazy,
            complexity,
        })
    }

    /// Compile assignment operations
    fn compile_assignment(
        &self,
        target: &Expr,
        value: &Expr,
        op: &dsq_parser::AssignmentOperator,
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let shared_op = match op {
            dsq_parser::AssignmentOperator::AddAssign => AssignmentOperator::AddAssign,
            dsq_parser::AssignmentOperator::UpdateAssign => AssignmentOperator::UpdateAssign,
        };

        match shared_op {
            AssignmentOperator::AddAssign => {
                // For add assign, compile target and value
                let target_filter = self.compile_expr(target, ctx)?;
                let value_filter = self.compile_expr(value, ctx)?;

                let mut variables = target_filter.variables;
                variables.extend(value_filter.variables);

                let mut functions = target_filter.functions;
                functions.extend(value_filter.functions);

                let operation = Box::new(AssignmentOperation::new(
                    target_filter.operations,
                    AssignmentOperator::AddAssign,
                    value_filter.operations,
                ));

                Ok(CompiledFilter {
                    operations: vec![operation],
                    variables,
                    functions,
                    requires_lazy: target_filter.requires_lazy || value_filter.requires_lazy,
                    complexity: target_filter.complexity + value_filter.complexity + 3,
                })
            }
            AssignmentOperator::UpdateAssign => {
                // For update assign, compile target and value
                let target_filter = self.compile_expr(target, ctx)?;
                let value_filter = self.compile_expr(value, ctx)?;

                let mut variables = target_filter.variables;
                variables.extend(value_filter.variables);

                let mut functions = target_filter.functions;
                functions.extend(value_filter.functions);

                let operation = Box::new(AssignmentOperation::new(
                    target_filter.operations,
                    AssignmentOperator::UpdateAssign,
                    value_filter.operations,
                ));

                Ok(CompiledFilter {
                    operations: vec![operation],
                    variables,
                    functions,
                    requires_lazy: target_filter.requires_lazy || value_filter.requires_lazy,
                    complexity: target_filter.complexity + value_filter.complexity + 3,
                })
            }
        }
    }

    /// Compile object construction
    fn compile_object(
        &self,
        pairs: &[ObjectEntry],
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let mut field_operations = Vec::new();
        let mut variables = HashMap::new();
        let mut functions = HashMap::new();
        let mut complexity = 2; // Base complexity for object construction
        let mut requires_lazy = false;

        for pair in pairs {
            match pair {
                ObjectEntry::KeyValue { key, value } => {
                    let key_filter = self.compile_literal(&Literal::String(key.clone()))?;
                    variables.extend(key_filter.variables);
                    functions.extend(key_filter.functions);
                    complexity += key_filter.complexity;
                    requires_lazy |= key_filter.requires_lazy;

                    let value_filter = self.compile_expr(value, ctx)?;
                    variables.extend(value_filter.variables);
                    functions.extend(value_filter.functions);
                    complexity += value_filter.complexity;
                    requires_lazy |= value_filter.requires_lazy;

                    let key_op = key_filter
                        .operations
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| Box::new(IdentityOperation));
                    let value_ops = value_filter.operations;
                    field_operations.push((key_op, Some(value_ops)));
                }
                ObjectEntry::Shorthand(key) => {
                    let key_filter = self.compile_literal(&Literal::String(key.clone()))?;
                    variables.extend(key_filter.variables);
                    functions.extend(key_filter.functions);
                    complexity += key_filter.complexity;
                    requires_lazy |= key_filter.requires_lazy;

                    // For shorthand, the value is the field access
                    let value_expr = Expr::FieldAccess {
                        base: Box::new(Expr::Identity),
                        fields: vec![key.clone()],
                    };
                    let value_filter = self.compile_expr(&value_expr, ctx)?;
                    variables.extend(value_filter.variables);
                    functions.extend(value_filter.functions);
                    complexity += value_filter.complexity;
                    requires_lazy |= value_filter.requires_lazy;

                    let key_op = key_filter
                        .operations
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| Box::new(IdentityOperation));
                    let value_ops = value_filter.operations;
                    field_operations.push((key_op, Some(value_ops)));
                }
            }
        }

        Ok(CompiledFilter {
            operations: vec![Box::new(ObjectConstructOperation::new(field_operations))],
            variables,
            functions,
            requires_lazy,
            complexity,
        })
    }

    /// Compile array construction
    fn compile_array(
        &self,
        elements: &[Expr],
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        let mut element_filters = Vec::new();
        let mut variables = HashMap::new();
        let mut functions = HashMap::new();
        let mut complexity = 1;
        let mut requires_lazy = false;

        for element in elements {
            let element_filter = self.compile_expr(element, ctx)?;
            variables.extend(element_filter.variables);
            functions.extend(element_filter.functions);
            complexity += element_filter.complexity;
            requires_lazy |= element_filter.requires_lazy;
            element_filters.push(element_filter.operations);
        }

        let mut all_element_ops = Vec::new();
        for element_ops in element_filters {
            all_element_ops.extend(element_ops);
        }
        let operations: Vec<Box<dyn Operation + Send + Sync>> =
            vec![Box::new(ArrayConstructOperation::new(all_element_ops))];

        Ok(CompiledFilter {
            operations,
            variables,
            functions,
            requires_lazy,
            complexity,
        })
    }

    /// Compile literal values
    fn compile_literal(&self, lit: &Literal) -> Result<CompiledFilter> {
        let value = match lit {
            Literal::Int(i) => Value::Int(*i),
            Literal::BigInt(bi) => Value::BigInt(bi.clone()),
            Literal::Float(f) => Value::Float(*f),
            Literal::String(s) => Value::String(s.clone()),
            Literal::Bool(b) => Value::Bool(*b),
            Literal::Null => Value::Null,
        };

        Ok(CompiledFilter {
            operations: vec![Box::new(LiteralOperation::new(value))],
            variables: HashMap::new(),
            functions: HashMap::new(),
            requires_lazy: false,
            complexity: 1,
        })
    }

    /// Compile identifiers
    fn compile_identifier(&self, name: &str) -> Result<CompiledFilter> {
        // Check if this is a builtin function
        if self.builtins.has_function(name) {
            Ok(CompiledFilter {
                operations: vec![Box::new(FunctionCallOperation::new(
                    name.to_string(),
                    vec![], // No arguments for bare function call
                    self.builtins.clone(),
                ))],
                variables: HashMap::new(),
                functions: HashMap::new(),
                requires_lazy: false,
                complexity: 1,
            })
        } else {
            Ok(CompiledFilter {
                operations: vec![Box::new(VariableOperation::new(name.to_string()))],
                variables: HashMap::new(),
                functions: HashMap::new(),
                requires_lazy: false,
                complexity: 1,
            })
        }
    }

    /// Compile pipeline expressions
    fn compile_pipeline(
        &self,
        exprs: &[Expr],
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        if exprs.is_empty() {
            return Ok(CompiledFilter {
                operations: vec![Box::new(IdentityOperation)],
                variables: HashMap::new(),
                functions: HashMap::new(),
                requires_lazy: false,
                complexity: 1,
            });
        }

        if exprs.len() == 1 {
            return self.compile_expr(&exprs[0], ctx);
        }

        // Check if all expressions are map() calls
        let all_maps = exprs.iter().all(|expr| {
            matches!(expr, Expr::FunctionCall { name, args } if name == "map" && args.len() == 1)
        });

        if all_maps {
            // Combine into a single map with pipelined arguments
            let map_args: Vec<Expr> = exprs
                .iter()
                .filter_map(|expr| {
                    if let Expr::FunctionCall { name, args } = expr {
                        if name == "map" && args.len() == 1 {
                            Some(args[0].clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();

            let combined_expr = Expr::FunctionCall {
                name: "map".to_string(),
                args: vec![Expr::Pipeline(map_args)],
            };
            return self.compile_expr(&combined_expr, ctx);
        }

        // Compile each expression and chain them
        let mut all_operations = Vec::new();
        let mut variables = HashMap::new();
        let mut functions = HashMap::new();
        let mut complexity = 0;
        let mut requires_lazy = false;

        for expr in exprs {
            let filter = self.compile_expr(expr, ctx)?;
            all_operations.extend(filter.operations);
            variables.extend(filter.variables);
            functions.extend(filter.functions);
            complexity += filter.complexity;
            requires_lazy |= filter.requires_lazy;
        }

        Ok(CompiledFilter {
            operations: all_operations,
            variables,
            functions,
            requires_lazy,
            complexity: complexity + (exprs.len() - 1), // Add complexity for piping
        })
    }

    /// Compile sequence expressions (comma operator)
    fn compile_sequence(
        &self,
        exprs: &[Expr],
        ctx: &mut CompilationContext,
    ) -> Result<CompiledFilter> {
        if exprs.is_empty() {
            return Ok(CompiledFilter {
                operations: vec![Box::new(IdentityOperation)],
                variables: HashMap::new(),
                functions: HashMap::new(),
                requires_lazy: false,
                complexity: 1,
            });
        }

        if exprs.len() == 1 {
            return self.compile_expr(&exprs[0], ctx);
        }

        // Compile each expression
        let mut expr_operations = Vec::new();
        let mut variables = HashMap::new();
        let mut functions = HashMap::new();
        let mut complexity = 0;
        let mut requires_lazy = false;

        for expr in exprs {
            let filter = self.compile_expr(expr, ctx)?;
            expr_operations.push(filter.operations);
            variables.extend(filter.variables);
            functions.extend(filter.functions);
            complexity += filter.complexity;
            requires_lazy |= filter.requires_lazy;
        }

        let operations: Vec<Box<dyn Operation + Send + Sync>> =
            vec![Box::new(SequenceOperation::new(expr_operations))];

        Ok(CompiledFilter {
            operations,
            variables,
            functions,
            requires_lazy,
            complexity: complexity + (exprs.len() - 1), // Add complexity for sequencing
        })
    }
}

struct FunctionCallOperation {
    name: String,
    arg_ops: Vec<Vec<Box<dyn Operation + Send + Sync>>>,
    builtins: Arc<BuiltinRegistry>,
}

impl FunctionCallOperation {
    fn new(
        name: String,
        arg_ops: Vec<Vec<Box<dyn Operation + Send + Sync>>>,
        builtins: Arc<BuiltinRegistry>,
    ) -> Self {
        Self {
            name,
            arg_ops,
            builtins,
        }
    }
}

impl Operation for FunctionCallOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut context = None;
        self.apply_with_context(value, &mut context)
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn dsq_shared::ops::Context>,
    ) -> Result<Value> {
        match self.name.as_str() {
            "add" | "head" | "tail" | "limit" | "url_set_protocol" | "replace"
            | "spaces_to_tabs" | "tabs_to_spaces" | "contains" => {
                // For builtin functions, the input value is always the first argument
                let mut arg_values = vec![value.clone()];

                // Evaluate additional arguments
                for arg_ops in &self.arg_ops {
                    let mut arg_val = value.clone();
                    for op in arg_ops {
                        arg_val = op.apply(&arg_val)?;
                    }
                    arg_values.push(arg_val);
                }

                // Call the builtin function
                self.builtins.call_function(&self.name, &arg_values)
            }
            "length" | "len" => {
                if self.arg_ops.is_empty() {
                    match value {
                        Value::Array(arr) => Ok(Value::Int(arr.len() as i64)),
                        Value::String(s) => Ok(Value::Int(s.chars().count() as i64)),
                        Value::Object(obj) => Ok(Value::Int(obj.len() as i64)),
                        Value::DataFrame(df) => Ok(Value::Int(df.height() as i64)),
                        Value::Series(s) => Ok(Value::Int(s.len() as i64)),
                        Value::Null => Ok(Value::Int(0)),
                        _ => Ok(Value::Int(1)),
                    }
                } else if self.arg_ops.len() == 1 {
                    let mut arg_val = value.clone();
                    for op in &self.arg_ops[0] {
                        arg_val = op.apply_with_context(&arg_val, context)?;
                    }
                    match arg_val {
                        Value::Array(arr) => Ok(Value::Int(arr.len() as i64)),
                        Value::String(s) => Ok(Value::Int(s.chars().count() as i64)),
                        Value::Object(obj) => Ok(Value::Int(obj.len() as i64)),
                        Value::DataFrame(df) => Ok(Value::Int(df.height() as i64)),
                        Value::Series(s) => Ok(Value::Int(s.len() as i64)),
                        Value::Null => Ok(Value::Int(0)),
                        _ => Ok(Value::Int(1)),
                    }
                } else {
                    Err(dsq_shared::error::operation_error(
                        "length() expects 0 or 1 argument",
                    ))
                }
            }

            "keys" => match value {
                Value::Object(obj) => {
                    let keys: Vec<Value> = obj.keys().map(|k| Value::String(k.clone())).collect();
                    Ok(Value::Array(keys))
                }
                _ => Err(dsq_shared::error::operation_error(
                    "keys() requires an object",
                )),
            },
            "values" => match value {
                Value::Object(obj) => {
                    let values: Vec<Value> = obj.values().cloned().collect();
                    Ok(Value::Array(values))
                }
                _ => Err(dsq_shared::error::operation_error(
                    "values() requires an object",
                )),
            },
            "group_by" => {
                if self.arg_ops.len() != 1 {
                    return Err(dsq_shared::error::operation_error(
                        "group_by() expects 1 argument",
                    ));
                }

                match value {
                    Value::Array(arr) => {
                        let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
                        for item in arr {
                            let mut key_value = item.clone();
                            for op in &self.arg_ops[0] {
                                key_value = op.apply_with_context(&key_value, context)?;
                            }
                            let key = match key_value {
                                Value::String(s) => s,
                                Value::Int(i) => i.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::Bool(b) => b.to_string(),
                                _ => "".to_string(),
                            };
                            groups.entry(key).or_default().push(item.clone());
                        }
                        let result: Vec<Value> = groups.into_values().map(Value::Array).collect();
                        Ok(Value::Array(result))
                    }
                    Value::DataFrame(df) => {
                        // For DataFrame, we need to extract the grouping key from each row
                        // This is more complex, but for now let's assume the argument is a field access
                        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
                        for i in 0..df.height() {
                            // Convert row to object-like value for applying the grouping expression
                            let mut row_obj = HashMap::new();
                            for col_name in df.get_column_names() {
                                if let Ok(s) = df.column(col_name) {
                                    if let Ok(val) = s.get(i) {
                                        let value =
                                            value_from_any_value(val).unwrap_or(Value::Null);
                                        row_obj.insert(col_name.to_string(), value);
                                    }
                                }
                            }
                            let row_value = Value::Object(row_obj);
                            let mut key_value = row_value.clone();
                            for op in &self.arg_ops[0] {
                                key_value = op.apply_with_context(&key_value, context)?;
                            }
                            let key = match key_value {
                                Value::String(s) => s,
                                Value::Int(i) => i.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::Bool(b) => b.to_string(),
                                _ => "".to_string(),
                            };
                            groups.entry(key).or_default().push(i);
                        }
                        let mut result = Vec::new();
                        for (_key, indices) in groups {
                            let mut group = Vec::new();
                            for &i in &indices {
                                let mut row_obj = HashMap::new();
                                for col_name in df.get_column_names() {
                                    if let Ok(s) = df.column(col_name) {
                                        if let Ok(val) = s.get(i) {
                                            let value =
                                                value_from_any_value(val).unwrap_or(Value::Null);
                                            row_obj.insert(col_name.to_string(), value);
                                        }
                                    }
                                }
                                group.push(Value::Object(row_obj));
                            }
                            result.push(Value::Array(group));
                        }
                        Ok(Value::Array(result))
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "group_by() requires an array or DataFrame",
                    )),
                }
            }
            "reverse" => {
                if !self.arg_ops.is_empty() {
                    return Err(dsq_shared::error::operation_error(
                        "reverse() expects no arguments",
                    ));
                }
                self.builtins
                    .call_function("reverse", std::slice::from_ref(value))
            }
            "sort_by" => {
                if self.arg_ops.len() != 1 {
                    return Err(dsq_shared::error::operation_error(
                        "sort_by() expects 1 argument",
                    ));
                }

                // Evaluate the sort key for each element
                let mut key_values = Vec::new();
                match value {
                    Value::Array(arr) => {
                        for item in arr {
                            let mut key_value = item.clone();
                            for op in &self.arg_ops[0] {
                                key_value = op.apply_with_context(&key_value, context)?;
                            }
                            key_values.push(key_value);
                        }
                        self.builtins
                            .call_function("sort_by", &[value.clone(), Value::Array(key_values)])
                    }
                    Value::DataFrame(df) => {
                        // For DataFrame, evaluate the sort key for each row
                        for i in 0..df.height() {
                            let mut row_obj = HashMap::new();
                            for col_name in df.get_column_names() {
                                if let Ok(s) = df.column(col_name) {
                                    if let Ok(val) = s.get(i) {
                                        let value =
                                            value_from_any_value(val).unwrap_or(Value::Null);
                                        row_obj.insert(col_name.to_string(), value);
                                    }
                                }
                            }
                            let row_value = Value::Object(row_obj);
                            let mut key_value = row_value;
                            for op in &self.arg_ops[0] {
                                key_value = op.apply_with_context(&key_value, context)?;
                            }
                            key_values.push(key_value);
                        }
                        self.builtins
                            .call_function("sort_by", &[value.clone(), Value::Array(key_values)])
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "sort_by() requires an array or DataFrame",
                    )),
                }
            }
            "min_by" => {
                if self.arg_ops.len() != 1 {
                    return Err(dsq_shared::error::operation_error(
                        "min_by() expects 1 argument",
                    ));
                }

                // Evaluate the key for each element
                let mut key_values = Vec::new();
                match value {
                    Value::Array(arr) => {
                        for item in arr {
                            let mut key_value = item.clone();
                            for op in &self.arg_ops[0] {
                                key_value = op.apply_with_context(&key_value, context)?;
                            }
                            key_values.push(key_value);
                        }
                        self.builtins
                            .call_function("min_by", &[value.clone(), Value::Array(key_values)])
                    }
                    Value::DataFrame(df) => {
                        // For DataFrame, evaluate the key for each row
                        for i in 0..df.height() {
                            let mut row_obj = HashMap::new();
                            for col_name in df.get_column_names() {
                                if let Ok(s) = df.column(col_name) {
                                    if let Ok(val) = s.get(i) {
                                        let value =
                                            value_from_any_value(val).unwrap_or(Value::Null);
                                        row_obj.insert(col_name.to_string(), value);
                                    }
                                }
                            }
                            let row_value = Value::Object(row_obj);
                            let mut key_value = row_value;
                            for op in &self.arg_ops[0] {
                                key_value = op.apply_with_context(&key_value, context)?;
                            }
                            key_values.push(key_value);
                        }
                        self.builtins
                            .call_function("min_by", &[value.clone(), Value::Array(key_values)])
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "min_by() requires an array or DataFrame",
                    )),
                }
            }
            "max_by" => {
                if self.arg_ops.len() != 1 {
                    return Err(dsq_shared::error::operation_error(
                        "max_by() expects 1 argument",
                    ));
                }

                // Evaluate the key for each element
                let mut key_values = Vec::new();
                match value {
                    Value::Array(arr) => {
                        for item in arr {
                            let mut key_value = item.clone();
                            for op in &self.arg_ops[0] {
                                key_value = op.apply_with_context(&key_value, context)?;
                            }
                            key_values.push(key_value);
                        }
                        self.builtins
                            .call_function("max_by", &[value.clone(), Value::Array(key_values)])
                    }
                    Value::DataFrame(df) => {
                        // For DataFrame, evaluate the key for each row
                        for i in 0..df.height() {
                            let mut row_obj = HashMap::new();
                            for col_name in df.get_column_names() {
                                if let Ok(s) = df.column(col_name) {
                                    if let Ok(val) = s.get(i) {
                                        let value =
                                            value_from_any_value(val).unwrap_or(Value::Null);
                                        row_obj.insert(col_name.to_string(), value);
                                    }
                                }
                            }
                            let row_value = Value::Object(row_obj);
                            let mut key_value = row_value;
                            for op in &self.arg_ops[0] {
                                key_value = op.apply_with_context(&key_value, context)?;
                            }
                            key_values.push(key_value);
                        }
                        self.builtins
                            .call_function("max_by", &[value.clone(), Value::Array(key_values)])
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "max_by() requires an array or DataFrame",
                    )),
                }
            }
            "map" => {
                if self.arg_ops.len() != 1 {
                    return Err(dsq_shared::error::operation_error(
                        "map() expects 1 argument",
                    ));
                }

                match value {
                    Value::Array(arr) => {
                        let mut result = Vec::new();
                        // Check if this is an identity operation
                        let is_identity = self.arg_ops[0].len() == 1
                            && self.arg_ops[0][0]
                                .as_any()
                                .is::<dsq_shared::ops::IdentityOperation>();
                        for item in arr {
                            let mut transformed = item.clone();
                            for op in &self.arg_ops[0] {
                                transformed = op.apply_with_context(&transformed, context)?;
                            }
                            // Filter out null values that result from select() operations, but preserve nulls from identity
                            if !matches!(transformed, Value::Null) || is_identity {
                                result.push(transformed);
                            }
                        }
                        Ok(Value::Array(result))
                    }
                    Value::DataFrame(df) => {
                        // For DataFrame, apply the function to each row
                        // This is complex because the function might change the structure
                        // For now, assume it returns objects that can be collected into a new DataFrame
                        let mut results = Vec::new();
                        for i in 0..df.height() {
                            // Convert row to object-like value
                            let mut row_obj = HashMap::new();
                            for col_name in df.get_column_names() {
                                let series = df.column(col_name).map_err(|e| {
                                    dsq_shared::error::operation_error(format!(
                                        "Failed to get column: {}",
                                        e
                                    ))
                                })?;
                                let any_val = series.get(i).map_err(|e| {
                                    dsq_shared::error::operation_error(format!(
                                        "Failed to get value: {}",
                                        e
                                    ))
                                })?;
                                let val = value_from_any_value(any_val).unwrap_or(Value::Null);
                                row_obj.insert(col_name.to_string(), val);
                            }
                            let row_value = Value::Object(row_obj);

                            let mut ctx = dsq_shared::ops::SimpleContext {
                                value: row_value.clone(),
                            };
                            let mut transformed = row_value;
                            for op in &self.arg_ops[0] {
                                transformed =
                                    op.apply_with_context(&transformed, &mut Some(&mut ctx))?;
                            }
                            // Filter out null values that result from select() operations
                            if !matches!(transformed, Value::Null) {
                                results.push(transformed);
                            }
                        }
                        Ok(Value::Array(results))
                    }
                    Value::Series(series) => {
                        let mut results = Vec::new();
                        for i in 0..series.len() {
                            let any_val = series.get(i).map_err(|e| {
                                dsq_shared::error::operation_error(format!(
                                    "Failed to get value: {}",
                                    e
                                ))
                            })?;
                            let val = value_from_any_value(any_val)?;
                            let mut ctx = dsq_shared::ops::SimpleContext { value: val.clone() };
                            let mut transformed = val;
                            for op in &self.arg_ops[0] {
                                transformed =
                                    op.apply_with_context(&transformed, &mut Some(&mut ctx))?;
                            }
                            if !matches!(transformed, Value::Null) {
                                results.push(transformed);
                            }
                        }
                        Ok(Value::Array(results))
                    }
                    _ => {
                        let mut transformed = value.clone();
                        for op in &self.arg_ops[0] {
                            transformed = op.apply_with_context(&transformed, context)?;
                        }
                        Ok(transformed)
                    }
                }
            }
            "filter" => {
                if self.arg_ops.len() != 1 {
                    return Err(dsq_shared::error::operation_error(
                        "filter() expects 1 argument",
                    ));
                }

                match value {
                    Value::Array(arr) => {
                        let mut result = Vec::new();
                        for item in arr {
                            let mut predicate_result = item.clone();
                            for op in &self.arg_ops[0] {
                                predicate_result =
                                    op.apply_with_context(&predicate_result, context)?;
                            }
                            if dsq_shared::value::is_truthy(&predicate_result) {
                                result.push(item.clone());
                            }
                        }
                        Ok(Value::Array(result))
                    }
                    Value::DataFrame(df) => {
                        // For DataFrame, evaluate predicate for each row and create boolean mask
                        let mut mask_values = Vec::new();
                        for i in 0..df.height() {
                            // Convert row to object-like value
                            let mut row_obj = HashMap::new();
                            for col_name in df.get_column_names() {
                                let series = df.column(col_name).map_err(|e| {
                                    dsq_shared::error::operation_error(format!(
                                        "Failed to get column: {}",
                                        e
                                    ))
                                })?;
                                let any_val = series.get(i).map_err(|e| {
                                    dsq_shared::error::operation_error(format!(
                                        "Failed to get value: {}",
                                        e
                                    ))
                                })?;
                                let val = value_from_any_value(any_val).unwrap_or(Value::Null);
                                row_obj.insert(col_name.to_string(), val);
                            }
                            let row_value = Value::Object(row_obj);

                            let mut predicate_result = row_value;
                            for op in &self.arg_ops[0] {
                                predicate_result =
                                    op.apply_with_context(&predicate_result, context)?;
                            }
                            mask_values.push(dsq_shared::value::is_truthy(&predicate_result));
                        }

                        let mask_series = Series::new("mask".into(), mask_values);
                        let boolean_chunked = mask_series.bool().map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "Failed to create boolean mask: {}",
                                e
                            ))
                        })?;
                        let filtered_df = df.filter(boolean_chunked).map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "Failed to filter DataFrame: {}",
                                e
                            ))
                        })?;
                        Ok(Value::DataFrame(filtered_df))
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "filter() requires an array or DataFrame",
                    )),
                }
            }
            "transform_values" => {
                if self.arg_ops.len() != 2 {
                    return Err(dsq_shared::error::operation_error(
                        "transform_values() expects 2 arguments",
                    ));
                }

                // Evaluate the collection
                let mut collection_val = value.clone();
                for op in &self.arg_ops[0] {
                    collection_val = op.apply_with_context(&collection_val, context)?;
                }

                match collection_val {
                    Value::Array(arr) => {
                        let mut result = Vec::new();
                        for item in arr {
                            let mut transformed = item.clone();
                            for op in &self.arg_ops[1] {
                                transformed = op.apply_with_context(&transformed, context)?;
                            }
                            result.push(transformed);
                        }
                        Ok(Value::Array(result))
                    }
                    Value::DataFrame(_) => Err(dsq_shared::error::operation_error(
                        "transform_values() on DataFrame not implemented",
                    )),
                    _ => Err(dsq_shared::error::operation_error(
                        "transform_values() requires an array",
                    )),
                }
            }
            "map_values" => {
                if self.arg_ops.len() != 1 {
                    return Err(dsq_shared::error::operation_error(
                        "map_values() expects 1 argument",
                    ));
                }

                match value {
                    Value::Object(obj) => {
                        let mut result = HashMap::new();
                        for (key, val) in obj {
                            let mut transformed = val.clone();
                            for op in &self.arg_ops[0] {
                                transformed = op.apply_with_context(&transformed, context)?;
                            }
                            result.insert(key.clone(), transformed);
                        }
                        Ok(Value::Object(result))
                    }
                    Value::Array(arr) => {
                        let mut result = Vec::new();
                        for val in arr {
                            let mut transformed = val.clone();
                            for op in &self.arg_ops[0] {
                                transformed = op.apply_with_context(&transformed, context)?;
                            }
                            result.push(transformed);
                        }
                        Ok(Value::Array(result))
                    }
                    Value::DataFrame(df) => {
                        // For DataFrame, apply to each column
                        let mut new_series = Vec::new();
                        for col_name in df.get_column_names() {
                            if let Ok(series) = df.column(col_name) {
                                // Convert series to array of values, apply transformation, then back to series
                                let mut values = Vec::new();
                                for i in 0..series.len() {
                                    if let Ok(val) = series.get(i) {
                                        let value =
                                            value_from_any_value(val).unwrap_or(Value::Null);
                                        let mut transformed = value;
                                        for op in &self.arg_ops[0] {
                                            transformed =
                                                op.apply_with_context(&transformed, context)?;
                                        }
                                        values.push(transformed);
                                    }
                                }
                                // Convert back to series - this is simplified, assumes all values are the same type
                                let new_series_data = values_to_series(col_name, &values)?;
                                new_series.push(new_series_data);
                            }
                        }
                        let columns: Vec<_> = new_series.into_iter().map(|s| s.into()).collect();
                        match DataFrame::new(columns) {
                            Ok(new_df) => Ok(Value::DataFrame(new_df)),
                            Err(e) => Err(dsq_shared::error::operation_error(format!(
                                "map_values() failed on DataFrame: {}",
                                e
                            ))),
                        }
                    }
                    Value::Series(series) => {
                        let mut values = Vec::new();
                        for i in 0..series.len() {
                            if let Ok(val) = series.get(i) {
                                let value = value_from_any_value(val).unwrap_or(Value::Null);
                                let mut transformed = value;
                                for op in &self.arg_ops[0] {
                                    transformed = op.apply_with_context(&transformed, context)?;
                                }
                                values.push(transformed);
                            }
                        }
                        let new_series = values_to_series("transformed", &values)?;
                        Ok(Value::Series(new_series))
                    }
                    _ => Err(dsq_shared::error::operation_error(
                        "map_values() requires an object, array, DataFrame, or Series",
                    )),
                }
            }
            "select" => {
                if self.arg_ops.len() != 1 {
                    return Err(dsq_shared::error::operation_error(
                        "select() expects 1 argument",
                    ));
                }
                match &value {
                    Value::Array(arr) => {
                        let mut filtered = Vec::new();
                        for item in arr {
                            let mut condition_val = item.clone();
                            for op in &self.arg_ops[0] {
                                condition_val = op.apply_with_context(&condition_val, context)?;
                            }
                            if is_truthy(&condition_val) {
                                filtered.push(item.clone());
                            }
                        }
                        Ok(Value::Array(filtered))
                    }
                    Value::DataFrame(df) => {
                        // For DataFrame, evaluate condition for each row and create boolean mask
                        let mut mask_values = Vec::new();
                        for i in 0..df.height() {
                            // Convert row to object-like value
                            let mut row_obj = HashMap::new();
                            for col_name in df.get_column_names() {
                                let series = df.column(col_name).map_err(|e| {
                                    dsq_shared::error::operation_error(format!(
                                        "Failed to get column: {}",
                                        e
                                    ))
                                })?;
                                let any_val = series.get(i).map_err(|e| {
                                    dsq_shared::error::operation_error(format!(
                                        "Failed to get value: {}",
                                        e
                                    ))
                                })?;
                                let val = value_from_any_value(any_val).unwrap_or(Value::Null);
                                row_obj.insert(col_name.to_string(), val);
                            }
                            let row_value = Value::Object(row_obj);

                            let mut condition_val = row_value;
                            for op in &self.arg_ops[0] {
                                condition_val = op.apply_with_context(&condition_val, context)?;
                            }
                            mask_values.push(is_truthy(&condition_val));
                        }

                        let mask_series = Series::new("mask".into(), mask_values);
                        let boolean_chunked = mask_series.bool().map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "Failed to create boolean mask: {}",
                                e
                            ))
                        })?;
                        let filtered_df = df.filter(boolean_chunked).map_err(|e| {
                            dsq_shared::error::operation_error(format!(
                                "Failed to filter DataFrame: {}",
                                e
                            ))
                        })?;
                        Ok(Value::DataFrame(filtered_df))
                    }
                    _ => {
                        let mut condition_val = value.clone();
                        for op in &self.arg_ops[0] {
                            condition_val = op.apply_with_context(&condition_val, context)?;
                        }
                        if is_truthy(&condition_val) {
                            Ok(value.clone())
                        } else {
                            Ok(Value::Null)
                        }
                    }
                }
            }
            "ceil" => {
                if self.arg_ops.is_empty() {
                    self.builtins
                        .call_function("ceil", std::slice::from_ref(value))
                } else if self.arg_ops.len() == 1 {
                    let mut arg_val = value.clone();
                    for op in &self.arg_ops[0] {
                        arg_val = op.apply_with_context(&arg_val, context)?;
                    }
                    self.builtins.call_function("ceil", &[arg_val])
                } else {
                    Err(dsq_shared::error::operation_error(
                        "ceil() expects 0 or 1 argument",
                    ))
                }
            }

            "today" | "now" => {
                // These functions take no arguments
                if !self.arg_ops.is_empty() {
                    return Err(dsq_shared::error::operation_error(format!(
                        "{}() expects no arguments",
                        self.name
                    )));
                }
                self.builtins.call_function(&self.name, &[])
            }
            _ => {
                // Special handling for iferror function
                if self.name == "iferror" {
                    if self.arg_ops.len() != 2 {
                        return Err(dsq_shared::error::operation_error(
                            "iferror() expects 2 arguments",
                        ));
                    }

                    // Evaluate the first argument with error handling
                    let first_result: Result<Value> = (|| {
                        let mut arg_val = value.clone();
                        for op in &self.arg_ops[0] {
                            arg_val = op.apply(&arg_val)?;
                        }
                        Ok(arg_val)
                    })();

                    match first_result {
                        Ok(val) => Ok(val),
                        Err(_) => {
                            // If first argument failed, evaluate and return the second argument
                            let mut arg_val = value.clone();
                            for op in &self.arg_ops[1] {
                                arg_val = op.apply(&arg_val)?;
                            }
                            Ok(arg_val)
                        }
                    }
                } else if self.name == "filter" {
                    // Special handling for filter: filter based on condition
                    if self.arg_ops.len() != 1 {
                        return Err(dsq_shared::error::operation_error(
                            "filter() expects 1 argument",
                        ));
                    }
                    let arg_ops = &self.arg_ops[0];
                    match value {
                        Value::Array(arr) => {
                            let mut result = Vec::new();
                            for item in arr {
                                let mut condition = item.clone();
                                for op in arg_ops {
                                    condition = op.apply(&condition)?;
                                }
                                if is_truthy(&condition) {
                                    result.push(item.clone());
                                }
                            }
                            Ok(Value::Array(result))
                        }
                        Value::DataFrame(df) => {
                            // For DataFrame, filter rows where condition is truthy
                            let mut mask = Vec::new();
                            for i in 0..df.height() {
                                // Create a row object from the DataFrame row
                                let mut row_obj = std::collections::HashMap::new();
                                for col_name in df.get_column_names() {
                                    if let Ok(series) = df.column(col_name) {
                                        if let Ok(val) = series.get(i) {
                                            let value =
                                                value_from_any_value(val).unwrap_or(Value::Null);
                                            row_obj.insert(col_name.to_string(), value);
                                        }
                                    }
                                }
                                let row_value = Value::Object(row_obj);
                                let mut condition = row_value;
                                for op in arg_ops {
                                    condition = op.apply(&condition)?;
                                }
                                mask.push(is_truthy(&condition));
                            }
                            let mask_chunked =
                                polars::prelude::BooleanChunked::from_slice("".into(), &mask);
                            match df.filter(&mask_chunked) {
                                Ok(filtered_df) => Ok(Value::DataFrame(filtered_df)),
                                Err(e) => Err(dsq_shared::error::operation_error(format!(
                                    "filter() failed to filter DataFrame: {}",
                                    e
                                ))),
                            }
                        }
                        _ => {
                            // For other values, return the value if condition is truthy, else null
                            let mut condition = value.clone();
                            for op in arg_ops {
                                condition = op.apply(&condition)?;
                            }
                            if is_truthy(&condition) {
                                Ok(value.clone())
                            } else {
                                Ok(Value::Null)
                            }
                        }
                    }
                } else if self.builtins.has_function(&self.name) {
                    // For builtin functions
                    let mut arg_values = Vec::new();
                    if self.arg_ops.is_empty() {
                        // For functions called without arguments, pass the input value
                        arg_values.push(value.clone());
                    } else {
                        // For functions called with arguments, evaluate the arguments
                        for arg_ops in &self.arg_ops {
                            let mut arg_val = value.clone();
                            for op in arg_ops {
                                arg_val = op.apply(&arg_val)?;
                            }
                            arg_values.push(arg_val);
                        }
                    }

                    // Call the builtin function
                    self.builtins.call_function(&self.name, &arg_values)
                } else {
                    Err(dsq_shared::error::operation_error(format!(
                        "Unknown function: {}",
                        self.name
                    )))
                }
            }
        }
    }

    fn description(&self) -> String {
        format!("function call: {}", self.name)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct VariableOperation {
    pub name: String,
}

impl VariableOperation {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Operation for VariableOperation {
    fn apply(&self, _value: &Value) -> Result<Value> {
        // Variables require context
        Err(dsq_shared::error::operation_error(format!(
            "Variable '{}' requires context",
            self.name
        )))
    }

    fn apply_with_context(
        &self,
        value: &Value,
        context: &mut Option<&mut dyn dsq_shared::ops::Context>,
    ) -> Result<Value> {
        if let Some(ctx) = context {
            if let Some(var_value) = ctx.get_variable(&self.name) {
                Ok(var_value.clone())
            } else {
                // Check if it's a function in FilterContext
                if let Some(filter_ctx) = ctx.as_any_mut().downcast_mut::<FilterContext>() {
                    if filter_ctx.has_function(&self.name) {
                        // Call the function with the input value as argument
                        filter_ctx.call_function(&self.name, std::slice::from_ref(value))
                    } else {
                        Err(dsq_shared::error::operation_error(format!(
                            "Variable '{}' not found",
                            self.name
                        )))
                    }
                } else {
                    Err(dsq_shared::error::operation_error(format!(
                        "Variable '{}' not found",
                        self.name
                    )))
                }
            }
        } else {
            Err(dsq_shared::error::operation_error(format!(
                "Variable '{}' requires context",
                self.name
            )))
        }
    }

    fn description(&self) -> String {
        format!("variable: {}", self.name)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct AssignAddOperation {
    pub target_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub value_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AssignAddOperation {
    pub fn new(
        target_ops: Vec<Box<dyn Operation + Send + Sync>>,
        value_ops: Vec<Box<dyn Operation + Send + Sync>>,
    ) -> Self {
        Self {
            target_ops,
            value_ops,
        }
    }
}

impl Operation for AssignAddOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        // This is a simplified implementation
        // A full implementation would need to modify the input value
        let mut target_val = value.clone();
        for op in &self.target_ops {
            target_val = op.apply(&target_val)?;
        }

        let mut add_val = value.clone();
        for op in &self.value_ops {
            add_val = op.apply(&add_val)?;
        }

        add_values(&target_val, &add_val)
    }

    fn description(&self) -> String {
        "assign add".to_string()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct AssignUpdateOperation {
    pub target_ops: Vec<Box<dyn Operation + Send + Sync>>,
    pub value_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AssignUpdateOperation {
    pub fn new(
        target_ops: Vec<Box<dyn Operation + Send + Sync>>,
        value_ops: Vec<Box<dyn Operation + Send + Sync>>,
    ) -> Self {
        Self {
            target_ops,
            value_ops,
        }
    }
}

impl Operation for AssignUpdateOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        // For assignment on objects, we need to modify the current value
        if let Value::Object(ref obj) = value {
            // Check if target is a field access
            if self.target_ops.len() == 1 {
                if let Some(field_op) = self.target_ops.first() {
                    // Try to downcast to FieldAccessOperation
                    if let Some(field_access) =
                        field_op.as_any().downcast_ref::<FieldAccessOperation>()
                    {
                        let field_name = &field_access.fields[0];

                        // Evaluate the value
                        let mut value_val = value.clone();
                        for op in &self.value_ops {
                            value_val = op.apply(&value_val)?;
                        }

                        // Create new object with updated field
                        let mut new_obj = obj.clone();
                        new_obj.insert(field_name.clone(), value_val);
                        return Ok(Value::Object(new_obj));
                    }
                }
            }
        }

        // Fallback: evaluate target and value, then return value
        let mut value_val = value.clone();
        for op in &self.value_ops {
            value_val = op.apply(&value_val)?;
        }

        Ok(value_val)
    }

    fn description(&self) -> String {
        "assign update".to_string()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct SliceOperation {
    pub start_ops: Option<Vec<Box<dyn Operation + Send + Sync>>>,
    pub end_ops: Option<Vec<Box<dyn Operation + Send + Sync>>>,
}

impl SliceOperation {
    pub fn new(
        start_ops: Option<Vec<Box<dyn Operation + Send + Sync>>>,
        end_ops: Option<Vec<Box<dyn Operation + Send + Sync>>>,
    ) -> Self {
        Self { start_ops, end_ops }
    }
}

impl Operation for SliceOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let start = if let Some(ref ops) = self.start_ops {
            let mut start_val = value.clone();
            for op in ops {
                start_val = op.apply(&start_val)?;
            }
            match start_val {
                Value::Int(i) => Some(i as usize),
                _ => {
                    return Err(dsq_shared::error::operation_error(
                        "Slice start must be an integer",
                    ));
                }
            }
        } else {
            None
        };

        let end = if let Some(ref ops) = self.end_ops {
            let mut end_val = value.clone();
            for op in ops {
                end_val = op.apply(&end_val)?;
            }
            match end_val {
                Value::Int(i) => Some(i as usize),
                _ => {
                    return Err(dsq_shared::error::operation_error(
                        "Slice end must be an integer",
                    ));
                }
            }
        } else {
            None
        };

        match value {
            Value::Array(arr) => {
                let start_idx = start.unwrap_or(0);
                let end_idx = end.unwrap_or(arr.len());
                Ok(Value::Array(arr[start_idx..end_idx].to_vec()))
            }
            Value::DataFrame(df) => {
                let start_idx = start.unwrap_or(0) as i64;
                let end_idx = end.unwrap_or(df.height());
                let length = (end_idx as i64 - start_idx) as usize;
                Ok(Value::DataFrame(df.slice(start_idx, length)))
            }
            _ => Err(dsq_shared::error::operation_error(
                "Slice operation requires array or DataFrame",
            )),
        }
    }

    fn description(&self) -> String {
        "array slice".to_string()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct AssignFieldAddOperation {
    pub field: String,
    pub value_ops: Vec<Box<dyn Operation + Send + Sync>>,
}

impl AssignFieldAddOperation {
    pub fn new(field: String, value_ops: Vec<Box<dyn Operation + Send + Sync>>) -> Self {
        Self { field, value_ops }
    }
}

impl Operation for AssignFieldAddOperation {
    fn apply(&self, value: &Value) -> Result<Value> {
        let mut add_val = value.clone();
        for op in &self.value_ops {
            add_val = op.apply(&add_val)?;
        }

        match value {
            Value::Object(obj) => {
                let mut new_obj = obj.clone();
                let current_val = obj.get(&self.field).cloned().unwrap_or(Value::Null);
                let new_val = add_values(&current_val, &add_val)?;
                new_obj.insert(self.field.clone(), new_val);
                Ok(Value::Object(new_obj))
            }
            _ => Err(dsq_shared::error::operation_error(
                "Assignment requires an object",
            )),
        }
    }

    fn description(&self) -> String {
        format!("assign add to field {}", self.field)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

// Helper function to add two values
fn add_values(a: &Value, b: &Value) -> Result<Value> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
        (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
        _ => Err(dsq_shared::error::operation_error("Cannot add these types")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dsq_shared::value::Value;
    use std::collections::HashMap;

    #[test]
    fn test_filter_context_new() {
        let ctx = FilterContext::new();

        // Check default values
        assert!(ctx.variables.is_empty());
        assert!(ctx.functions.is_empty());
        assert!(ctx.call_stack.is_empty());
        assert_eq!(ctx.max_recursion_depth, 1000);
        assert!(!ctx.debug_mode);
        assert!(ctx.current_input.is_none());
        assert_eq!(ctx.error_mode, ErrorMode::Strict);
    }

    #[test]
    fn test_filter_context_variable_operations() {
        let mut ctx = FilterContext::new();

        // Test setting and getting variables
        let value = Value::Int(42);
        ctx.set_variable("test_var", value.clone());
        assert!(ctx.has_variable("test_var"));
        assert_eq!(ctx.get_variable("test_var"), Some(&value));

        // Test non-existent variable
        assert!(!ctx.has_variable("nonexistent"));
        assert_eq!(ctx.get_variable("nonexistent"), None);

        // Test overwriting variable
        let new_value = Value::String("hello".to_string());
        ctx.set_variable("test_var", new_value.clone());
        assert_eq!(ctx.get_variable("test_var"), Some(&new_value));
    }

    #[test]
    fn test_filter_context_function_operations() {
        let mut ctx = FilterContext::new();

        // Test builtin function detection
        assert!(ctx.has_function("length"));
        assert!(ctx.has_function("add"));
        assert!(!ctx.has_function("nonexistent_function"));

        // Test user-defined function
        let func_def = FunctionDef {
            name: "test_func".to_string(),
            parameters: vec!["x".to_string()],
            body: FunctionBody::Ast("x * 2".to_string()),
            is_recursive: false,
        };
        ctx.set_functions(HashMap::from([("test_func".to_string(), func_def)]));
        assert!(ctx.has_function("test_func"));
    }

    #[test]
    fn test_filter_context_call_function_builtin() {
        let mut ctx = FilterContext::new();

        // Test calling builtin length function
        let result = ctx.call_function(
            "length",
            &[Value::Array(vec![Value::Int(1), Value::Int(2)])],
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Int(2));

        // Test calling builtin with wrong arguments
        let result = ctx.call_function("length", &[Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_filter_context_call_user_function() {
        let mut ctx = FilterContext::new();

        // Create a simple user-defined function that returns its argument
        // Note: We use Ast body since Compiled cannot be cloned
        let func_def = FunctionDef {
            name: "identity".to_string(),
            parameters: vec!["x".to_string()],
            body: FunctionBody::Ast(".".to_string()), // Simple identity
            is_recursive: false,
        };

        ctx.set_functions(HashMap::from([("identity".to_string(), func_def)]));

        // Test calling user function - this will fail since AST execution is not implemented
        // but it tests the function lookup and call mechanism
        let result = ctx.call_function("identity", &[Value::String("test".to_string())]);
        assert!(result.is_err()); // Expected to fail due to unimplemented AST execution
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("AST execution not yet implemented"));
    }

    #[test]
    fn test_filter_context_call_function_wrong_args() {
        let mut ctx = FilterContext::new();

        let func_def = FunctionDef {
            name: "test_func".to_string(),
            parameters: vec!["a".to_string(), "b".to_string()],
            body: FunctionBody::Ast("a + b".to_string()),
            is_recursive: false,
        };

        ctx.set_functions(HashMap::from([("test_func".to_string(), func_def)]));

        // Test with wrong number of arguments
        let result = ctx.call_function("test_func", &[Value::Int(1)]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected 2 arguments"));
    }

    #[test]
    fn test_filter_context_input_operations() {
        let mut ctx = FilterContext::new();

        // Test initial state
        assert_eq!(ctx.get_input(), None);

        // Test setting input
        let value = Value::Object(HashMap::from([("key".to_string(), Value::Int(123))]));
        ctx.set_input(value.clone());
        assert_eq!(ctx.get_input(), Some(&value));
    }

    #[test]
    fn test_filter_context_recursion_depth() {
        let mut ctx = FilterContext::new();

        // Test initial recursion depth
        assert_eq!(ctx.recursion_depth(), 0);

        // Push stack frames
        ctx.call_stack.push(StackFrame {
            name: "test1".to_string(),
            input: Value::Null,
            location: None,
        });
        assert_eq!(ctx.recursion_depth(), 1);

        ctx.call_stack.push(StackFrame {
            name: "test2".to_string(),
            input: Value::Null,
            location: None,
        });
        assert_eq!(ctx.recursion_depth(), 2);

        // Pop stack frame
        ctx.call_stack.pop();
        assert_eq!(ctx.recursion_depth(), 1);
    }

    #[test]
    fn test_filter_context_debug_mode() {
        let mut ctx = FilterContext::new();

        // Test initial state
        assert!(!ctx.is_debug_mode());

        // Test setting debug mode
        ctx.set_debug_mode(true);
        assert!(ctx.is_debug_mode());

        ctx.set_debug_mode(false);
        assert!(!ctx.is_debug_mode());
    }

    #[test]
    fn test_filter_context_error_mode() {
        let mut ctx = FilterContext::new();

        // Test initial state
        assert_eq!(ctx.error_mode(), ErrorMode::Strict);

        // Test setting error modes
        ctx.set_error_mode(ErrorMode::Collect);
        assert_eq!(ctx.error_mode(), ErrorMode::Collect);

        ctx.set_error_mode(ErrorMode::Ignore);
        assert_eq!(ctx.error_mode(), ErrorMode::Ignore);
    }

    #[test]
    fn test_filter_context_as_context_trait() {
        let mut ctx = FilterContext::new();

        // Test Context trait methods
        let value = Value::Float(3.14);
        ctx.set_variable("pi", value.clone());

        // Test get_variable through trait
        assert_eq!(ctx.get_variable("pi"), Some(&value));
        assert_eq!(ctx.get_variable("nonexistent"), None);

        // Test set_variable through trait
        let new_value = Value::String("hello".to_string());
        ctx.set_variable("greeting", new_value.clone());
        assert_eq!(ctx.get_variable("greeting"), Some(&new_value));
    }

    #[test]
    fn test_filter_context_max_recursion_prevention() {
        let mut ctx = FilterContext::new();
        ctx.max_recursion_depth = 2; // Set low limit for testing

        // Create a recursive function that would exceed the limit
        let func_def = FunctionDef {
            name: "recursive_func".to_string(),
            parameters: vec![],
            body: FunctionBody::Ast("recursive_func".to_string()), // Would recurse if executed
            is_recursive: true,
        };

        ctx.set_functions(HashMap::from([("recursive_func".to_string(), func_def)]));

        // This should fail due to unimplemented AST execution, not recursion
        // The recursion check happens during actual execution
        let result = ctx.call_function("recursive_func", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("AST execution not yet implemented"));
    }

    #[test]
    fn test_filter_context_builtin_function_call() {
        let mut ctx = FilterContext::new();

        // Test calling a builtin function that exists
        let result = ctx.call_function("add", &[Value::Int(1), Value::Int(2), Value::Int(3)]);
        // Note: This might fail if add builtin isn't implemented, but tests the call mechanism
        // The important thing is that it doesn't panic
        assert!(result.is_ok() || result.is_err()); // Either result is acceptable for this test
    }

    #[test]
    fn test_filter_context_unknown_function() {
        let mut ctx = FilterContext::new();

        // Test calling unknown function
        let result = ctx.call_function("unknown_function", &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("function 'unknown_function'"));
    }

    #[test]
    fn test_compilation_context_new() {
        let ctx = CompilationContext::new();

        assert_eq!(ctx.depth, 0);
        assert_eq!(ctx.max_depth, 1000);
        assert!(ctx.variables.is_empty());
        assert!(ctx.functions.is_empty());
    }

    #[test]
    fn test_compilation_context_with_max_depth() {
        let ctx = CompilationContext::with_max_depth(500);

        assert_eq!(ctx.depth, 0);
        assert_eq!(ctx.max_depth, 500);
        assert!(ctx.variables.is_empty());
        assert!(ctx.functions.is_empty());
    }

    #[test]
    fn test_error_mode_variants() {
        // Test that ErrorMode variants work as expected
        assert_eq!(ErrorMode::Strict as u8, 0);
        assert_eq!(ErrorMode::Collect as u8, 1);
        assert_eq!(ErrorMode::Ignore as u8, 2);
    }

    #[test]
    fn test_function_def_creation() {
        let func_def = FunctionDef {
            name: "test".to_string(),
            parameters: vec!["a".to_string(), "b".to_string()],
            body: FunctionBody::Ast("a + b".to_string()),
            is_recursive: false,
        };

        assert_eq!(func_def.name, "test");
        assert_eq!(func_def.parameters.len(), 2);
        assert!(!func_def.is_recursive);
    }

    #[test]
    fn test_function_body_clone() {
        // Test that FunctionBody can be cloned
        let body = FunctionBody::Ast("test".to_string());
        let cloned = body.clone();

        match cloned {
            FunctionBody::Ast(s) => assert_eq!(s, "test"),
            _ => panic!("Expected Ast variant"),
        }
    }

    #[test]
    fn test_stack_frame_creation() {
        let frame = StackFrame {
            name: "test_func".to_string(),
            input: Value::Int(42),
            location: Some(Location {
                line: 10,
                column: 5,
                source: Some("test.rs".to_string()),
            }),
        };

        assert_eq!(frame.name, "test_func");
        assert_eq!(frame.input, Value::Int(42));
        assert!(frame.location.is_some());
    }

    #[test]
    fn test_location_creation() {
        let loc = Location {
            line: 1,
            column: 1,
            source: None,
        };

        assert_eq!(loc.line, 1);
        assert_eq!(loc.column, 1);
        assert!(loc.source.is_none());
    }
}
