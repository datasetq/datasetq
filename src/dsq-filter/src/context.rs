//! Filter execution context for dsq
//!
//! This module provides the execution context for filter operations, managing
//! variable bindings, function definitions, and the interface between jaq's
//! filter system and dsq's DataFrame operations.
//!
//! The context maintains state during filter execution and provides access to
//! built-in functions, user-defined functions, and variables.

pub use crate::compiler::{
    BuiltinFunction, CompilationContext, ErrorMode, FilterContext, FunctionBody, FunctionDef,
    Location, StackFrame,
};
pub use dsq_functions::BuiltinRegistry;
