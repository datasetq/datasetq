//! Core operations for data processing
//!
//! This module provides the fundamental Operation trait and basic operations
//! that are shared across DSQ crates.

pub mod arithmetic_ops;
pub mod basic_ops;
pub mod comparison_ops;
pub mod construct_ops;
pub mod logical_ops;
pub mod special_ops;
pub mod traits;
pub mod utils;

#[cfg(test)]
mod tests;

// Re-export commonly used types for convenience
pub use arithmetic_ops::{AddOperation, DivOperation, MulOperation, SubOperation};
pub use basic_ops::{
    FieldAccessOperation, IdentityOperation, IndexOperation, IterateOperation, LiteralOperation,
};
pub use comparison_ops::{
    EqOperation, GeOperation, GtOperation, LeOperation, LtOperation, NeOperation,
};
pub use construct_ops::{ArrayConstructOperation, ObjectConstructOperation, SequenceOperation};
pub use logical_ops::{AndOperation, IfOperation, NegationOperation, OrOperation};
pub use special_ops::{
    AssignmentOperation, DelOperation, FunctionCallOperation, JoinFromFileOperation,
};
pub use traits::{AssignmentOperator, Context, Operation, SimpleContext};
pub use utils::{add_values, compare_values, div_values, mul_values, sub_values};
