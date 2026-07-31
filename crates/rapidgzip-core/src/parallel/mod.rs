//! Building blocks for rapidgzip's speculative marker/window algorithm.
//!
//! These types are public only as a module for architectural documentation and
//! focused benchmarking; the stable decoder API does not expose block indexes.

pub(crate) mod deflate;
mod marker;

pub use marker::{MarkerBuffer, MarkerError, Symbol, Window};
