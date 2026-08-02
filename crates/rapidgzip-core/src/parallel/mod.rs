//! Building blocks for rapidgzip's speculative marker/window algorithm.
//!
//! These types are public only as a module for architectural documentation and
//! focused benchmarking; the stable decoder API exposes only validated
//! [`crate::DeflateIndex`] checkpoints, not these internal speculative blocks.

pub(crate) mod adaptive;
pub(crate) mod deflate;
mod marker;

pub use marker::{MarkerBuffer, MarkerError, Symbol, Window};
