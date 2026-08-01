//! Open-source attributions for the dependencies this binary links.
//!
//! rapidgzip prints the licenses of the libraries it bundles, so packagers can
//! collect them without inspecting the build. This binary links far fewer, and
//! lists exactly those.

/// Human-readable attributions, one entry per linked dependency.
pub const PLAIN: &str = "\
rapidgzip-rust and rapidgzip-core
    BSD-3-Clause AND MIT
    https://github.com/COMBINE-lab/rapidgzip-rust
    Decoder-only Rust implementation of the rapidgzip approach. The
    marker/window algorithm follows rapidgzip by Maximilian Knespel,
    BSD-3-Clause AND MIT, https://github.com/mxmlnkn/rapidgzip

zlib-rs, linked through libz-rs-sys
    Zlib
    https://github.com/trifectatechfoundation/zlib-rs
    Safe Rust implementation of zlib, used for raw inflate.

crossbeam-deque
    MIT OR Apache-2.0
    https://github.com/crossbeam-rs/crossbeam
    Work-stealing deques for the decoder task queues.

clap
    MIT OR Apache-2.0
    https://github.com/clap-rs/clap
    Command-line argument parsing.

ISA-L, linked only when built with the optional `isal` feature
    BSD-3-Clause
    https://github.com/intel/isa-l
    Intel Intelligent Storage Acceleration Library. Not linked by default
    builds, and never bundled: the system library is used.
";

/// The same attributions as YAML, which is what Conda packaging consumes.
pub const YAML: &str = "\
---
- name: rapidgzip-rust
  license: BSD-3-Clause AND MIT
  url: https://github.com/COMBINE-lab/rapidgzip-rust
- name: rapidgzip
  license: BSD-3-Clause AND MIT
  url: https://github.com/mxmlnkn/rapidgzip
  note: the marker/window algorithm follows this project
- name: zlib-rs
  license: Zlib
  url: https://github.com/trifectatechfoundation/zlib-rs
- name: crossbeam-deque
  license: MIT OR Apache-2.0
  url: https://github.com/crossbeam-rs/crossbeam
- name: clap
  license: MIT OR Apache-2.0
  url: https://github.com/clap-rs/clap
- name: isa-l
  license: BSD-3-Clause
  url: https://github.com/intel/isa-l
  note: linked only with the optional isal feature, never bundled
";
