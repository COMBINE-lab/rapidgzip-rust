//! Open-source attributions for components linked by the CLI.

/// Human-readable attributions.
pub const PLAIN: &str = "\
rapidgzip-rust and rapidgzip-core
    BSD-3-Clause AND MIT
    https://github.com/COMBINE-lab/rapidgzip-rust
    Rust implementation of rapidgzip's marker/window approach.

rapidgzip
    BSD-3-Clause AND MIT
    https://github.com/mxmlnkn/rapidgzip
    Original algorithm and command-line interface.

zlib-rs, through libz-rs-sys
    Zlib
    https://github.com/trifectatechfoundation/zlib-rs
    Safe Rust DEFLATE implementation used as the inflate backend.

crossbeam-deque
    MIT OR Apache-2.0
    https://github.com/crossbeam-rs/crossbeam

clap
    MIT OR Apache-2.0
    https://github.com/clap-rs/clap
";

/// Machine-readable YAML attributions.
pub const YAML: &str = "\
---
- name: rapidgzip-rust
  license: BSD-3-Clause AND MIT
  url: https://github.com/COMBINE-lab/rapidgzip-rust
- name: rapidgzip
  license: BSD-3-Clause AND MIT
  url: https://github.com/mxmlnkn/rapidgzip
- name: zlib-rs
  license: Zlib
  url: https://github.com/trifectatechfoundation/zlib-rs
- name: crossbeam-deque
  license: MIT OR Apache-2.0
  url: https://github.com/crossbeam-rs/crossbeam
- name: clap
  license: MIT OR Apache-2.0
  url: https://github.com/clap-rs/clap
";
