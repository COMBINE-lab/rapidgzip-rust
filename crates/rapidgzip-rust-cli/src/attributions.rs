//! Open-source attributions for the CLI and its complete normal dependency tree.
//!
//! The list is intentionally checked in so attribution output is available at
//! runtime without Cargo metadata or a filesystem installation. Keep it in
//! sync with `cargo tree -p rapidgzip-rust-cli --edges normal`.

/// Names expected in the resolved normal dependency tree.
#[cfg(test)]
pub const DEPENDENCY_NAMES: &[&str] = &[
    "anstream",
    "anstyle",
    "anstyle-parse",
    "anstyle-query",
    "clap",
    "clap_builder",
    "clap_derive",
    "clap_lex",
    "colorchoice",
    "crossbeam-deque",
    "crossbeam-epoch",
    "crossbeam-utils",
    "heck",
    "is_terminal_polyfill",
    "libz-rs-sys",
    "proc-macro2",
    "quote",
    "rapidgzip-core",
    "strsim",
    "syn",
    "unicode-ident",
    "utf8parse",
    "zlib-rs",
];

/// Human-readable attributions.
pub const PLAIN: &str = "\
rapidgzip-rust-cli and rapidgzip-core
    BSD-3-Clause AND MIT
    https://github.com/COMBINE-lab/rapidgzip-rust

rapidgzip (algorithm and CLI inspiration)
    BSD-3-Clause AND MIT
    https://github.com/mxmlnkn/rapidgzip

zlib-rs and libz-rs-sys
    Zlib
    https://github.com/trifectatechfoundation/zlib-rs

crossbeam-deque, crossbeam-epoch, and crossbeam-utils
    MIT OR Apache-2.0
    https://github.com/crossbeam-rs/crossbeam

clap, clap_builder, clap_derive, and clap_lex
    MIT OR Apache-2.0
    https://github.com/clap-rs/clap

anstream, anstyle, anstyle-parse, anstyle-query, and colorchoice
    MIT OR Apache-2.0
    https://github.com/rust-cli/anstyle

is_terminal_polyfill
    MIT OR Apache-2.0
    https://github.com/polyfill-rs/is_terminal_polyfill

utf8parse
    Apache-2.0 OR MIT
    https://github.com/alacritty/vte

strsim
    MIT
    https://github.com/rapidfuzz/strsim-rs

heck
    MIT OR Apache-2.0
    https://github.com/withoutboats/heck

proc-macro2, quote, and syn
    MIT OR Apache-2.0
    https://github.com/dtolnay

unicode-ident
    (MIT OR Apache-2.0) AND Unicode-3.0
    https://github.com/dtolnay/unicode-ident
";

/// Machine-readable YAML attributions.
pub const YAML: &str = "\
---
root_name: rapidgzip-rust-cli
third_party_libraries:
  - name: rapidgzip
    license: BSD-3-Clause AND MIT
    url: https://github.com/mxmlnkn/rapidgzip
  - name: rapidgzip-core
    license: BSD-3-Clause AND MIT
    url: https://github.com/COMBINE-lab/rapidgzip-rust
  - name: zlib-rs
    license: Zlib
    url: https://github.com/trifectatechfoundation/zlib-rs
  - name: libz-rs-sys
    license: Zlib
    url: https://github.com/trifectatechfoundation/zlib-rs
  - name: crossbeam-deque
    license: MIT OR Apache-2.0
    url: https://github.com/crossbeam-rs/crossbeam
  - name: crossbeam-epoch
    license: MIT OR Apache-2.0
    url: https://github.com/crossbeam-rs/crossbeam
  - name: crossbeam-utils
    license: MIT OR Apache-2.0
    url: https://github.com/crossbeam-rs/crossbeam
  - name: clap
    license: MIT OR Apache-2.0
    url: https://github.com/clap-rs/clap
  - name: clap_builder
    license: MIT OR Apache-2.0
    url: https://github.com/clap-rs/clap
  - name: clap_derive
    license: MIT OR Apache-2.0
    url: https://github.com/clap-rs/clap
  - name: clap_lex
    license: MIT OR Apache-2.0
    url: https://github.com/clap-rs/clap
  - name: anstream
    license: MIT OR Apache-2.0
    url: https://github.com/rust-cli/anstyle
  - name: anstyle
    license: MIT OR Apache-2.0
    url: https://github.com/rust-cli/anstyle
  - name: anstyle-parse
    license: MIT OR Apache-2.0
    url: https://github.com/rust-cli/anstyle
  - name: anstyle-query
    license: MIT OR Apache-2.0
    url: https://github.com/rust-cli/anstyle
  - name: colorchoice
    license: MIT OR Apache-2.0
    url: https://github.com/rust-cli/anstyle
  - name: is_terminal_polyfill
    license: MIT OR Apache-2.0
    url: https://github.com/polyfill-rs/is_terminal_polyfill
  - name: utf8parse
    license: Apache-2.0 OR MIT
    url: https://github.com/alacritty/vte
  - name: strsim
    license: MIT
    url: https://github.com/rapidfuzz/strsim-rs
  - name: heck
    license: MIT OR Apache-2.0
    url: https://github.com/withoutboats/heck
  - name: proc-macro2
    license: MIT OR Apache-2.0
    url: https://github.com/dtolnay/proc-macro2
  - name: quote
    license: MIT OR Apache-2.0
    url: https://github.com/dtolnay/quote
  - name: syn
    license: MIT OR Apache-2.0
    url: https://github.com/dtolnay/syn
  - name: unicode-ident
    license: (MIT OR Apache-2.0) AND Unicode-3.0
    url: https://github.com/dtolnay/unicode-ident
";

#[cfg(test)]
mod tests {
    use super::{DEPENDENCY_NAMES, PLAIN, YAML};

    #[test]
    fn every_declared_dependency_is_present_in_attribution_output() {
        for name in DEPENDENCY_NAMES {
            assert!(
                PLAIN.contains(name) || YAML.contains(&format!("name: {name}")),
                "missing attribution for {name}",
            );
        }
    }
}
