# Fair benchmark snapshot (2026-08-01)

Host: local ThinkPad; rapidgzip C++ **0.15.2** (venv); rapidgzip-rust **0.1.0** release.
Corpora: synthetic ~4 MiB uncompressed each (single-member, multi-member, bgzf-like).
Method: verify mode (`-t` / `-t --verify`), warmups=2, runs=5, median wall time → MiB/s.

## bgzf-like.gz (verify)

| tool | threads | median MiB/s | median RSS MiB |
|------|--------:|-------------:|---------------:|
| rapidgzip-cpp-zlib-ng | 1 | 86.5 | 20.75 |
| rapidgzip-cpp-zlib-ng | 4 | 84.6 | 25.41 |
| rapidgzip-cpp-zlib-ng | 8 | 100.7 | 25.84 |
| rapidgzip-cpp-zlib-ng | 16 | 93.8 | 26.04 |
| rapidgzip-rust | 1 | 224.0 | 4.33 |
| rapidgzip-rust | 4 | 381.2 | 5.47 |
| rapidgzip-rust | 8 | 416.1 | 5.44 |
| rapidgzip-rust | 16 | 387.0 | 8.01 |

## multi-member.gz (verify)

| tool | threads | median MiB/s | median RSS MiB |
|------|--------:|-------------:|---------------:|
| rapidgzip-cpp-zlib-ng | 1 | 81.2 | 21.23 |
| rapidgzip-cpp-zlib-ng | 4 | 90.6 | 25.72 |
| rapidgzip-cpp-zlib-ng | 8 | 92.7 | 26.27 |
| rapidgzip-cpp-zlib-ng | 16 | 84.9 | 26.31 |
| rapidgzip-rust | 1 | 218.6 | 5.39 |
| rapidgzip-rust | 4 | 368.4 | 7.98 |
| rapidgzip-rust | 8 | 344.1 | 7.98 |
| rapidgzip-rust | 16 | 351.4 | 8.05 |

## single-member.gz (verify)

| tool | threads | median MiB/s | median RSS MiB |
|------|--------:|-------------:|---------------:|
| rapidgzip-cpp-zlib-ng | 1 | 83.3 | 21.09 |
| rapidgzip-cpp-zlib-ng | 4 | 81.3 | 27.32 |
| rapidgzip-cpp-zlib-ng | 8 | 83.7 | 26.96 |
| rapidgzip-cpp-zlib-ng | 16 | 82.2 | 27.00 |
| rapidgzip-rust | 1 | 222.7 | 5.96 |
| rapidgzip-rust | 4 | 146.0 | 18.50 |
| rapidgzip-rust | 8 | 158.1 | 19.19 |
| rapidgzip-rust | 16 | 133.9 | 18.65 |

## Notes

- C++ binary is PyPI rapidgzip 0.15.2 (not a separate ISA-L build); labeled zlib-ng path in harness.
- Synthetic corpora are small; absolute rates favor low-overhead tools. Use public FASTQ for official parity gates.
- Rust peak RSS is much lower on these cells (no large C++ runtime footprint).