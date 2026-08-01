#!/usr/bin/env bash
# Generate small deterministic synthetic corpora for fair CI-friendly benchmarks.
#
# Usage:
#   benchmarks/gen-corpora.sh [OUT_DIR]
#
# Environment (sizes are *uncompressed* payload sizes unless noted):
#   OUT_DIR              Output directory (default: target/bench-corpora)
#   CORPUS_BYTES         Single-member + multi-member total payload (default: 4 MiB)
#   MEMBER_COUNT         Ordinary concatenated gzip members (default: 4)
#   BGZF_BLOCK_UNCOMP    Uncompressed bytes per BGZF-like block (default: 65280)
#   SEED                 Deterministic PRNG seed string (default: rapidgzip-rust-bench-v1)
#   SKIP_BGZF=1          Skip BGZF-like multi-block corpus
#   SKIP_ZLIB=1          Skip zlib-wrapped stream
#   LARGE_BYTES          If set (>0), also emit a larger single-member gzip of this size
#
# Outputs (when generated):
#   single-member.gz     One gzip member, compressible text
#   multi-member.gz      MEMBER_COUNT concatenated gzip members
#   bgzf-like.gz         Multi-member gzip with fixed-size members (BGZF-shaped)
#   zlib-stream.zz       zlib-wrapped DEFLATE (RFC 1950), single stream
#   corpora.meta.json        Sizes / digests for harnesses
#   (optional) large-single.gz when LARGE_BYTES is set
#
# No network access. Deterministic given SEED and size envs.
set -euo pipefail

out_dir=${1:-${OUT_DIR:-target/bench-corpora}}
corpus_bytes=${CORPUS_BYTES:-$((4 * 1024 * 1024))}
member_count=${MEMBER_COUNT:-4}
bgzf_block=${BGZF_BLOCK_UNCOMP:-65280}
seed=${SEED:-rapidgzip-rust-bench-v1}
large_bytes=${LARGE_BYTES:-0}

mkdir -p "$out_dir"

python3 - "$out_dir" "$corpus_bytes" "$member_count" "$bgzf_block" "$seed" "$large_bytes" \
    "${SKIP_BGZF:-0}" "${SKIP_ZLIB:-0}" <<'PY'
import gzip
import hashlib
import json
import os
import struct
import sys
import zlib
from pathlib import Path

out_dir = Path(sys.argv[1])
corpus_bytes = int(sys.argv[2])
member_count = max(1, int(sys.argv[3]))
bgzf_block = max(1024, int(sys.argv[4]))
seed = sys.argv[5].encode()
large_bytes = int(sys.argv[6])
skip_bgzf = sys.argv[7] == "1"
skip_zlib = sys.argv[8] == "1"

out_dir.mkdir(parents=True, exist_ok=True)


def prng_bytes(n: int, salt: bytes) -> bytes:
    """Deterministic expandable byte stream from SHA-256 counter mode."""
    out = bytearray()
    counter = 0
    while len(out) < n:
        block = hashlib.sha256(seed + salt + counter.to_bytes(8, "little")).digest()
        out.extend(block)
        counter += 1
    return bytes(out[:n])


def compressible_text(n: int, salt: bytes) -> bytes:
    """Mostly-compressible ASCII resembling FASTQ/text (high gzip ratio)."""
    # Mix a repeating base64-like alphabet with short pseudo-random lines.
    alphabet = b"ACGTNacgtn0123456789_."
    rnd = prng_bytes(n + 64, salt)
    out = bytearray()
    i = 0
    line_no = 0
    while len(out) < n:
        # FASTQ-ish header every 4 lines for structure.
        if line_no % 4 == 0:
            hdr = f"@synth.read.{line_no // 4} len=64\n".encode()
            out.extend(hdr)
        elif line_no % 4 == 2:
            out.extend(b"+\n")
        else:
            # 64-char sequence/quality-like line.
            chunk = bytearray(65)
            for j in range(64):
                chunk[j] = alphabet[rnd[i % len(rnd)] % len(alphabet)]
                i += 1
            chunk[64] = ord("\n")
            out.extend(chunk)
        line_no += 1
    return bytes(out[:n])


def gzip_member(data: bytes, mtime: int = 0, name: bytes = b"") -> bytes:
    """Single gzip member with fixed mtime for determinism (OS=255)."""
    # Use gzip.compress then rewrite mtime/OS for stability across platforms.
    raw = gzip.compress(data, compresslevel=6, mtime=mtime)
    # gzip.compress produces: 1f8b 08 flags mtime[4] xfl os ...
    ba = bytearray(raw)
    if len(ba) >= 10:
        struct.pack_into("<I", ba, 4, mtime)
        ba[9] = 255  # unknown OS
    return bytes(ba)


def write_file(path: Path, data: bytes) -> dict:
    path.write_bytes(data)
    return {
        "path": path.name,
        "compressed_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


meta = {
    "seed": seed.decode(),
    "corpus_bytes": corpus_bytes,
    "member_count": member_count,
    "bgzf_block_uncomp": bgzf_block,
    "files": {},
}

# --- single-member gzip ---
payload = compressible_text(corpus_bytes, b"single")
single = gzip_member(payload, mtime=1)
info = write_file(out_dir / "single-member.gz", single)
info["uncompressed_bytes"] = len(payload)
info["members"] = 1
info["kind"] = "gzip-single"
meta["files"]["single-member.gz"] = info
print(f"wrote {out_dir / 'single-member.gz'} ({info['compressed_bytes']} B compressed, {len(payload)} B raw)")

# --- multi-member ordinary gzip ---
# Split payload into member_count nearly equal slices.
parts = []
base = corpus_bytes // member_count
rem = corpus_bytes % member_count
offset = 0
for mi in range(member_count):
    size = base + (1 if mi < rem else 0)
    part = compressible_text(size, f"member-{mi}".encode())
    parts.append(gzip_member(part, mtime=mi + 1))
    offset += size
multi = b"".join(parts)
info = write_file(out_dir / "multi-member.gz", multi)
info["uncompressed_bytes"] = corpus_bytes
info["members"] = member_count
info["kind"] = "gzip-multi"
meta["files"]["multi-member.gz"] = info
print(f"wrote {out_dir / 'multi-member.gz'} ({info['compressed_bytes']} B, {member_count} members)")

# --- BGZF-like multi-block (fixed uncompressed member size; ordinary gzip members) ---
# True BGZF has extra subfields; for decoder stress we emit concatenated gzip
# members of ~bgzf_block uncompressed bytes. If `bgzip` is on PATH we also try
# a real BGZF file for optional comparison.
if not skip_bgzf:
    blocks = []
    remaining = corpus_bytes
    bi = 0
    while remaining > 0:
        take = min(bgzf_block, remaining)
        block_data = compressible_text(take, f"bgzf-{bi}".encode())
        blocks.append(gzip_member(block_data, mtime=(bi % 1000) + 1))
        remaining -= take
        bi += 1
    bgzf_like = b"".join(blocks)
    info = write_file(out_dir / "bgzf-like.gz", bgzf_like)
    info["uncompressed_bytes"] = corpus_bytes
    info["members"] = bi
    info["kind"] = "gzip-bgzf-like"
    info["block_uncomp"] = bgzf_block
    meta["files"]["bgzf-like.gz"] = info
    print(f"wrote {out_dir / 'bgzf-like.gz'} ({info['compressed_bytes']} B, {bi} blocks)")

    # Optional real BGZF via bgzip if present (not required).
    bgzip = None
    for cand in ("bgzip",):
        from shutil import which
        bgzip = which(cand)
        if bgzip:
            break
    if bgzip:
        raw_path = out_dir / "bgzf-real.tmp"
        raw_path.write_bytes(payload)
        bgz_path = out_dir / "bgzf-real.bgz"
        # bgzip -c < raw > bgz
        import subprocess
        with open(bgz_path, "wb") as outfh:
            subprocess.run([bgzip, "-c", str(raw_path)], check=True, stdout=outfh)
        raw_path.unlink(missing_ok=True)
        info = write_file(bgz_path, bgz_path.read_bytes())
        info["uncompressed_bytes"] = len(payload)
        info["kind"] = "bgzf-real"
        meta["files"]["bgzf-real.bgz"] = info
        print(f"wrote {bgz_path} via bgzip ({info['compressed_bytes']} B)")

# --- zlib stream ---
if not skip_zlib:
    zpayload = compressible_text(corpus_bytes, b"zlib")
    # zlib.compress uses wbits=15 (zlib wrapper).
    zdata = zlib.compress(zpayload, level=6)
    info = write_file(out_dir / "zlib-stream.zz", zdata)
    info["uncompressed_bytes"] = len(zpayload)
    info["members"] = 1
    info["kind"] = "zlib"
    meta["files"]["zlib-stream.zz"] = info
    print(f"wrote {out_dir / 'zlib-stream.zz'} ({info['compressed_bytes']} B zlib)")

# --- optional large single-member ---
if large_bytes > 0:
    lpayload = compressible_text(large_bytes, b"large")
    lgz = gzip_member(lpayload, mtime=2)
    info = write_file(out_dir / "large-single.gz", lgz)
    info["uncompressed_bytes"] = len(lpayload)
    info["members"] = 1
    info["kind"] = "gzip-single-large"
    meta["files"]["large-single.gz"] = info
    print(f"wrote {out_dir / 'large-single.gz'} ({info['compressed_bytes']} B, {large_bytes} B raw)")

meta_path = out_dir / "single-member.meta.json"
# Store full meta under a stable name used by harnesses.
meta_path = out_dir / "corpora.meta.json"
meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
print(f"wrote {meta_path}")
PY
