# CLI — Commands & Flags (with examples)

This CLI has **two subcommands**:

- `convert` — batch convert folders (recursive)
- `cat` — print JSON metadata for a single file (`.mzML`, `.b64`, or `.b32`)

Examples below use:

- `tiny.msdata.mzML0.99.9.mzML`
- `tiny.msdata.mzML0.99.9.b64`

---

## 1) `octo cat`

Prints JSON to stdout for a **single file**.

### Flag

- `--file-path <PATH>`
  - Path to the file you want to inspect.
  - Supported file types: `.b64`, `.b32`.

### Example:

```bash
octo cat --file-path crates/parser/data/b64/tiny.msdata.mzML0.99.9.b64
```

---

## 2) `octo convert`

Batch converts files under a folder **recursively**, preserving the relative folder structure under `--output-path`.

### Common flags

- `--input-path <DIR>`
  - Directory to scan recursively (required)

- `--output-path <DIR>`
  - Output directory root (subfolders are created) (required)

- `--level <0..22>`
  - Default: `12`
  - Compression level (only relevant for mzML → b64/b32)

- `--overwrite`
  - If set, overwrites output files even if they already exist.
  - If not set, existing non-empty outputs are skipped.

### Choose exactly one conversion mode (required)

- `--mzml-to-b64`
  - Converts `.mzML` → `.b64`
  - Uses `encode(mzml, level, f32_compress=false)`

- `--mzml-to-b32`
  - Converts `.mzML` → `.b32`
  - Uses `encode(mzml, level, f32_compress=true)`

- `--b64-to-mzml`
  - Converts `.b64` and `.b32` → `.mzML`
  - Uses `decode(...)` into `MzML`, then `bin_to_mzml(&mzml)` to produce mzML XML

### Filtering flags (optional)

You may pass **at most one** of the following:

- `--pattern <STR>`
  - Unicode case-insensitive substring match.

- `--pattern-exact <STR>`
  - Case-sensitive substring match.

- `--regex <REGEX>`
  - Regex match against the filename.

---

### 2.1) Convert `.mzML` → `.b64/.b32`

Defaults:

- `--level 12`

Run (defaults):

```bash
octo convert --mzml-to-b64 --input-path crates/parser/data/mzml --output-path crates/parser/data/b64
```

Custom compression level:

```bash
octo convert --mzml-to-b64 --input-path crates/parser/data/mzml --output-path crates/parser/data/b64 --level 12
```

---

### 2.2) Convert `.b64/.b32` → `.mzML`

### Example:

```bash
octo convert --b64-to-mzml --input-path crates/parser/data/b64 --output-path crates/parser/data/mzml_out
```

(If the input tree contains `.b32` too, it is also converted by this mode.)

---
