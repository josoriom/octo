use clap::{ArgAction, ArgGroup, Args, Parser, Subcommand};
use serde::Serialize;
use std::{
    fs,
    path::{Path, PathBuf},
};

use octo::{
    b64::{decode, encode},
    mzml::{bin_to_mzml::bin_to_mzml, parse_mzml::parse_mzml, structs::*},
};

const VERSION: &str = "0.0.0";

const HELP_TXT: &str = r#"octo v0.0.0

USAGE:
  octo -h | --help
  octo -v | --version

  octo convert (--mzml-to-b64 | --mzml-to-b32 | --b64-to-mzml) [--input-path DIR] [--output-path DIR] [--level 0..22]
  octo cat --file-path PATH

CAT FLAGS:
  --file-path PATH     input file (.b64/.b32), prints full parsed JSON

CONVERT FLAGS:
  --mzml-to-b64        .mzML -> .b64
  --mzml-to-b32        .mzML -> .b32
  --b64-to-mzml        .b64/.b32 -> .mzML
  --input-path DIR     default: crates/parser/data/mzml
  --output-path DIR    default: crates/parser/data/b64
  --level 0..22        default: 12
  --overwrite          default: false (skip if output already exists)

EXAMPLES:
  octo convert --mzml-to-b64 --input-path crates/parser/data/mzml --output-path crates/parser/data/b64
  octo convert --b64-to-mzml --input-path crates/parser/data/b64 --output-path crates/parser/data/mzml_out

  octo cat --file-path crates/parser/data/b64/tiny.msdata.mzML0.99.9.b64
"#;

#[derive(Parser)]
#[command(
    name = "b",
    about = "octo CLI",
    version = VERSION,
    disable_help_flag = true,
    disable_version_flag = true,
    disable_help_subcommand = true
)]
struct Cli {
    #[arg(short = 'h', long = "help", action = ArgAction::SetTrue)]
    help: bool,

    #[arg(short = 'v', long = "version", action = ArgAction::SetTrue)]
    version: bool,

    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    Convert(ConvertArgs),
    Cat(CatArgs),
}

#[derive(Args)]
#[command(
    group(
        ArgGroup::new("convert_mode")
            .args(["mzml_to_b64", "mzml_to_b32", "b64_to_mzml"])
            .required(true)
            .multiple(false)
    )
)]
struct ConvertArgs {
    #[arg(long)]
    input_path: Option<PathBuf>,

    #[arg(long)]
    output_path: Option<PathBuf>,

    #[arg(long = "level", default_value_t = 12, value_parser = clap::value_parser!(u8).range(0..=22))]
    compression_level: u8,

    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    overwrite: bool,

    #[command(flatten)]
    which: ConvertWhich,
}

#[derive(Args)]
struct ConvertWhich {
    #[arg(long = "mzml-to-b64")]
    mzml_to_b64: bool,

    #[arg(long = "mzml-to-b32")]
    mzml_to_b32: bool,

    #[arg(long = "b64-to-mzml")]
    b64_to_mzml: bool,
}

#[derive(Args)]
struct CatArgs {
    #[arg(long = "file-path")]
    file_path: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.version {
        println!("{VERSION}");
        return Ok(());
    }

    if cli.help || cli.cmd.is_none() {
        print!("{HELP_TXT}");
        return Ok(());
    }

    match cli.cmd.unwrap() {
        Cmd::Convert(cmd) => convert(cmd).map_err(|e| e.into()),
        Cmd::Cat(cmd) => cat(cmd).map_err(|e| e.into()),
    }
}

fn print_json_full<T: Serialize>(v: &T) -> Result<(), String> {
    let s = serde_json::to_string_pretty(v).map_err(|e| format!("json failed: {e}"))?;
    println!("{s}");
    Ok(())
}

fn cat(cmd: CatArgs) -> Result<(), String> {
    let cwd = std::env::current_dir().map_err(|e| format!("get current dir failed: {e}"))?;
    let file_path = resolve_user_path(&cwd, &cmd.file_path);

    let mzml = read_mzml_or_b64(&file_path)?;
    print_json_full(&mzml)
}

fn workspace_root() -> PathBuf {
    let here = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    here.ancestors()
        .nth(2)
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn file_ext_lower(path: &Path) -> String {
    path.extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase()
}

fn out_name_for_mzml_file(path: &Path, out_ext: &str) -> Option<String> {
    if file_ext_lower(path) != "mzml" {
        return None;
    }
    let stem = path.file_stem()?.to_string_lossy();
    Some(format!("{stem}.{out_ext}"))
}

fn out_name_for_bin_file_as_mzml(path: &Path) -> Option<String> {
    let ext = file_ext_lower(path);
    if ext != "b64" && ext != "b32" {
        return None;
    }
    let stem = path.file_stem()?.to_string_lossy();
    Some(format!("{stem}.mzML"))
}

fn read_mzml_or_b64(file_path: &Path) -> Result<MzML, String> {
    let bytes = fs::read(file_path).map_err(|e| format!("read failed: {e}"))?;
    let ext = file_ext_lower(file_path);

    if ext == "b64" || ext == "b32" {
        return decode(&bytes).map_err(|e| format!("decode failed: {e}"));
    }
    if ext == "mzml" {
        return parse_mzml(&bytes, false).map_err(|e| format!("parse_mzml failed: {e}"));
    }

    Err(format!(
        "unsupported file extension: {ext:?} (expected .mzML or .b64/.b32)"
    ))
}

fn collect_files_with_exts(input_root: &Path, exts: &[&str]) -> Result<Vec<PathBuf>, String> {
    let mut out = Vec::new();
    let mut stack = vec![input_root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let entries = fs::read_dir(&dir).map_err(|e| format!("read dir failed: {e}"))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("read dir entry failed: {e}"))?;
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if !p.is_file() {
                continue;
            }
            let ext = file_ext_lower(&p);
            let mut ok = false;
            for want in exts {
                if ext == *want {
                    ok = true;
                    break;
                }
            }
            if ok {
                out.push(p);
            }
        }
    }

    out.sort();
    Ok(out)
}

fn convert(cmd: ConvertArgs) -> Result<(), String> {
    let workspace = workspace_root();
    let cwd = std::env::current_dir().map_err(|e| format!("get current dir failed: {e}"))?;

    let input_root = match cmd.input_path.as_deref() {
        Some(p) => resolve_user_path(&cwd, p),
        None => workspace.join("crates/parser/data/mzml"),
    };

    let output_root = match cmd.output_path.as_deref() {
        Some(p) => resolve_user_path(&cwd, p),
        None => workspace.join("crates/parser/data/b64"),
    };

    fs::create_dir_all(&output_root).map_err(|e| format!("create output dir failed: {e}"))?;

    const MB: f64 = 1024.0 * 1024.0;

    if cmd.which.mzml_to_b64 || cmd.which.mzml_to_b32 {
        let out_ext = if cmd.which.mzml_to_b32 { "b32" } else { "b64" };
        let f32_compress = cmd.which.mzml_to_b32;

        let files = collect_files_with_exts(&input_root, &["mzml"])?;
        if files.is_empty() {
            return Err(format!(
                "no .mzML files found under {}",
                input_root.display()
            ));
        }

        let mut ok = 0u32;
        let mut failed = 0u32;
        let mut skipped = 0u32;

        let total = files.len();
        for (i, in_path) in files.into_iter().enumerate() {
            let idx = i + 1;

            let rel = match in_path.strip_prefix(&input_root) {
                Ok(v) => v,
                Err(_) => {
                    eprintln!("{}: cannot make relative path", in_path.display());
                    failed += 1;
                    continue;
                }
            };

            let out_name = match out_name_for_mzml_file(&in_path, out_ext) {
                Some(v) => v,
                None => continue,
            };

            let parent_rel = rel.parent().unwrap_or_else(|| Path::new(""));
            let out_dir = output_root.join(parent_rel);
            let out_path = out_dir.join(out_name);

            if !cmd.overwrite {
                if let Ok(m) = fs::metadata(&out_path) {
                    if m.is_file() && m.len() > 0 {
                        let in_mb = fs::metadata(&in_path)
                            .map(|m| m.len() as f64 / MB)
                            .unwrap_or(0.0);
                        let out_mb = m.len() as f64 / MB;

                        println!(
                            "[{}/{}] skip: {}  input={:.2} MB, output={:.2} MB",
                            idx,
                            total,
                            out_path.display(),
                            in_mb,
                            out_mb
                        );

                        skipped += 1;
                        continue;
                    }
                }
            }

            if let Err(e) = fs::create_dir_all(&out_dir) {
                eprintln!("{}: create output dir failed: {e}", out_dir.display());
                failed += 1;
                continue;
            }

            let bytes = match fs::read(&in_path) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{}: read failed: {e}", in_path.display());
                    failed += 1;
                    continue;
                }
            };

            let mzml = match parse_mzml(&bytes, false) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{}: parse_mzml failed: {e}", in_path.display());
                    failed += 1;
                    continue;
                }
            };

            let encoded = encode(&mzml, cmd.compression_level, f32_compress);

            let in_mb = bytes.len() as f64 / MB;
            let out_mb = encoded.len() as f64 / MB;

            println!(
                "[{}/{}] output: {}  input={:.2} MB, output={:.2} MB",
                idx,
                total,
                out_path.display(),
                in_mb,
                out_mb
            );

            if let Err(e) = fs::write(&out_path, encoded) {
                eprintln!("{}: write failed: {e}", out_path.display());
                failed += 1;
                continue;
            }

            ok += 1;
        }

        println!("converted_ok={ok} converted_failed={failed} converted_skipped={skipped}");
        if failed != 0 {
            return Err("some files failed".to_string());
        }
        return Ok(());
    }

    if cmd.which.b64_to_mzml {
        let files = collect_files_with_exts(&input_root, &["b64", "b32"])?;
        if files.is_empty() {
            return Err(format!(
                "no .b64/.b32 files found under {}",
                input_root.display()
            ));
        }

        let mut ok = 0u32;
        let mut failed = 0u32;
        let mut skipped = 0u32;

        let total = files.len();
        for (i, in_path) in files.into_iter().enumerate() {
            let idx = i + 1;

            let rel = match in_path.strip_prefix(&input_root) {
                Ok(v) => v,
                Err(_) => {
                    eprintln!("{}: cannot make relative path", in_path.display());
                    failed += 1;
                    continue;
                }
            };

            let out_name = match out_name_for_bin_file_as_mzml(&in_path) {
                Some(v) => v,
                None => continue,
            };

            let parent_rel = rel.parent().unwrap_or_else(|| Path::new(""));
            let out_dir = output_root.join(parent_rel);
            let out_path = out_dir.join(out_name);

            if !cmd.overwrite {
                if let Ok(m) = fs::metadata(&out_path) {
                    if m.is_file() && m.len() > 0 {
                        let in_mb = fs::metadata(&in_path)
                            .map(|m| m.len() as f64 / MB)
                            .unwrap_or(0.0);
                        let out_mb = m.len() as f64 / MB;

                        println!(
                            "[{}/{}] skip: {}  input={:.2} MB, output={:.2} MB",
                            idx,
                            total,
                            out_path.display(),
                            in_mb,
                            out_mb
                        );

                        skipped += 1;
                        continue;
                    }
                }
            }

            if let Err(e) = fs::create_dir_all(&out_dir) {
                eprintln!("{}: create output dir failed: {e}", out_dir.display());
                failed += 1;
                continue;
            }

            let in_bytes = match fs::read(&in_path) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{}: read failed: {e}", in_path.display());
                    failed += 1;
                    continue;
                }
            };

            let mzml = match read_mzml_or_b64_from_bytes(&in_path, &in_bytes) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{}: {e}", in_path.display());
                    failed += 1;
                    continue;
                }
            };

            let xml = match bin_to_mzml(&mzml) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{}: bin_to_mzml failed: {e}", in_path.display());
                    failed += 1;
                    continue;
                }
            };

            let in_mb = in_bytes.len() as f64 / MB;
            let out_mb = xml.len() as f64 / MB;

            println!(
                "[{}/{}] output: {}  input={:.2} MB, output={:.2} MB",
                idx,
                total,
                out_path.display(),
                in_mb,
                out_mb
            );

            if let Err(e) = fs::write(&out_path, xml.as_bytes()) {
                eprintln!("{}: write failed: {e}", out_path.display());
                failed += 1;
                continue;
            }

            ok += 1;
        }

        println!("converted_ok={ok} converted_failed={failed} converted_skipped={skipped}");
        if failed != 0 {
            return Err("some files failed".to_string());
        }
        return Ok(());
    }

    Err("no convert mode selected".to_string())
}

fn read_mzml_or_b64_from_bytes(file_path: &Path, bytes: &[u8]) -> Result<MzML, String> {
    let ext = file_ext_lower(file_path);

    if ext == "b64" || ext == "b32" {
        return decode(bytes).map_err(|e| format!("decode failed: {e}"));
    }
    if ext == "mzml" {
        return parse_mzml(bytes, false).map_err(|e| format!("parse_mzml failed: {e}"));
    }

    Err(format!(
        "unsupported file extension: {ext:?} (expected .mzML or .b64/.b32)"
    ))
}

fn resolve_user_path(cwd: &Path, p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        cwd.join(p)
    }
}
