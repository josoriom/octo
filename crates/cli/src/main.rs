use std::{
    fs,
    io::{Read, Seek, SeekFrom, Write, stderr, stdout},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
    time::Instant,
};

use clap::{
    ArgAction, ArgGroup, Args, ColorChoice, CommandFactory, FromArgMatches, Parser, Subcommand,
    builder::styling::{AnsiColor, Color, Style, Styles},
};
use rayon::{ThreadPoolBuilder, prelude::*};
use mimalloc::MiMalloc;
use regex::Regex;
use serde::Serialize;

use octo::{
    b64::{decoder::decode, encoder::encode::encode, FileEncoderOutput}, mzml::{bin_to_mzml::bin_to_mzml, parse_mzml::parse_mzml, structs::*}
};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const VERSION: &str = "0.0.0";
const FILE_TRAILER: [u8; 8] = *b"END\0\0\0\0\0";

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_GREEN: &str = "\x1b[1;32m";
const ANSI_YELLOW: &str = "\x1b[1;33m";
const ANSI_RED: &str = "\x1b[1;31m";
const ANSI_BLUE: &str = "\x1b[1;34m";

const AFTER_HELP: &str = "
\x1b[1;33mQUICK REFERENCE\x1b[0m (full flags are in `octo convert --help` / `octo cat --help`)

\x1b[1;32mUSAGE:\x1b[0m
  \x1b[96mocto convert\x1b[0m [--mzml-to-b64 | --mzml-to-b32 | --b64-to-mzml]
               -i, --input-path DIR
               -o, --output-path DIR

  \x1b[96mocto cat\x1b[0m PATH

\x1b[1;32mOPTIONS:\x1b[0m
  \x1b[96m-h\x1b[0m, \x1b[96m--help\x1b[0m
  \x1b[96m-v\x1b[0m, \x1b[96m--version\x1b[0m

\x1b[1;32mEXAMPLES:\x1b[0m
  \x1b[96mocto convert\x1b[0m -i crates/parser/data/mzml -o crates/parser/data/b64
  \x1b[96mocto convert\x1b[0m --b64-to-mzml -i crates/parser/data/b64 -o crates/parser/data/mzml_out
  \x1b[96mocto cat\x1b[0m crates/parser/data/b64/tiny.msdata.mzML0.99.9.b64
";

fn cli_styles() -> Styles {
    Styles::styled().literal(Style::new().fg_color(Some(Color::Ansi(AnsiColor::Cyan))))
}

#[derive(Parser)]
#[command(
    name = "octo",
    version = VERSION,
    arg_required_else_help = true,
    disable_help_subcommand = true,
    disable_version_flag = true
)]
struct Cli {
    #[arg(short = 'v', long = "version", action = ArgAction::SetTrue, global = true)]
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
            .multiple(false)
    ),
    group(
        ArgGroup::new("pattern_mode")
            .args(["pattern", "pattern_exact", "regex"])
            .multiple(false)
    )
)]
struct ConvertArgs {
    #[arg(short = 'i', long = "input-path", required = true)]
    input_path: PathBuf,

    #[arg(short = 'o', long = "output-path", required = true)]
    output_path: PathBuf,

    #[arg(
        long = "level",
        default_value_t = 12,
        value_parser = clap::value_parser!(u8).range(0..=22)
    )]
    compression_level: u8,

    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    overwrite: bool,

    #[arg(long = "pattern")]
    pattern: Option<String>,

    #[arg(long = "pattern-exact")]
    pattern_exact: Option<String>,

    #[arg(long = "regex")]
    regex: Option<String>,

    #[arg(
        long = "cores",
        default_value_t = 1u16,
        value_parser = clap::value_parser!(u16).range(1..=1024)
    )]
    cores: u16,

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
    #[arg(value_name = "PATH")]
    file_path: PathBuf,

    #[arg(long = "full", short = 'f', action = ArgAction::SetTrue, default_value_t = false)]
    full: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Cli::command();
    cmd = cmd
        .styles(cli_styles())
        .color(ColorChoice::Auto)
        .after_help(AFTER_HELP);

    let matches = cmd.get_matches();
    let cli = Cli::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());

    if cli.version {
        println!("{VERSION}");
        return Ok(());
    }

    match cli.cmd {
        Some(Cmd::Convert(cmd)) => convert(cmd).map_err(|e| e.into()),
        Some(Cmd::Cat(cmd)) => cat(cmd).map_err(|e| e.into()),
        None => Ok(()),
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
    let mut mzml = read_mzml_or_b64(&file_path)?;
    if !cmd.full {
        trim_mzml_for_cat(&mut mzml);
    }
    print_json_full(&mzml)
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
        return parse_mzml(&bytes).map_err(|e| format!("parse_mzml failed: {e}"));
    }

    Err(format!(
        "unsupported file extension: {ext:?} (expected .mzML or .b64/.b32)"
    ))
}

#[derive(Debug, Clone)]
enum FilterExpr {
    Leaf(String),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
}

impl FilterExpr {
    fn matches(&self, name_lower: &str) -> bool {
        match self {
            Self::Leaf(pat) => name_lower.contains(pat),
            Self::And(exprs) => exprs.iter().all(|e| e.matches(name_lower)),
            Self::Or(exprs) => exprs.iter().any(|e| e.matches(name_lower)),
        }
    }

    fn parse(input: &str) -> Result<Self, String> {
        let input = input.trim();
        if input.is_empty() {
            return Err("Filter pattern cannot be empty".to_string());
        }
        let or_parts: Vec<&str> = input.split('|').collect();
        if or_parts.len() > 1 {
            let exprs: Result<Vec<Self>, String> =
                or_parts.into_iter().map(Self::parse_and).collect();
            return Ok(Self::Or(exprs?));
        }
        Self::parse_and(input)
    }

    fn parse_and(input: &str) -> Result<Self, String> {
        let and_parts: Vec<&str> = input.split('&').collect();
        if and_parts.len() > 1 {
            let exprs: Vec<Self> = and_parts
                .into_iter()
                .map(|s| Self::Leaf(s.trim().to_lowercase()))
                .collect();
            return Ok(Self::And(exprs));
        }
        Ok(Self::Leaf(input.trim().to_lowercase()))
    }
}

fn build_name_filter(
    pattern: Option<&str>,
    pattern_exact: Option<&str>,
    regex: Option<&str>,
) -> Result<Option<Box<dyn Fn(&str) -> bool>>, String> {
    let tree = if let Some(p) = pattern {
        Some(FilterExpr::parse(p)?)
    } else {
        None
    };

    let exact = pattern_exact.map(|s| s.to_string());

    let re = if let Some(r) = regex {
        Some(Regex::new(r).map_err(|e| format!("invalid regex: {e}"))?)
    } else {
        None
    };

    if tree.is_none() && exact.is_none() && re.is_none() {
        return Ok(None);
    }

    Ok(Some(Box::new(move |name: &str| {
        if let Some(needle) = &exact {
            if name.contains(needle) {
                return true;
            }
        }

        if let Some(r) = &re {
            if r.is_match(name) {
                return true;
            }
        }

        if let Some(t) = &tree {
            let name_lower = name.to_lowercase();
            if t.matches(&name_lower) {
                return true;
            }
        }

        false
    })))
}

fn collect_files_with_exts(
    input_root: &Path,
    exts: &[&str],
    name_filter: Option<&dyn Fn(&str) -> bool>,
) -> Result<Vec<PathBuf>, String> {
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
            if !exts.iter().any(|want| ext == *want) {
                continue;
            }
            if let Some(f) = name_filter {
                let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if !f(name) {
                    continue;
                }
            }
            out.push(p);
        }
    }

    out.sort();
    Ok(out)
}

fn convert(cmd: ConvertArgs) -> Result<(), String> {
    let cwd = std::env::current_dir().map_err(|e| format!("get current dir failed: {e}"))?;

    let input_root = resolve_user_path(&cwd, &cmd.input_path);
    let output_root = resolve_user_path(&cwd, &cmd.output_path);

    fs::create_dir_all(&output_root).map_err(|e| format!("create output dir failed: {e}"))?;

    let filter = build_name_filter(
        cmd.pattern.as_deref(),
        cmd.pattern_exact.as_deref(),
        cmd.regex.as_deref(),
    )?;

    const MB: f64 = 1024.0 * 1024.0;

    let cores = cmd.cores as usize;
    if cores == 0 {
        return Err("--cores must be >= 1".to_string());
    }
    let pool = ThreadPoolBuilder::new()
        .num_threads(cores)
        .build()
        .map_err(|e| format!("rayon thread pool init failed: {e}"))?;

    let t_all = Instant::now();

    let default_mzml_to_b64 =
        !cmd.which.mzml_to_b64 && !cmd.which.mzml_to_b32 && !cmd.which.b64_to_mzml;

    let mzml_to_b64 = cmd.which.mzml_to_b64 || default_mzml_to_b64;
    let mzml_to_b32 = cmd.which.mzml_to_b32;
    let b64_to_mzml = cmd.which.b64_to_mzml;

    let print_lock = Arc::new(Mutex::new(()));
    let done = Arc::new(AtomicUsize::new(0));
    let ok = Arc::new(AtomicU32::new(0));
    let failed = Arc::new(AtomicU32::new(0));
    let skipped = Arc::new(AtomicU32::new(0));
    let fixed_bad_total = Arc::new(AtomicU32::new(0));
    let had_failed = Arc::new(AtomicBool::new(false));

    if mzml_to_b64 || mzml_to_b32 {
        let out_ext = if mzml_to_b32 { "b32" } else { "b64" };
        let f32_compress = mzml_to_b32;

        let files = collect_files_with_exts(&input_root, &["mzml"], filter.as_deref())?;
        if files.is_empty() {
            return Err(format!(
                "no matching .mzML files found under {}",
                input_root.display()
            ));
        }

        let total = files.len();

        pool.install(|| {
            files.par_iter().for_each(|in_path| {
                let rel = match in_path.strip_prefix(&input_root) {
                    Ok(v) => v,
                    Err(_) => {
                        had_failed.store(true, Ordering::Relaxed);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        eprintln!(
                            "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: cannot make relative path",
                            n, total, name
                        );
                        let _ = stderr().flush();
                        return;
                    }
                };

                let out_name = match out_name_for_mzml_file(in_path, out_ext) {
                    Some(v) => v,
                    None => {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        println!("{ANSI_YELLOW}[skipped]{ANSI_RESET} [{}/{}] {}", n, total, name);
                        let _ = stdout().flush();
                        return;
                    }
                };

                let parent_rel = rel.parent().unwrap_or_else(|| Path::new(""));
                let out_dir = output_root.join(parent_rel);
                let out_path = out_dir.join(out_name);

                let mut fixed_bad = false;

                if !cmd.overwrite {
                    if let Ok(m) = fs::metadata(&out_path) {
                        if m.is_file() {
                            let out_len = m.len();
                            if out_len > 0 && has_valid_trailer(&out_path, out_len) {
                                skipped.fetch_add(1, Ordering::Relaxed);
                                let n = done.fetch_add(1, Ordering::Relaxed) + 1;

                                let in_mb = fs::metadata(in_path)
                                    .map(|m| m.len() as f64 / MB)
                                    .unwrap_or(0.0);
                                let out_mb = out_len as f64 / MB;

                                let name = basename(&out_path);
                                let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                                println!(
                                    "{ANSI_YELLOW}[skipped]{ANSI_RESET} [{}/{}] {}  input={:.2} MB, output={:.2} MB",
                                    n, total, name, in_mb, out_mb
                                );
                                let _ = stdout().flush();
                                return;
                            } else {
                                fixed_bad = true;
                            }
                        }
                    }
                }

                if let Err(e) = fs::create_dir_all(&out_dir) {
                    had_failed.store(true, Ordering::Relaxed);
                    failed.fetch_add(1, Ordering::Relaxed);
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    let name = basename(&out_dir);
                    let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                    eprintln!(
                        "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: create output dir failed: {e}",
                        n, total, name
                    );
                    let _ = stderr().flush();
                    return;
                }

                let t0 = Instant::now();

                let bytes = match fs::read(in_path) {
                    Ok(v) => v,
                    Err(e) => {
                        had_failed.store(true, Ordering::Relaxed);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        eprintln!(
                            "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: read failed: {e}",
                            n, total, name
                        );
                        let _ = stderr().flush();
                        return;
                    }
                };

                let mzml = match parse_mzml(&bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        had_failed.store(true, Ordering::Relaxed);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        eprintln!(
                            "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: parse_mzml failed: {e}",
                            n, total, name
                        );
                        let _ = stderr().flush();
                        return;
                    }
                };

                let in_mb = bytes.len() as f64 / MB;
                drop(bytes);

                let out_path_str = out_path.to_string_lossy();
                let mut file_output = match FileEncoderOutput::open_for_writing(out_path_str.as_ref()) {
                    Ok(f) => f,
                    Err(e) => {
                        had_failed.store(true, Ordering::Relaxed);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(&out_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        eprintln!(
                            "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: open file failed: {e}",
                            n, total, name
                        );
                        let _ = stderr().flush();
                        return;
                    }
                };

                if let Err(e) = encode(&mzml, cmd.compression_level, f32_compress, octo::encoder::encode::WritingMode::Streaming, &mut file_output) {
                    had_failed.store(true, Ordering::Relaxed);
                    failed.fetch_add(1, Ordering::Relaxed);
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    let name = basename(&out_path);
                    let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                    eprintln!(
                        "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: encode failed: {e}",
                        n, total, name
                    );
                    let _ = stderr().flush();
                    return;
                }

                drop(mzml);
                drop(file_output);

                let out_mb = fs::metadata(&out_path).map(|m| m.len() as f64 / MB).unwrap_or(0.0);

                ok.fetch_add(1, Ordering::Relaxed);
                if fixed_bad {
                    fixed_bad_total.fetch_add(1, Ordering::Relaxed);
                }
                let n = done.fetch_add(1, Ordering::Relaxed) + 1;

                let elapsed_s = t0.elapsed().as_secs_f64();
                
                let (tag, color) = if fixed_bad {
                    ("[fixed]", ANSI_BLUE)
                } else {
                    ("[ok]", ANSI_GREEN)
                };

                let name = basename(&out_path);

                let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                println!(
                    "{color}{tag}{ANSI_RESET} [{}/{}] output: {}  input={:.2} MB, output={:.2} MB, time={:.3}s",
                    n, total, name, in_mb, out_mb, elapsed_s
                );
                let _ = stdout().flush();
            })
        });

        let ok = ok.load(Ordering::Relaxed);
        let failed = failed.load(Ordering::Relaxed);
        let skipped = skipped.load(Ordering::Relaxed);
        let fixed_bad = fixed_bad_total.load(Ordering::Relaxed);

        let d = t_all.elapsed();
        let total_secs = d.as_secs();
        let h = total_secs / 3600;
        let m = (total_secs % 3600) / 60;
        let s = total_secs % 60;

        println!(
            "ok={ok} failed={failed} skipped={skipped} fixed={fixed_bad} total_time={:02}:{:02}:{:02}",
            h, m, s
        );

        if had_failed.load(Ordering::Relaxed) {
            return Err("some files failed".to_string());
        }
        return Ok(());
    }

    if b64_to_mzml {
        let files = collect_files_with_exts(&input_root, &["b64", "b32"], filter.as_deref())?;
        if files.is_empty() {
            return Err(format!(
                "no matching .b64/.b32 files found under {}",
                input_root.display()
            ));
        }

        let total = files.len();

        pool.install(|| {
            files.par_iter().for_each(|in_path| {
                let rel = match in_path.strip_prefix(&input_root) {
                    Ok(v) => v,
                    Err(_) => {
                        had_failed.store(true, Ordering::Relaxed);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        eprintln!(
                            "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: cannot make relative path",
                            n, total, name
                        );
                        let _ = stderr().flush();
                        return;
                    }
                };

                let out_name = match out_name_for_bin_file_as_mzml(in_path) {
                    Some(v) => v,
                    None => {
                        skipped.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        println!("{ANSI_YELLOW}[skipped]{ANSI_RESET} [{}/{}] {}", n, total, name);
                        let _ = stdout().flush();
                        return;
                    }
                };

                let parent_rel = rel.parent().unwrap_or_else(|| Path::new(""));
                let out_dir = output_root.join(parent_rel);
                let out_path = out_dir.join(out_name);

                if !cmd.overwrite {
                    if let Ok(m) = fs::metadata(&out_path) {
                        if m.is_file() && m.len() > 0 {
                            skipped.fetch_add(1, Ordering::Relaxed);
                            let n = done.fetch_add(1, Ordering::Relaxed) + 1;

                            let in_mb = fs::metadata(in_path)
                                .map(|m| m.len() as f64 / MB)
                                .unwrap_or(0.0);
                            let out_mb = m.len() as f64 / MB;

                            let name = basename(&out_path);
                            let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                            println!(
                                "{ANSI_YELLOW}[skipped]{ANSI_RESET} [{}/{}] {}  input={:.2} MB, output={:.2} MB",
                                n, total, name, in_mb, out_mb
                            );
                            let _ = stdout().flush();
                            return;
                        }
                    }
                }

                if let Err(e) = fs::create_dir_all(&out_dir) {
                    had_failed.store(true, Ordering::Relaxed);
                    failed.fetch_add(1, Ordering::Relaxed);
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    let name = basename(&out_dir);
                    let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                    eprintln!(
                        "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: create output dir failed: {e}",
                        n, total, name
                    );
                    let _ = stderr().flush();
                    return;
                }

                let t0 = Instant::now();

                let in_bytes = match fs::read(in_path) {
                    Ok(v) => v,
                    Err(e) => {
                        had_failed.store(true, Ordering::Relaxed);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        eprintln!(
                            "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: read failed: {e}",
                            n, total, name
                        );
                        let _ = stderr().flush();
                        return;
                    }
                };

                let mzml = match read_mzml_or_b64_from_bytes(in_path, &in_bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        had_failed.store(true, Ordering::Relaxed);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        eprintln!(
                            "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: {e}",
                            n, total, name
                        );
                        let _ = stderr().flush();
                        return;
                    }
                };
                let in_mb = in_bytes.len() as f64 / MB;
                drop(in_bytes);

                let xml = match bin_to_mzml(&mzml) {
                    Ok(v) => v,
                    Err(e) => {
                        had_failed.store(true, Ordering::Relaxed);
                        failed.fetch_add(1, Ordering::Relaxed);
                        let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                        let name = basename(in_path);
                        let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                        eprintln!(
                            "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: bin_to_mzml failed: {e}",
                            n, total, name
                        );
                        let _ = stderr().flush();
                        return;
                    }
                };
                drop(mzml);

                if let Err(e) = fs::write(&out_path, xml.as_bytes()) {
                    had_failed.store(true, Ordering::Relaxed);
                    failed.fetch_add(1, Ordering::Relaxed);
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    let name = basename(&out_path);
                    let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                    eprintln!(
                        "{ANSI_RED}[error]{ANSI_RESET} [{}/{}] {}: write failed: {e}",
                        n, total, name
                    );
                    let _ = stderr().flush();
                    return;
                }
                let out_mb = xml.len() as f64 / MB;
                drop(xml);

                ok.fetch_add(1, Ordering::Relaxed);
                let n = done.fetch_add(1, Ordering::Relaxed) + 1;

                let elapsed_s = t0.elapsed().as_secs_f64();

                let name = basename(&out_path);

                let _g = print_lock.lock().unwrap_or_else(|e| e.into_inner());
                println!(
                    "{ANSI_GREEN}[ok]{ANSI_RESET} [{}/{}] output: {}  input={:.2} MB, output={:.2} MB, time={:.3}s",
                    n, total, name, in_mb, out_mb, elapsed_s
                );
                let _ = stdout().flush();
            })
        });

        let ok = ok.load(Ordering::Relaxed);
        let failed = failed.load(Ordering::Relaxed);
        let skipped = skipped.load(Ordering::Relaxed);

        let d = t_all.elapsed();
        let total_secs = d.as_secs();
        let h = total_secs / 3600;
        let m = (total_secs % 3600) / 60;
        let s = total_secs % 60;

        println!(
            "ok={ok} failed={failed} skipped={skipped} total_time={:02}:{:02}:{:02}",
            h, m, s
        );

        if had_failed.load(Ordering::Relaxed) {
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
        return parse_mzml(bytes).map_err(|e| format!("parse_mzml failed: {e}"));
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

#[inline]
fn has_valid_trailer(path: &Path, file_len: u64) -> bool {
    if file_len < FILE_TRAILER.len() as u64 {
        return false;
    }

    let mut f = match fs::File::open(path) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let back = -(FILE_TRAILER.len() as i64);
    if f.seek(SeekFrom::End(back)).is_err() {
        return false;
    }

    let mut tail = [0u8; 8];
    if f.read_exact(&mut tail).is_err() {
        return false;
    }

    tail == FILE_TRAILER
}

#[inline]
fn basename(p: &Path) -> std::borrow::Cow<'_, str> {
    p.file_name()
        .unwrap_or_else(|| p.as_os_str())
        .to_string_lossy()
}

#[inline]
fn trim_mzml_for_cat(mzml: &mut MzML) {
    if let Some(s) = mzml.run.spectrum_list.as_mut() {
        s.spectra.clear();
    }
    if let Some(c) = mzml.run.chromatogram_list.as_mut() {
        c.chromatograms.clear();
    }
}