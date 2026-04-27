// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    env,
    fmt::{self, Display, Formatter},
    hint::black_box,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use engine_rocks::{RocksCfOptions, RocksDbOptions, RocksEngine, util};
use engine_traits::{
    CF_DEFAULT, CF_LOCK, CF_WRITE, CfOptions, IterOptions, Iterable, Iterator as EngineIterator,
    MiscExt, Mutable, WriteBatch, WriteBatchExt,
};
use tempfile::TempDir;
use tokio::task::LocalSet;
use txn_types::{Key, Lock, LockType, TimeStamp, Write, WriteRef, WriteType};

const DEFAULT_ROWS: usize = 100_000;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_TASKS_PER_CF: usize = 8;
const DEFAULT_BATCH_SIZE: usize = 1_000;
const DEFAULT_YIELD_EVERY: u64 = 1_024;
const DEFAULT_VALUE_SIZE: usize = 32;
const DEFAULT_START_TS: u64 = 42;
const DEFAULT_COMMIT_TS: u64 = 100;

type BenchResult<T> = std::result::Result<T, BenchError>;

#[derive(Debug)]
struct BenchError(String);

impl BenchError {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl Display for BenchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BenchError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BenchMode {
    Both,
    Lock,
    Write,
}

impl BenchMode {
    fn cfs(self) -> &'static [&'static str] {
        match self {
            BenchMode::Both => &[CF_WRITE, CF_LOCK],
            BenchMode::Lock => &[CF_LOCK],
            BenchMode::Write => &[CF_WRITE],
        }
    }
}

impl FromStr for BenchMode {
    type Err = BenchError;

    fn from_str(s: &str) -> BenchResult<Self> {
        match s {
            "both" => Ok(Self::Both),
            "lock" | "lockcf" => Ok(Self::Lock),
            "write" | "writecf" => Ok(Self::Write),
            other => Err(BenchError::new(format!(
                "unknown mode `{other}`, expected both|lock|write"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DecodeMode {
    None,
    CfRecord,
}

impl FromStr for DecodeMode {
    type Err = BenchError;

    fn from_str(s: &str) -> BenchResult<Self> {
        match s {
            "none" => Ok(Self::None),
            "cf-record" | "record" | "parse" => Ok(Self::CfRecord),
            other => Err(BenchError::new(format!(
                "unknown decode mode `{other}`, expected none|cf-record"
            ))),
        }
    }
}

#[derive(Clone, Debug)]
struct Config {
    rows: usize,
    duration: Duration,
    tasks_per_cf: usize,
    batch_size: usize,
    yield_every: u64,
    value_size: usize,
    mode: BenchMode,
    decode: DecodeMode,
    fill_cache: bool,
    final_flush: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rows: DEFAULT_ROWS,
            duration: Duration::from_secs(DEFAULT_DURATION_SECS),
            tasks_per_cf: DEFAULT_TASKS_PER_CF,
            batch_size: DEFAULT_BATCH_SIZE,
            yield_every: DEFAULT_YIELD_EVERY,
            value_size: DEFAULT_VALUE_SIZE,
            mode: BenchMode::Both,
            decode: DecodeMode::CfRecord,
            fill_cache: false,
            final_flush: true,
        }
    }
}

#[derive(Default, Debug)]
struct WorkerStats {
    cf: &'static str,
    task_id: usize,
    scans: u64,
    rows: u64,
    bytes: u64,
    cpu_nanos: u64,
    wall_nanos: u64,
    checksum: u64,
    parse_errors: u64,
}

impl WorkerStats {
    fn merge(&mut self, other: &WorkerStats) {
        self.scans += other.scans;
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.cpu_nanos += other.cpu_nanos;
        self.wall_nanos += other.wall_nanos;
        self.checksum ^= other.checksum;
        self.parse_errors += other.parse_errors;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = Config::parse()?;
    validate_config(&config)?;

    println!("cf scan benchmark config: {config:?}");
    println!(
        "opening temporary RocksDB and inserting {} rows per CF",
        config.rows
    );

    let (_temp_dir, engine) = open_engine()?;
    load_rows(&engine, &config)?;
    if config.final_flush {
        engine.flush_cf(CF_WRITE, true).context("flush write cf")?;
        engine.flush_cf(CF_LOCK, true).context("flush lock cf")?;
    }
    print_cf_layout(&engine)?;

    let started = Instant::now();
    let cfs = config.mode.cfs();
    let mut stats = Vec::with_capacity(cfs.len() * config.tasks_per_cf);
    for (phase_idx, &cf) in cfs.iter().enumerate() {
        println!(
            "starting phase {}/{}: scan {} for {:.3}s",
            phase_idx + 1,
            cfs.len(),
            cf,
            config.duration.as_secs_f64()
        );
        stats.extend(run_scan_phase(engine.clone(), &config, cf).await?);
    }

    print_results(&config, started.elapsed(), &stats);
    Ok(())
}

impl Config {
    fn parse() -> Result<Self> {
        let mut config = Config::default();
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "-h" || arg == "--help" {
                print_help();
                std::process::exit(0);
            }
            if arg == "--fill-cache" {
                config.fill_cache = true;
                continue;
            }
            if arg == "--no-final-flush" || arg == "--no-final-switch" {
                config.final_flush = false;
                continue;
            }

            let (name, value) = if let Some((name, value)) = arg.split_once('=') {
                (name.to_owned(), value.to_owned())
            } else {
                let value = args
                    .next()
                    .ok_or_else(|| BenchError::new(format!("missing value for `{arg}`")))?;
                (arg, value)
            };

            match name.as_str() {
                "--rows" | "-n" => config.rows = parse_value(&name, &value)?,
                "--duration-secs" | "--duration" => {
                    config.duration = Duration::from_secs(parse_value(&name, &value)?)
                }
                "--tasks-per-cf" | "--tasks" => config.tasks_per_cf = parse_value(&name, &value)?,
                "--batch-size" => config.batch_size = parse_value(&name, &value)?,
                "--yield-every" => config.yield_every = parse_value(&name, &value)?,
                "--value-size" => config.value_size = parse_value(&name, &value)?,
                "--mode" => config.mode = value.parse()?,
                "--decode" => config.decode = value.parse()?,
                other => {
                    return Err(BenchError::new(format!(
                        "unknown option `{other}`; use --help for usage"
                    ))
                    .into());
                }
            }
        }
        Ok(config)
    }
}

fn validate_config(config: &Config) -> Result<()> {
    if config.rows == 0 {
        bail!("--rows must be greater than 0");
    }
    if config.tasks_per_cf == 0 {
        bail!("--tasks-per-cf must be greater than 0");
    }
    if config.batch_size == 0 {
        bail!("--batch-size must be greater than 0");
    }
    if config.yield_every == 0 {
        bail!("--yield-every must be greater than 0");
    }
    if config.value_size > txn_types::SHORT_VALUE_MAX_LEN {
        bail!(
            "--value-size must be <= {} because WRITE_CF stores it as short_value",
            txn_types::SHORT_VALUE_MAX_LEN
        );
    }
    Ok(())
}

fn parse_value<T>(name: &str, value: &str) -> BenchResult<T>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|err| BenchError::new(format!("invalid value for {name}: {err}")))
}

fn print_help() {
    println!(
        "\
Usage:
  cargo run --release --bin cf_scan_bench -- [options]

Options:
  -n, --rows <N>              Rows inserted into each CF. Default: {DEFAULT_ROWS}
      --duration-secs <N>     Benchmark duration. Default: {DEFAULT_DURATION_SECS}
      --tasks-per-cf <N>      Tokio local tasks per tested CF. Default: {DEFAULT_TASKS_PER_CF}
      --batch-size <N>        Rows per RocksDB write batch during loading. Default: {DEFAULT_BATCH_SIZE}
      --yield-every <N>       Rows scanned before a task yields. Default: {DEFAULT_YIELD_EVERY}
      --value-size <N>        Short value bytes stored in each lock/write record. Default: {DEFAULT_VALUE_SIZE}
      --mode <both|lock|write> both runs WRITE_CF first, then LOCK_CF, each for --duration-secs.
      --decode <cf-record|none>
      --fill-cache            Enable RocksDB iterator fill_cache.
      --no-final-flush        Keep loaded data in memtables instead of flushing to SST.
"
    );
}

fn open_engine() -> Result<(TempDir, RocksEngine)> {
    let temp_dir = tempfile::Builder::new()
        .prefix("tikv-cf-scan-bench")
        .tempdir()
        .context("create temporary RocksDB directory")?;
    let mut default_cf_opts = RocksCfOptions::default();
    let mut write_cf_opts = RocksCfOptions::default();
    let mut lock_cf_opts = RocksCfOptions::default();
    default_cf_opts.set_disable_auto_compactions(true);
    write_cf_opts.set_disable_auto_compactions(true);
    lock_cf_opts.set_disable_auto_compactions(true);

    let engine = util::new_engine_opt(
        temp_dir.path().to_str().unwrap(),
        RocksDbOptions::default(),
        vec![
            (CF_DEFAULT, default_cf_opts),
            (CF_WRITE, write_cf_opts),
            (CF_LOCK, lock_cf_opts),
        ],
    )
    .context("open RocksDB engine")?;

    Ok((temp_dir, engine))
}

fn load_rows(engine: &RocksEngine, config: &Config) -> Result<()> {
    let mut wb = engine.write_batch_with_cap(config.batch_size * 2);
    for i in 0..config.rows {
        let raw_key = bench_key(i);
        let lock_key = Key::from_raw(raw_key.as_slice()).into_encoded();
        let write_key = Key::from_raw(raw_key.as_slice())
            .append_ts(TimeStamp::from(DEFAULT_COMMIT_TS))
            .into_encoded();
        let short_value = vec![b'v'; config.value_size];
        let lock_value = Lock::new(
            LockType::Put,
            raw_key,
            TimeStamp::from(DEFAULT_START_TS),
            3_000,
            Some(short_value.clone()),
            TimeStamp::zero(),
            config.rows as u64,
            TimeStamp::zero(),
            false,
        )
        .to_bytes();
        let write_value = Write::new(
            WriteType::Put,
            TimeStamp::from(DEFAULT_START_TS),
            Some(short_value),
        )
        .as_ref()
        .to_bytes();

        wb.put_cf(CF_LOCK, &lock_key, &lock_value)
            .context("write lock cf")?;
        wb.put_cf(CF_WRITE, &write_key, &write_value)
            .context("write write cf")?;

        if (i + 1) % config.batch_size == 0 {
            wb.write().context("commit RocksDB write batch")?;
            wb.clear();
        }
    }
    if !wb.is_empty() {
        wb.write().context("commit final RocksDB write batch")?;
    }
    Ok(())
}

async fn run_scan_phase(
    engine: RocksEngine,
    config: &Config,
    cf: &'static str,
) -> Result<Vec<WorkerStats>> {
    let stop_at = Instant::now() + config.duration;
    let local = LocalSet::new();
    let mut handles = Vec::with_capacity(config.tasks_per_cf);

    local
        .run_until(async {
            for task_id in 0..config.tasks_per_cf {
                let engine = engine.clone();
                let config = config.clone();
                handles.push(tokio::task::spawn_local(async move {
                    scan_worker(engine, config, cf, task_id, stop_at).await
                }));
            }

            let mut stats = Vec::with_capacity(handles.len());
            for handle in handles {
                stats.push(handle.await.context("worker join error")??);
            }
            Ok::<_, anyhow::Error>(stats)
        })
        .await
}

async fn scan_worker(
    engine: RocksEngine,
    config: Config,
    cf: &'static str,
    task_id: usize,
    stop_at: Instant,
) -> Result<WorkerStats> {
    let mut stats = WorkerStats {
        cf,
        task_id,
        ..WorkerStats::default()
    };

    while Instant::now() < stop_at {
        let wall_start = Instant::now();
        let mut cpu_mark = thread_cpu_nanos();
        let mut iter = engine
            .iterator_opt(cf, IterOptions::new(None, None, config.fill_cache))
            .with_context(|| format!("create iterator for {cf}"))?;

        let mut rows_since_yield = 0;
        let mut valid = iter.seek_to_first()?;
        while valid {
            let key = iter.key();
            let value = iter.value();
            stats.bytes += key.len() as u64 + value.len() as u64;
            stats.checksum ^= (key.len() as u64)
                .wrapping_mul(31)
                .wrapping_add(value.len() as u64);
            match consume_value(cf, value, config.decode) {
                Ok(parsed) => stats.checksum ^= parsed,
                Err(_) => stats.parse_errors += 1,
            }

            stats.rows += 1;
            rows_since_yield += 1;
            valid = iter.next()?;

            if rows_since_yield >= config.yield_every {
                let now = thread_cpu_nanos();
                stats.cpu_nanos += now.saturating_sub(cpu_mark);
                rows_since_yield = 0;
                tokio::task::yield_now().await;
                cpu_mark = thread_cpu_nanos();
            }
        }

        let now = thread_cpu_nanos();
        stats.cpu_nanos += now.saturating_sub(cpu_mark);
        stats.wall_nanos += duration_to_nanos(wall_start.elapsed());
        stats.scans += 1;
        black_box(stats.checksum);
    }

    Ok(stats)
}

fn consume_value(cf: &str, value: &[u8], decode: DecodeMode) -> std::result::Result<u64, ()> {
    match decode {
        DecodeMode::None => Ok(value.len() as u64),
        DecodeMode::CfRecord if cf == CF_LOCK => Lock::parse(value)
            .map(|lock| lock.ts.into_inner())
            .map_err(|_| ()),
        DecodeMode::CfRecord if cf == CF_WRITE => WriteRef::parse(value)
            .map(|write| write.start_ts.into_inner())
            .map_err(|_| ()),
        DecodeMode::CfRecord => Ok(value.len() as u64),
    }
}

fn thread_cpu_nanos() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        panic!(
            "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed: {}",
            std::io::Error::last_os_error()
        );
    }
    (ts.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(ts.tv_nsec as u64)
}

fn duration_to_nanos(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_mul(1_000_000_000)
        .saturating_add(duration.subsec_nanos() as u64)
}

fn bench_key(i: usize) -> Vec<u8> {
    format!("bench_key_{i:020}").into_bytes()
}

fn print_cf_layout(engine: &RocksEngine) -> Result<()> {
    for cf in [CF_WRITE, CF_LOCK] {
        let handle = util::get_cf_handle(engine.as_inner(), cf)
            .with_context(|| format!("get {cf} handle"))?;
        let cf_meta = engine.as_inner().get_column_family_meta_data(handle);
        let levels = cf_meta
            .get_levels()
            .iter()
            .enumerate()
            .map(|(idx, level)| {
                let files = level.get_files();
                let size: usize = files.iter().map(|file| file.get_size()).sum();
                format!("L{idx}:files={},bytes={size}", files.len())
            })
            .collect::<Vec<_>>()
            .join(", ");
        println!("{cf} layout: levels=[{levels}]");
    }
    Ok(())
}

fn print_results(config: &Config, elapsed: Duration, worker_stats: &[WorkerStats]) {
    println!();
    println!(
        "elapsed: {:.3}s, per_cf_duration={:.3}s, decode={:?}, tasks_per_cf={}",
        elapsed.as_secs_f64(),
        config.duration.as_secs_f64(),
        config.decode,
        config.tasks_per_cf
    );
    println!(
        "{:<8} {:>6} {:>10} {:>14} {:>14} {:>12} {:>12} {:>14} {:>10}",
        "cf", "tasks", "scans", "rows", "bytes", "cpu_ms", "ns/row", "rows/cpu_s", "errors"
    );

    for &cf in config.mode.cfs() {
        let mut total = WorkerStats {
            cf,
            ..WorkerStats::default()
        };
        let mut tasks = 0usize;
        for stats in worker_stats.iter().filter(|stats| stats.cf == cf) {
            tasks += 1;
            total.merge(stats);
        }

        let cpu_secs = total.cpu_nanos as f64 / 1_000_000_000.0;
        let ns_per_row = if total.rows == 0 {
            0.0
        } else {
            total.cpu_nanos as f64 / total.rows as f64
        };
        let rows_per_cpu_sec = if cpu_secs == 0.0 {
            0.0
        } else {
            total.rows as f64 / cpu_secs
        };

        println!(
            "{:<8} {:>6} {:>10} {:>14} {:>14} {:>12.3} {:>12.1} {:>14.0} {:>10}",
            cf,
            tasks,
            total.scans,
            total.rows,
            total.bytes,
            total.cpu_nanos as f64 / 1_000_000.0,
            ns_per_row,
            rows_per_cpu_sec,
            total.parse_errors
        );

        for stats in worker_stats.iter().filter(|stats| stats.cf == cf) {
            println!(
                "  task {:<3} scans={:<8} rows={:<12} cpu_ms={:.3} checksum={}",
                stats.task_id,
                stats.scans,
                stats.rows,
                stats.cpu_nanos as f64 / 1_000_000.0,
                stats.checksum
            );
        }
    }
}
