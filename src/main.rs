use bluer::{
    monitor::{Monitor, MonitorEvent, Pattern, RssiSamplingPeriod},
    Address,
};
use clap::{ArgAction, Parser, Subcommand};
use futures::StreamExt;
use log::{debug, error, info, warn};
use prometheus_client::encoding::text::encode;
use prometheus_client::registry::Registry;
use serde::{
    de::{Deserializer, Error},
    Deserialize,
};
use std::io::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};
use std::{path::PathBuf, str::FromStr};
use tokio::sync::mpsc;

// https://docs.rs/prometheus-client/latest/prometheus_client/
mod sensor;
use sensor::{RuuviData, SensorData, WaterSensor};

#[derive(serde::Deserialize, Debug, Clone)]
#[allow(dead_code)]
struct KnownDevice {
    #[serde(deserialize_with = "addr_from_str")]
    addr: bluer::Address,
    name: String,
    labels: HashMap<String, String>,
}

fn addr_from_str<'de, D>(deserializer: D) -> Result<bluer::Address, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    bluer::Address::from_str(s.as_str()).map_err(D::Error::custom)
}

#[derive(Debug)]
struct Measurement {
    data: Arc<dyn SensorData + Send + Sync>,
    labels: Vec<(String, String)>,
    key: String,
    when: SystemTime,
}

#[derive(Debug)]
enum WriteError {
    #[allow(dead_code)]
    IOError(std::io::Error),
    FormatError(std::fmt::Error),
}

impl From<std::io::Error> for WriteError {
    fn from(value: std::io::Error) -> Self {
        WriteError::IOError(value)
    }
}

impl From<std::fmt::Error> for WriteError {
    fn from(value: std::fmt::Error) -> Self {
        WriteError::FormatError(value)
    }
}

#[derive(Debug)]
struct WriteMetricsOptions {
    dir: String,
    file_prefix: String,
    metric_prefix: Option<String>,
    #[allow(dead_code)]
    const_labels: Vec<(String, String)>,
    stale_after: Duration,
}

#[allow(dead_code)]
#[derive(Debug)]
enum StaleFileError {
    IOError(std::io::Error),
    ParseFloatError(std::num::ParseFloatError),
    SystemTimeError(std::time::SystemTimeError),
    MarkerNotFound,
    BadMarker,
}

impl From<std::time::SystemTimeError> for StaleFileError {
    fn from(value: std::time::SystemTimeError) -> Self {
        StaleFileError::SystemTimeError(value)
    }
}

impl From<std::io::Error> for StaleFileError {
    fn from(value: std::io::Error) -> Self {
        StaleFileError::IOError(value)
    }
}

impl From<std::num::ParseFloatError> for StaleFileError {
    fn from(value: std::num::ParseFloatError) -> Self {
        StaleFileError::ParseFloatError(value)
    }
}

const STALE_MARKER: &str = "# Stale after ";

fn get_file_stale(pb: &PathBuf) -> Result<Duration, StaleFileError> {
    let contents = std::fs::read_to_string(pb)?;
    let spl: Vec<_> = contents
        .split('\n')
        .filter(|s| s.starts_with(STALE_MARKER))
        .collect();
    if spl.is_empty() {
        return Err(StaleFileError::MarkerNotFound);
    }
    let first = spl[0];
    if STALE_MARKER.len() > first.len() - 1 {
        return Err(StaleFileError::BadMarker);
    }
   Ok(Duration::from_secs_f64(first[STALE_MARKER.len()..first.len() - 1].parse()?))
}

fn stale_clean_once(prefix: &str, dir: &str, clean_epoch: Duration, dry_run: bool) {
    let rd = match std::fs::read_dir(dir) {
        Err(e) => {
            error!("stale checker failed to read {} {:?}", dir, e);
            return;
        }
        Ok(d) => d,
    };
    let mut candidates: Vec<PathBuf> = Vec::new();
    for c in rd {
        match c {
            Err(e) => {
                error!("Failed to get entry {:?}", e);
                continue;
            }
            Ok(de) => {
                if !de.file_name().to_string_lossy().starts_with(prefix) {
                    continue;
                }
                candidates.push(de.path());
            }
        }
    }
    for c in candidates {
        let gfs = get_file_stale(&c);
        let stale_dur = match gfs {
            Err(error) => {
                error!("Failed to get file staleness {:?} {:?}", &c, error);
                continue;
            }
            Ok(d) => d, 
        };
        
        
            

            if stale_dur > clean_epoch {
                debug!("Not deleting {:?} because it's not stale yet", &c);
                continue;
            }
            info!("Deleting {:?} because stale marker {:?} is before clean epoch {:?} (dry run: {})", &c, stale_dur, clean_epoch, dry_run);
            if dry_run {
                continue;
            }
            if let Err(e) = std::fs::remove_file(&c) {
                error!("Failed to delete {:?}: {:?}", &c, e)
            }
        
    }
}

async fn stale_cleaner(prefix: String, dir: String, check_interval: Duration) {
    info!("Starting stale-cleaner on {dir} with prefix {prefix} for checking every {}", check_interval.as_secs_f64());
    loop {
        tokio::time::sleep(check_interval).await;
        let clean_epoch = Duration::from_secs_f64(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs_f64());
        stale_clean_once(&prefix, &dir, clean_epoch, false);
    }
}

fn write_metrics(
    opts: &WriteMetricsOptions,
    stale: f64,
    m: &Measurement,
) -> Result<(), WriteError> {
    let mut dstbuf = std::path::PathBuf::from(opts.dir.clone());
    let mut tmpbuf = dstbuf.clone();
    let filename = format!("{}-{}.prom", opts.file_prefix, m.key);
    dstbuf.push(&filename);
    tmpbuf.push(format!(".{}.notprom", &filename));
    let tmpfn = tmpbuf.as_path();
    let dstfn = dstbuf.as_path();
    let mut ofh = std::fs::File::create(tmpfn)?;
    writeln!(ofh, "{}{}", STALE_MARKER, stale)?;
    let mut registry = match &opts.metric_prefix {
        Some(prefix) => <Registry>::with_prefix(prefix),
        None => <Registry>::default(),
    };
    m.data.add_metrics(&mut registry, &m.labels);
    let mut out = String::new();
    encode(&mut out, &registry)?;
    ofh.write_all(out.as_bytes())?;
    ofh.flush()?;
    debug!(
        "renaming from {} -> {}",
        tmpfn.to_string_lossy(),
        dstfn.to_string_lossy()
    );
    std::fs::rename(tmpfn, dstfn)?;
    Ok(())
}

async fn metrics_writer_processor(
    mut recv: mpsc::Receiver<Measurement>,
    opts: WriteMetricsOptions,
    write_interval: Duration,
) {
    let mut lastwrite: HashMap<String, SystemTime> = HashMap::new();
    loop {
        let m = match recv.recv().await {
            None => continue,
            Some(m) => m,
        };
        debug!("processing measurement {:?}", &m);
        let now = SystemTime::now();
        if let Some(lw) = lastwrite.get(&m.key) {
            if let Ok(d) = now.duration_since(*lw) {
                if d < write_interval {
                    continue;
                }
                debug!(
                    "allowing for time since:{}, interval:{}, now={:?}, last={:?}",
                    d.as_secs_f64(),
                    write_interval.as_secs_f64(),
                    now,
                    lw
                );
            } else {
                error!("system time overflew");
            }
        }
        let stale_after = m.when.duration_since(SystemTime::UNIX_EPOCH).unwrap() + opts.stale_after;
        if let Err(e) = write_metrics(&opts, stale_after.as_secs_f64(), &m) {
            error!("Failed to write metrics {:?} into {}", e, opts.dir);
            continue;
        }
        lastwrite.insert(m.key, now);
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum LoadKnownError {
    IOError(std::io::Error),
    Serialization(serde_yaml::Error),
    #[allow(dead_code)]
    Empty,
}

impl From<std::io::Error> for LoadKnownError {
    fn from(value: std::io::Error) -> Self {
        Self::IOError(value)
    }
}

impl From<serde_yaml::Error> for LoadKnownError {
    fn from(value: serde_yaml::Error) -> Self {
        Self::Serialization(value)
    }
}

fn load_known_devices_from_reader(
    r: std::fs::File,
    allow_empty: bool,
) -> Result<Vec<KnownDevice>, LoadKnownError> {
    let kd: Vec<KnownDevice> = serde_yaml::from_reader(r)?;
    if allow_empty && kd.is_empty() {
        return Err(LoadKnownError::Empty);
    }
    Ok(kd)
}

fn load_known_devices_from_file(
    s: &str,
    allow_empty: bool,
) -> Result<Vec<KnownDevice>, LoadKnownError> {
    match std::fs::File::open(s) {
        Ok(file) => load_known_devices_from_reader(file, allow_empty),
        Err(e) => {
            if let Some(ose) = e.raw_os_error() {
                if allow_empty && ose == libc::ENOENT {
                    Ok(Vec::new())
                } else {
                    Err(LoadKnownError::IOError(e))
                }
            } else {
                Err(LoadKnownError::IOError(e))
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum ParseLabelsError {
    BadChar(char),
    EmptyString,
    NoEq,
}

fn bad_label_part(s: &str) -> Option<ParseLabelsError> {
    if s.is_empty() {
        return Some(ParseLabelsError::EmptyString);
    }
    for char in s.chars() {
        if !char.is_alphanumeric() {
            return Some(ParseLabelsError::BadChar(char));
        }
    }
    None
}

impl std::error::Error for ParseLabelsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}

impl std::fmt::Display for ParseLabelsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

fn parse_label(l: &str) -> Result<(String, String), ParseLabelsError> {
    if let Some((s1, s2)) = l.split_once('=') {
        if let Some(e) = bad_label_part(s1) {
            Err(e)
        } else if let Some(e) = bad_label_part(s2) {
            Err(e)
        } else {
            Ok((s1.to_owned(), s2.to_owned()))
        }
    } else {
        Err(ParseLabelsError::NoEq)
    }
}

fn load_addrlist_file(l: &String) -> Vec<bluer::Address> {
    let input: Vec<String> = match std::fs::read_to_string(l) {
        Err(e) => {
            warn!("Failed to read blocklist from {} {:?}", l, e);
            Vec::new()
        }
        Ok(s) => serde_yaml::from_str(&s).unwrap_or_else(|e| {
            warn!("Failed to deserialize from {} {:?}", l, e);
            Vec::new()
        }),
    };
    input
        .iter()
        .filter_map(|addrstr| match bluer::Address::from_str(addrstr) {
            Err(e) => {
                warn!("Failed to parse address string {} {:?}", addrstr, e);
                None
            }
            Ok(a) => Some(a),
        })
        .collect()
}

#[allow(dead_code)]
#[derive(Debug)]
enum ProgramError {
    BluerError(bluer::Error),
    BlocklistAllowlistError,
    EmptyKnownDevices,
}

impl From<bluer::Error> for ProgramError {
    fn from(value: bluer::Error) -> Self {
        ProgramError::BluerError(value)
    }
}

const GIT_REV: &str = env!("GIT_REV");
const VERSION: &str = env!("CARGO_PKG_VERSION");

async fn run_gobbler(
    adapter_name: Option<String>,
    metrics_dir: String,
    stale_seconds: f64,
    file_prefix: String,
    metric_prefix: Option<String>,
    known_file: String,
    allow_empty_known_file: bool,
    const_label: Vec<(String, String)>,
    blocklist_file: String,
    block: Vec<String>,
    allowlist_file: String,
    allow: Vec<String>,
) -> Result<(), ProgramError> {
    let mut blocklist_vec = load_addrlist_file(&blocklist_file);
    for b in block {
        match bluer::Address::from_str(&b) {
            Err(e) => warn!("Invalid address {} {:?}", b, e),
            Ok(a) => blocklist_vec.push(a),
        }
    }

    let mut allowlist_vec = load_addrlist_file(&allowlist_file);
    for a in allow {
        match bluer::Address::from_str(&a) {
            Err(e) => warn!("Invalid address {} {:?}", a, e),
            Ok(a) => allowlist_vec.push(a),
        }
    }

    if !allowlist_vec.is_empty() && !blocklist_vec.is_empty() {
        error!("Cannot have both allowlist and blocklist");
        return Err(ProgramError::BlocklistAllowlistError);
    }

    let blocklist: HashMap<bluer::Address, ()> =
        blocklist_vec.into_iter().map(|a| (a, ())).collect();

    let allowlist: HashMap<bluer::Address, ()> =
        allowlist_vec.into_iter().map(|a| (a, ())).collect();

    let allowstr = blocklist.iter().fold(String::new(), |acc, (addr, _)| {
        if acc.is_empty() {
            addr.to_string()
        } else {
            format!("{},{}", acc, addr)
        }
    });

    let blockstr = blocklist.iter().fold(String::new(), |acc, (addr, _)| {
        if acc.is_empty() {
            addr.to_string()
        } else {
            format!("{},{}", acc, addr)
        }
    });

    info!("Allowlist: {allowstr}");
    info!("Blocklist: {blockstr}");

    let writeopts = WriteMetricsOptions {
        dir: metrics_dir,
        file_prefix,
        metric_prefix,
        const_labels: const_label,
        stale_after: Duration::from_secs_f64(stale_seconds),
    };

    let known_devices = match load_known_devices_from_file(&known_file, allow_empty_known_file) {
        Ok(known_devices) => known_devices,
        Err(e) => {
            error!("Failed to load known devices {}: {:?}", known_file, e);
            Vec::new()
        }
    };
    if !allow_empty_known_file && known_devices.is_empty() {
        error!("Empty known devices and not allowing empty known devices");
        return Err(ProgramError::EmptyKnownDevices);
    }

    let (tx_metrics, rx_metrics) = mpsc::channel::<Measurement>(10);

    let file_prefix = writeopts.file_prefix.clone();
    let clean_dir = writeopts.dir.clone();

    tokio::spawn(async move {
        metrics_writer_processor(rx_metrics, writeopts, Duration::from_secs(5)).await;
    });

    tokio::spawn(async move {
        stale_cleaner(
            file_prefix,
            clean_dir,
            Duration::from_secs(5),
        )
        .await
    });

    debug!("known devices is {:?}", known_devices);
    let session = bluer::Session::new().await?;
    let adapter = match adapter_name {
        Some(n) => session.adapter(&n)?,
        None => session.default_adapter().await?,
    };
    let adapter_addr = adapter.address().await?;
    info!(
        "Opened adapter {}/{}",
        adapter.name(),
        adapter_addr.to_string()
    );
    adapter.set_powered(true).await?;
    /* WTF?!
    for prop in adapter.all_properties().await? {
        debug!("adapter prop {:?}", prop);
    }; */
    let mm = adapter.monitor().await?;
    let mut monitor_handle = mm
        .register(Monitor {
            monitor_type: bluer::monitor::Type::OrPatterns,
            rssi_low_threshold: None,
            rssi_high_threshold: None,
            rssi_low_timeout: Some(Duration::from_secs(5)),
            rssi_high_timeout: Some(Duration::from_secs(5)),
            rssi_sampling_period: Some(RssiSamplingPeriod::Period(Duration::from_secs(1))),
            patterns: Some(vec![
                Pattern {
                    data_type: 0xff,
                    start_position: 0x00,
                    content: vec![0x99, 0x04],
                },
                Pattern {
                    data_type: 0xff,
                    start_position: 0x00,
                    content: vec![0xff, 0xff, b'L', b'K'],
                },
            ]),
            ..Default::default()
        })
        .await?;
    let aliases: HashMap<Address, String> = known_devices
        .iter()
        .map(|kd| (kd.addr, kd.name.clone()))
        .collect();
    let mut have_tasks: HashMap<String, Arc<AtomicU64>> = HashMap::new();
    while let Some(mevt) = &monitor_handle.next().await {
        let ikd = known_devices.clone();
        let tx_metrics = tx_metrics.clone();
        if let MonitorEvent::DeviceFound(devid) = mevt {
            if !allowlist.is_empty() {
                if !allowlist.contains_key(&devid.device) {
                    continue;
                } else {
                    debug!("not allowlisted {}", devid.device.to_string());
                }
            } else if blocklist.contains_key(&devid.device) {
                debug!("blocklisted {}", devid.device.to_string());
                continue;
            }
            let devkey = &devid.device.to_owned().to_string();
            let mut newdev = false;
            if let Some(ctr) = have_tasks.get(devkey) {
                if ctr.load(Ordering::SeqCst) > 0 {
                    continue;
                }
            } else {
                newdev = true;
                let ctr: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
                have_tasks.insert(devkey.to_owned(), ctr);
            }

            let ctr = have_tasks.get(devkey).unwrap().clone();
            let oldval = ctr.clone().fetch_add(1, Ordering::SeqCst);
            if oldval != 0 {
                panic!("oldval != 0");
            }
            let alias = aliases
                .get(&devid.device)
                .map_or_else(|| "".to_owned(), |alias| alias.clone());
            info!(
                "Discovered device {:?} (new: {:?}, alias: {})",
                devid, newdev, alias
            );

            let dev = adapter.device(devid.device)?;
            tokio::spawn(async move {
                let kd: Vec<KnownDevice> = ikd
                    .clone()
                    .into_iter()
                    .filter(|kd| kd.addr == dev.address())
                    .collect();
                let add_labels: Vec<(String, String)> = if !kd.is_empty() {
                    kd[0]
                        .labels
                        .iter()
                        .map(|(a, b)| (a.clone(), b.clone()))
                        .collect()
                } else {
                    Vec::new()
                };
                let mut events = dev.events().await.unwrap();
                while let Some(ev) = events.next().await {
                    let bluer::DeviceEvent::PropertyChanged(dp) = &ev;
                    if let bluer::DeviceProperty::ManufacturerData(d) = dp {
                        for (key, mfgdata) in d.iter() {
                            if *key == 0xffff {
                                debug!("0xffff mfg data {:?}", mfgdata);
                                if mfgdata.len() >= 6 && mfgdata[0] == b'L' && mfgdata[1] == b'K' {
                                    if let Ok(lkd) = WaterSensor::parse(mfgdata) {
                                        let m = Measurement {
                                            data: Arc::new(lkd),
                                            labels: add_labels.clone(),
                                            when: SystemTime::now(),
                                            key: dev.address().to_string(),
                                        };
                                        if let Err(e) = tx_metrics.send(m).await {
                                            error!("Failed to send {:?}", e);
                                        }
                                    } else {
                                        error!("Failed to parse {:?}", mfgdata);
                                    }
                                } else {
                                    info!("user mfg data {:?}", mfgdata);
                                }
                            } else if *key == 0x0499 {
                                match RuuviData::parse(mfgdata) {
                                    Ok(rd) => {
                                        let m = Measurement {
                                            data: Arc::new(rd),
                                            labels: add_labels.clone(),
                                            when: SystemTime::now(),
                                            key: dev.address().to_string(),
                                        };
                                        if let Err(e) = tx_metrics.send(m).await {
                                            error!("Failed to send {:?}", e);
                                        }
                                    }
                                    Err(e) => error!("Failed to parse ruuvi data {}", e),
                                }
                            }
                        }
                    } else {
                        debug!("On device {:?}, received event {:?}", dev, &ev);
                    }
                }
                info!("task shutting down for {:?}", dev);
                let oldctr = ctr.fetch_sub(1, Ordering::SeqCst);
                if oldctr != 1 {
                    panic!("expected refct of 1, got {}", oldctr);
                }
            });
        }
    }

    Ok(())
}

#[derive(Parser)]
#[command(name = "bt-gobbler-rs", version = VERSION, about = "A Bluetooth LE sensor data gobbler for Prometheus")]
struct Cli {
    #[arg(
        long = "dir",
        short = 'D',
        default_value = "/var/lib/prometheus/node-exporter"
    )]
    dir: String,

    #[arg(long = "file-prefix", short = 'P', default_value = "bt-gobbler-rs")]
    file_prefix: String,

    #[arg(long = "max-age-seconds", short = 'S', default_value = "600")]
    max_age_seconds: f64,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Gobble {
        #[arg(long = "adapter-name", short = 'i')]
        adapter_name: Option<String>,

        #[arg(long = "metric-prefix", short = 'M')]
        metric_prefix: Option<String>,

        #[arg(
            long = "known-file",
            short = 'K',
            default_value = "/etc/bt-gobbler/known-devices.yaml"
        )]
        known_file: String,

        #[arg(long = "allow-empty-known-file", default_value = "false")]
        allow_empty_known_file: bool,

        #[arg(long = "const-label", short = 'l', action = ArgAction::Append, value_parser = parse_label)]
        const_label: Vec<(String, String)>,

        #[arg(
            long = "blocklist-file",
            default_value = "/etc/bt-gobbler/blocklist.yaml"
        )]
        blocklist_file: String,

        #[arg(long = "block", short = 'B', action = ArgAction::Append)]
        block: Vec<String>,

        #[arg(
            long = "allowlist-file",
            default_value = "/etc/bt-gobbler/allowlist.yaml"
        )]
        allowlist_file: String,

        #[arg(long = "allow", short = 'A', action = ArgAction::Append)]
        allow: Vec<String>,
    },
    CleanStale {
        #[arg(
            long = "dry-run",
            default_value = "false",
             help = "If true, will only log files that would be deleted without actually deleting them"
        )]
        dry_run: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ProgramError> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", format!("{}=INFO", std::module_path!()));
    }
    env_logger::init();
    info!("bt-gobble-rs ({VERSION}/{GIT_REV}) starting");
    let cli = Cli::parse();
    match cli.command {
        Commands::Gobble {
            adapter_name,
            metric_prefix,
            known_file,
            allow_empty_known_file,
            const_label,
            blocklist_file,
            block,
            allowlist_file,
            allow,
        } => {
            run_gobbler(
                adapter_name,
                cli.dir,
                cli.max_age_seconds,
                cli.file_prefix,
                metric_prefix,
                known_file,
                allow_empty_known_file,
                const_label,
                blocklist_file,
                block,
                allowlist_file,
                allow,
            )
            .await?;
        }
        Commands::CleanStale { dry_run } => {
            stale_clean_once(
                &cli.file_prefix,
                &cli.dir,
                Duration::from_secs_f64(cli.max_age_seconds),
                dry_run,
            );
        }
    }
    Ok(())
}
