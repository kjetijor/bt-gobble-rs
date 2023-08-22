use bluer::monitor::{Monitor, MonitorEvent, Pattern, RssiSamplingPeriod};
use clap::{Arg, ArgAction, Command};
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

fn get_file_stale(pb: &PathBuf) -> Result<Option<Duration>, StaleFileError> {
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
    let file_stale_epoch =
        Duration::from_secs_f64(first[STALE_MARKER.len()..first.len() - 1].parse()?);
    let now_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
    Ok(now_epoch.checked_sub(file_stale_epoch))
}

async fn stale_cleaner(prefix: String, dir: String, check_interval: Duration, max_age: Duration) {
    info!("Starting stale-cleaner on {dir} with prefix {prefix} for checking every {} with stale age {}", check_interval.as_secs_f64(), max_age.as_secs_f64());
    loop {
        tokio::time::sleep(check_interval).await;
        let rd = match std::fs::read_dir(&dir) {
            Err(e) => {
                error!("stale checker failed to read {} {:?}", dir, e);
                continue;
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
                    if !de
                        .file_name()
                        .to_string_lossy()
                        .starts_with(prefix.as_str())
                    {
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
                Ok(None) => continue,
                Ok(Some(d)) => d,
            };
            if stale_dur > max_age {
                info!("Deleting {:?} because {:?} > {:?}", &c, stale_dur, max_age);
                if let Err(e) = std::fs::remove_file(&c) {
                    error!("Failed to delete {:?}: {:?}", &c, e)
                }
            }
        }
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
    let rdr = std::fs::File::open(s)?;
    load_known_devices_from_reader(rdr, allow_empty)
}

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

fn load_blocklist_file(l: &String) -> Vec<bluer::Address> {
    let input: Vec<String> = match std::fs::read_to_string(l) {
        Err(e) => {
            warn!("Failed to read blocklist from {} {:?}", l, e);
            Vec::new()
        }
        Ok(s) => serde_yaml::from_str(&s).map_or_else(
            |e| {
                warn!("Failed to deserialize from {} {:?}", l, e);
                Vec::new()
            },
            |v| v,
        ),
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

const GIT_REV: &str = env!("GIT_REV");

#[tokio::main(flavor = "current_thread")]
async fn main() -> bluer::Result<()> {
    let matches = Command::new("bt-gobble-rs")
        .arg(
            Arg::new("metrics-dir")
                .long("metrics-dir")
                .short('D')
                .default_value("/var/lib/prometheus/node-exporter"),
        )
        .arg(
            Arg::new("stale-seconds")
                .long("stale-seconds")
                .short('S')
                .default_value("600")
                .value_parser(clap::value_parser!(f64)),
        )
        .arg(
            Arg::new("file-prefix")
                .short('P')
                .long("file-prefix")
                .default_value("bt-gobbler-rs"),
        )
        .arg(Arg::new("metric-prefix").long("metric-prefix").short('M'))
        .arg(
            Arg::new("known-file")
                .long("known-file")
                .short('K')
                .default_value("/etc/bt-gobbler/known-devices.yaml"),
        )
        .arg(
            Arg::new("allow-empty-known-file")
                .long("allow-empty-known-file")
                .default_value("false")
                .value_parser(clap::value_parser!(bool)),
        )
        .arg(
            Arg::new("const-label")
                .long("const-label")
                .short('l')
                .action(ArgAction::Append)
                .value_parser(clap::builder::ValueParser::new(parse_label)),
        )
        .arg(
            Arg::new("blocklist-file")
                .long("blocklist-file")
                .default_value("/etc/bt-gobbler/blocklist.yaml"),
        )
        .arg(
            Arg::new("block")
                .long("block")
                .short('b')
                .action(ArgAction::Append),
        )
        .get_matches();
    let metrics_dir = matches
        .get_one::<String>("metrics-dir")
        .expect("No metrics dir given");
    let stale_secs = matches
        .get_one::<f64>("stale-seconds")
        .expect("No stale secs given");
    let file_prefix = matches
        .get_one::<String>("file-prefix")
        .expect("No metrics file prefix given");
    let known_devices_file = matches.get_one::<String>("known-file");
    let allow_empty_known_file = matches
        .get_one::<bool>("allow-empty-known-file")
        .expect("bug");
    let metric_prefix = matches.get_one::<String>("metric-prefix");
    let const_labels: Vec<(String, String)> =
        if let Some(m) = matches.get_many::<(String, String)>("const-label") {
            m.map(|(a, b)| (a.to_owned(), b.to_owned())).collect()
        } else {
            Vec::new()
        };
    let blocklist_file = matches
        .get_one::<String>("blocklist-file")
        .expect("expected blocklist filename");

    let mut blocklist_vec = load_blocklist_file(blocklist_file);

    if let Some(blocks) = matches.get_many::<String>("block") {
        for block in blocks {
            match bluer::Address::from_str(block) {
                Err(e) => warn!("Invalid address {} {:?}", block, e),
                Ok(a) => blocklist_vec.push(a),
            }
        }
    }

    let blocklist: HashMap<bluer::Address, ()> = blocklist_vec.into_iter().map(|a| (a, ())).collect();
    for (bl, _) in blocklist.iter() {
        info!("blocklist {}", bl.to_string());
    }

    let writeopts = WriteMetricsOptions {
        dir: metrics_dir.to_owned(),
        file_prefix: file_prefix.to_owned(), // validate
        metric_prefix: metric_prefix.map(|s| s.to_owned()), // validate
        const_labels,          // validate
        stale_after: Duration::from_secs_f64(*stale_secs), //validate <set min ?>
    };
    //    panic!("config {:?}", writeopts);
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", format!("{}=INFO", std::module_path!()));
    }
    env_logger::init();

    let known_devices = if let Some(kfn) = known_devices_file {
        load_known_devices_from_file(kfn, *allow_empty_known_file)
            .expect("failed to load known devices")
    } else {
        Vec::new()
    };
    if !allow_empty_known_file && known_devices.is_empty() {
        error!("Empty known devices and not allowing empty known devices");
        return Ok(());
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
            Duration::from_secs(600),
        )
        .await
    });

    debug!("known devices is {:?}", known_devices);
    info!("bt-gobble-rs starting {GIT_REV}");
    let session = bluer::Session::new().await?;
    let adapter = session.default_adapter().await?;
    let mm = adapter.monitor().await?;
    adapter.set_powered(true).await?;
    let mut monitor_handle = mm
        .register(Monitor {
            monitor_type: bluer::monitor::Type::OrPatterns,
            rssi_low_threshold: None,
            rssi_high_threshold: None,
            rssi_low_timeout: Some(Duration::from_secs(5)),
            rssi_high_timeout: Some(Duration::from_secs(5)),
            rssi_sampling_period: Some(RssiSamplingPeriod::First),
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

    let mut have_tasks: HashMap<String, Arc<AtomicU64>> = HashMap::new();
    while let Some(mevt) = &monitor_handle.next().await {
        let ikd = known_devices.clone();
        let tx_metrics = tx_metrics.clone();
        if let MonitorEvent::DeviceFound(devid) = mevt {
            if blocklist.contains_key(&devid.device) {
                debug!("blocklisted {}", devid.device.to_string());
                continue
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

            info!("Discovered device {:?} (new: {:?})", devid, newdev);

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
