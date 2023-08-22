use prometheus_client::{
    metrics::{family::Family, gauge::Gauge},
    registry::Registry,
};
use std::{fmt, sync::atomic::AtomicU64};

pub trait SensorData: fmt::Debug {
    fn add_metrics(&self, r: &mut Registry, add_labels: &[(String, String)]);
}

macro_rules! u16_at {
    ($sl:ident, $i:expr) => {
        u16::from_be_bytes([$sl[$i], $sl[$i + 1]])
    };
}

macro_rules! i16_at {
    ($sl:ident, $i:expr) => {
        i16::from_be_bytes([$sl[$i], $sl[$i + 1]])
    };
}

#[derive(Debug, PartialEq)]
pub struct WaterSensor {
    pub flags: u8,
    pub voltages: Vec<i16>,
    pub seq: u8,
}

impl WaterSensor {
    #[allow(dead_code)]
    pub fn parse(d: &[u8]) -> Result<WaterSensor, ParseError> {
        if d.len() < 6 {
            return Err(ParseError::BadLength);
        }
        if d[0] != b'L' || d[1] != b'K' {
            return Err(ParseError::BadWaterMagic((d[0] as u16) << 8 | d[1] as u16));
        }
        log::debug!("water data at {:x} {:x}", &d[4], &d[5]);
        let voltages = vec![i16_at!(d, 4), i16_at!(d,6)];

        Ok(WaterSensor {
            flags: d[2],
            voltages,
            seq: d[3],
        })
    }
}

impl SensorData for WaterSensor {
    fn add_metrics(&self, r: &mut Registry, add_labels: &[(String, String)]) {
        let base_labels = Vec::from(add_labels);
        let water_present = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register("water_present", "Water present", water_present.clone());
        water_present.get_or_create(&base_labels).set(
            if self.flags & 0x01 > 0 {
                1.0
            } else {
                0.0
            }
        );
        let raw_flags = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register("raw_flags", "Raw flag value", raw_flags.clone());
        raw_flags.get_or_create(&base_labels).set(self.flags as f64);

        let raw_adc = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register("raw_adc", "Raw ADC reading", raw_adc.clone());
        let voltage = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register("voltage", "Voltage", voltage.clone());
        for (idx, raw_adc_val) in self.voltages.iter().enumerate() {
            let mut labels = base_labels.clone();
            labels.push(("adc".to_owned(), format!("{}", idx)));
            let voltage_value = 3600f64 / 4096f64 * (raw_adc_val & 0x4000) as f64;
            raw_adc.get_or_create(&labels).set(*raw_adc_val as f64);
            voltage.get_or_create(&labels).set(voltage_value);
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct RuuviData {
    pub temperature_c: f64,
    pub rel_humidity: f64,
    pub pressure: u64,
    pub voltage: f64,
    pub movecount: u8,
    pub acc_x: i16,
    pub acc_y: i16,
    pub acc_z: i16,
    pub seq: u16,
    pub hwaddr: bluer::Address,
}

#[derive(Debug, PartialEq)]
pub enum ParseError {
    BadLength,
    BadWaterMagic(u16),
    RuuviVersion(u8),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadLength => write!(f, "bad length"),
            Self::RuuviVersion(v) => write!(f, "bad version expected 5, got {}", v),
            Self::BadWaterMagic(mg) => write!(
                f,
                "bad leak magic {:x} expected {:x}",
                mg,
                (b'L' as u16) << 8 | b'K' as u16
            ),
        }
    }
}

impl SensorData for RuuviData {
    fn add_metrics(&self, r: &mut Registry, add_labels: &[(String, String)]) {
        let mut base_labels = Vec::from(add_labels);
        base_labels.push(("device_type".to_owned(), "ruuvi".to_owned()));
        base_labels.push(("hwaddr".to_string(), self.hwaddr.to_string()));

        let temp_c = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register("temperature_C", "Temperature", temp_c.clone());
        temp_c.get_or_create(&base_labels).set(self.temperature_c);

        let rel_hum = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register(
            "relative_humidity_pct",
            "Relative humidity",
            rel_hum.clone(),
        );
        rel_hum.get_or_create(&base_labels).set(self.rel_humidity);

        let battery_voltage = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register(
            "battery_voltage_V",
            "Battery voltage",
            battery_voltage.clone(),
        );
        battery_voltage
            .get_or_create(&base_labels)
            .set(self.voltage);

        let pressure = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register("pressure_Pa", "Air pressure", pressure.clone());
        pressure
            .get_or_create(&base_labels)
            .set(self.pressure as f64);

        let acc = Family::<Vec<(String, String)>, Gauge>::default();
        r.register("acc", "accelrometer", acc.clone());
        let mut acc_x_labels = base_labels.clone();
        acc_x_labels.push(("axis".to_owned(), "x".to_owned()));
        let mut acc_y_labels = base_labels.clone();
        acc_y_labels.push(("axis".to_owned(), "y".to_owned()));
        let mut acc_z_labels = base_labels.clone();
        acc_z_labels.push(("axis".to_owned(), "z".to_owned()));
        acc.get_or_create(&acc_x_labels).set(self.acc_x.into());
        acc.get_or_create(&acc_y_labels).set(self.acc_y.into());
        acc.get_or_create(&acc_z_labels).set(self.acc_z.into());

        let seq = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register("sequence", "sequence number", seq.clone());
        seq.get_or_create(&base_labels).set(self.seq as f64);
        let mvct = Family::<Vec<(String, String)>, Gauge<f64, AtomicU64>>::default();
        r.register("move_count", "number of move events", mvct.clone());
        mvct.get_or_create(&base_labels).set(self.movecount as f64);
    }
}

impl RuuviData {
    pub fn parse(d: &[u8]) -> Result<Self, ParseError> {
        if d.len() < 24 {
            return Err(ParseError::BadLength);
        }
        if d[0] != 5 {
            return Err(ParseError::RuuviVersion(d[0]));
        }
        let raw_temp = i16_at!(d, 1);
        let raw_humidity = u16_at!(d, 3);
        let raw_pressure = u16_at!(d, 5);
        let acc_x = i16_at!(d, 7);
        let acc_y = i16_at!(d, 9);
        let acc_z = i16_at!(d, 11);
        let raw_voltage = u16_at!(d, 13);
        let movecount = d[15];
        let seq = u16_at!(d, 16);
        let hwaddr = bluer::Address::new(d[18..24].try_into().unwrap());

        let temperature_c = raw_temp as f64 * 0.005;
        let rel_humidity = raw_humidity as f64 * 0.0025;
        let pressure: u64 = raw_pressure as u64 + 50_000;
        let voltage = 1.6 + ((raw_voltage >> 5) as f64) / 1000f64;

        Ok(RuuviData {
            temperature_c,
            rel_humidity,
            pressure,
            voltage,
            movecount,
            acc_x,
            acc_y,
            acc_z,
            seq,
            hwaddr,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use prometheus_client::encoding::text::encode;
    use prometheus_client::registry::Registry;
    use similar_asserts::assert_eq as sim_assert_eq;
    #[test]
    fn test_bad_water() {
        assert!(WaterSensor::parse(&[]).is_err());
        assert!(WaterSensor::parse(&[0xff, 0xff, 0x01, 0x02, 0x03, 0x04, 0x05]).is_err());
        assert!(WaterSensor::parse(&[b'L', b'K']).is_err());
    }
    #[test]
    fn test_good_water() {
        let tcs = [(
            [b'L', b'K', 0x01, 0x02, 0x00, 0x00, 0x00, 0x00],
            WaterSensor {
                flags: 0x01,
                voltages: vec![0,0],
                seq: 0x02,
            },
        ),
        (
            [b'L', b'K', 0x00, 0xff, 0xff, 0xff, 0xff, 0xff],
            WaterSensor {
                flags: 0x00,
                voltages: vec![-1,-1],
                seq: 255,
            },    
        ),
        (
            [0x4c, 0x4b, 0x01, 0x4a, 0x0d, 0xb0, 0x1a, 0xa1],
            WaterSensor{
                flags: 0x01,
                voltages: vec![0x0db0, 0x1aa1],
                seq: 0x4a,
            }

        )];
        for (bs, res) in tcs {
            assert_eq!(WaterSensor::parse(&bs), Ok(res));
        }
    }
    #[test]
    fn test_failures() {
        let badv: Vec<u8> = vec![
            10, 21, 244, 75, 30, 198, 122, 0, 28, 255, 240, 3, 224, 175, 86, 26, 235, 224, 224, 8,
            51, 44, 56, 209,
        ];
        assert_eq!(RuuviData::parse(&badv), Err(ParseError::RuuviVersion(10)));
        let badlen: Vec<u8> = Vec::new();
        assert_eq!(RuuviData::parse(&badlen), Err(ParseError::BadLength));
    }
    #[test]
    fn test_ruuvi_good_data() {
        let testv: Vec<u8> = vec![
            5, 21, 244, 75, 30, 198, 122, 0, 28, 255, 240, 3, 224, 175, 86, 26, 235, 224, 224, 8,
            51, 44, 56, 209,
        ];
        let res = RuuviData::parse(&testv).unwrap();
        assert_eq!(res.temperature_c, 28.1);
        assert_eq!(res.rel_humidity, 48.075);
        assert_eq!(res.pressure, 100810);
        assert_eq!(res.acc_x, 28);
        assert_eq!(res.acc_y, -16);
        assert_eq!(res.acc_z, 992);
        assert_eq!(res.voltage, 3.002);
        assert_eq!(res.movecount, 26);
        assert_eq!(res.seq, 60384);
        assert_eq!(
            res.hwaddr,
            bluer::Address::new([0xe0, 0x08, 0x33, 0x2c, 0x38, 0xd1])
        );
    }
    #[test]
    fn test_encode_ruuvi() {
        let mut registry = <Registry>::default();
        let ruuvi = RuuviData {
            temperature_c: 20.0,
            rel_humidity: 50.0,
            pressure: 10000,
            voltage: 3.4,
            movecount: 10,
            acc_x: 800,
            acc_y: 700,
            acc_z: 600,
            seq: 1024,
            hwaddr: bluer::Address::new([0x01, 0x02, 0x03, 0x04, 0x05, 0x06]),
        };
        let labels = [("name".to_owned(), "testdevice".to_owned())];
        ruuvi.add_metrics(&mut registry, &labels);
        let mut buffer = String::new();
        encode(&mut buffer, &registry).unwrap();
        let expected = "# HELP temperature_C Temperature.\n".to_owned()
            + "# TYPE temperature_C gauge\n"
            + "temperature_C{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\"} 20.0\n"
            + "# HELP relative_humidity_pct Relative humidity.\n"
            + "# TYPE relative_humidity_pct gauge\n"
            + "relative_humidity_pct{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\"} 50.0\n"
            + "# HELP battery_voltage_V Battery voltage.\n"
            + "# TYPE battery_voltage_V gauge\n"
            + "battery_voltage_V{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\"} 3.4\n"
            + "# HELP pressure_Pa Air pressure.\n"
            + "# TYPE pressure_Pa gauge\n"
            + "pressure_Pa{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\"} 10000.0\n"
            + "# HELP acc accelrometer.\n"
            + "# TYPE acc gauge\n"
            + "acc{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\",axis=\"z\"} 600\n"
            + "acc{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\",axis=\"y\"} 700\n"
            + "acc{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\",axis=\"x\"} 800\n"
            + "# HELP sequence sequence number.\n"
            + "# TYPE sequence gauge\n"
            + "sequence{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\"} 1024.0\n"
            + "# HELP move_count number of move events.\n"
            + "# TYPE move_count gauge\n"
            + "move_count{name=\"testdevice\",device_type=\"ruuvi\",hwaddr=\"01:02:03:04:05:06\"} 10.0\n"
            + "# EOF\n";
        let mut el: Vec<String> = expected.split('\n').map(|s| s.to_owned()).collect();
        let mut gl: Vec<String> = buffer.split('\n').map(|s| s.to_owned()).collect();
        let expected_noacc: Vec<String> = el
            .clone()
            .into_iter()
            .filter(|i| !i.starts_with("acc{"))
            .collect();
        let got_noacc: Vec<String> = gl
            .clone()
            .into_iter()
            .filter(|i| !i.starts_with("acc{"))
            .collect();
        sim_assert_eq!(expected_noacc, got_noacc);
        el.sort();
        gl.sort();
        sim_assert_eq!(el, gl);
    }
}
