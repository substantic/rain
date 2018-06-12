use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use sys_info::mem_info;
use sysconf;

use rain_core::logging::events;
use rain_core::types::GovernorId;

type CpuTimes = Vec<u64>;
type CpuUsage = u8;

type MemUsage = u8;

pub struct Monitor {
    clk_tck: isize, // Result of syscall CLK_TCK
    last_timestamp: DateTime<Utc>,
    last_cpu_time: CpuTimes,
}

impl Monitor {
    pub fn new() -> Self {
        if cfg!(not(target_os = "linux")) {
            warn!("Resource monitoring may not work properly on non-linux systems");
        }
        Monitor {
            clk_tck: sysconf::sysconf(sysconf::SysconfVariable::ScClkTck).unwrap_or_else(|_| {
                warn!("Syscall sysconf(CLK_TCK) failed. Set to default value 100");
                100isize
            }),
            last_timestamp: Utc::now(),
            last_cpu_time: Vec::new(),
        }
    }

    fn get_cpu_time(&self) -> CpuTimes {
        let mut cpu_time_vec = Vec::new();

        if cfg!(target_os = "linux") {
            let f = match File::open("/proc/stat") {
                Ok(f) => f,
                Err(_) => panic!("Cannot open /proc/stat"),
            };
            let f = BufReader::new(&f);
            for l in f.lines() {
                let line = l.unwrap();
                if line.starts_with("cpu") {
                    if line.starts_with("cpu ") {
                        continue; // skip usage of all cpus
                    }
                    let mut parsed_line = line.split_whitespace();
                    let cpu_time = parsed_line.nth(1).unwrap().parse::<u64>().unwrap()
                        + parsed_line.next().unwrap().parse::<u64>().unwrap()
                        + parsed_line.next().unwrap().parse::<u64>().unwrap();
                    cpu_time_vec.push(cpu_time);
                } else {
                    break;
                }
            }
        }
        cpu_time_vec
    }

    fn get_cpu_usage(&self, cpu_time: &CpuTimes, timestamp: DateTime<Utc>) -> Vec<CpuUsage> {
        let mut cpu_usage = Vec::with_capacity(cpu_time.len());
        let time_diff = timestamp.signed_duration_since(self.last_timestamp);
        let mut millis = time_diff.num_milliseconds();
        if millis < 1 {
            warn!(
                "get_cpu_usage() called too often ({}ms since the last measurements)",
                millis
            );
            millis = 1;
        }
        let factor = millis as u64 as u64 * self.clk_tck as u64;
        for (new_time, old_time) in cpu_time.iter().zip(&self.last_cpu_time) {
            let cpu_time_diff = new_time - old_time;
            let usage = (100_000 * cpu_time_diff) / factor;
            cpu_usage.push(usage as CpuUsage);
        }
        cpu_usage
    }

    fn get_mem_usage(&self) -> MemUsage {
        if cfg!(target_os = "linux") || cfg!(target_os = "macos") {
            (match mem_info() {
                Ok(meminfo) => 100 * (meminfo.total - meminfo.avail) / meminfo.total,
                Err(_) => 0,
            }) as MemUsage
        } else {
            0 as MemUsage
        }
    }

    fn get_net_stat(&self) -> HashMap<String, Vec<u64>> {
        let mut net_stat = HashMap::new();
        if cfg!(target_os = "linux") {
            let f = match File::open("/proc/net/dev") {
                Ok(f) => f,
                Err(_) => panic!("Cannot open /proc/net/dev"),
            };
            let f = BufReader::new(&f);
            for l in f.lines() {
                let line = l.unwrap();
                if line.find(":").is_some() {
                    let spl: Vec<&str> = line.split(":").collect();
                    let data: Vec<&str> = spl[1].split_whitespace().collect();
                    net_stat.insert(
                        spl[0].to_string(),
                        vec![
                            data[0].parse::<u64>().unwrap(),
                            data[8].parse::<u64>().unwrap(),
                        ],
                    );
                }
            }
        }
        net_stat
    }

    pub fn build_event(&mut self, governor_id: &GovernorId) -> events::Event {
        let timestamp = Utc::now();
        let cpu_time = self.get_cpu_time();
        let cpu_usage = self.get_cpu_usage(&cpu_time, timestamp);
        let mem_usage = self.get_mem_usage();
        let net_stat = self.get_net_stat();

        self.last_timestamp = timestamp;
        self.last_cpu_time = cpu_time;

        events::Event::Monitoring(events::MonitoringEvent {
            governor: governor_id.clone(),
            cpu_usage: cpu_usage,
            mem_usage: mem_usage,
            net_stat: net_stat,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_usage() {
        let monitor = Monitor::new();
        let mem_usage = monitor.get_mem_usage();
        assert!(mem_usage <= 100);
    }

    #[test]
    fn test_cpu_uasge() {
        let monitor = Monitor::new();
        let cpu_usage = monitor.get_cpu_usage(&(monitor.get_cpu_time()), Utc::now());
        for u in cpu_usage {
            assert!(u <= 100)
        }
    }

    #[test]
    fn test_net_stat() {
        let monitor = Monitor::new();
        let net_stat = monitor.get_net_stat();
        for (_dev, bytes) in net_stat {
            assert!(bytes.len() == 2);
        }
    }
}
