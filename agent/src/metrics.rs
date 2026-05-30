use serde::Serialize;
use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, RefreshKind, System};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("failed to retrieve hostname")]
    Hostname(#[from] std::io::Error),
}

#[derive(Debug, Serialize)]
pub struct MetricsSnapshot {
    /// RFC 3339 timestamp of when this snapshot was collected
    pub collected_at: String,
    pub host: String,
    /// CPU utilisation averaged across all logical cores, 0.0–100.0
    pub cpu_pct: f64,
    /// Resident memory as a percentage of total physical RAM, 0.0–100.0
    pub memory_pct: f64,
    /// Cumulative bytes read across all processes since boot
    pub disk_read_bytes: u64,
    /// Cumulative bytes written across all processes since boot
    pub disk_write_bytes: u64,
}

pub struct Collector {
    system: System,
    host: String,
}

impl Collector {
    /// Creates a new collector, performing the first sysinfo refresh.
    ///
    /// `System` must be reused across ticks — recreating it each call resets
    /// the CPU usage delta calculation that sysinfo uses internally, which
    /// always produces 0% on the first sample after creation.
    pub fn new() -> Result<Self, MetricsError> {
        let mut system = System::new_with_specifics(
            RefreshKind::new()
                .with_cpu(sysinfo::CpuRefreshKind::everything())
                .with_memory(MemoryRefreshKind::everything())
                .with_processes(ProcessRefreshKind::everything()),
        );
        system.refresh_all();

        let host = hostname::get()
            .map_err(MetricsError::Hostname)?
            .to_string_lossy()
            .into_owned();

        Ok(Self { system, host })
    }

    /// Collects a snapshot. Must be called at least twice before CPU% is meaningful
    /// because sysinfo computes it as a delta between the previous and current refresh.
    pub fn collect(&mut self) -> Result<MetricsSnapshot, MetricsError> {
        self.system.refresh_all();

        let cpu_pct = self.system.global_cpu_info().cpu_usage() as f64;

        let total_mem = self.system.total_memory();
        let used_mem = self.system.used_memory();
        let memory_pct = if total_mem > 0 {
            (used_mem as f64 / total_mem as f64) * 100.0
        } else {
            0.0
        };

        // Sum cumulative disk I/O across all processes — sysinfo 0.30 exposes
        // per-process disk_usage() with total_read_bytes / total_written_bytes.
        let (disk_read_bytes, disk_write_bytes) =
            self.system
                .processes()
                .values()
                .fold((0u64, 0u64), |acc, p| {
                    let usage = p.disk_usage();
                    (
                        acc.0 + usage.total_read_bytes,
                        acc.1 + usage.total_written_bytes,
                    )
                });

        let collected_at = chrono::Utc::now().to_rfc3339();

        Ok(MetricsSnapshot {
            collected_at,
            host: self.host.clone(),
            cpu_pct,
            memory_pct,
            disk_read_bytes,
            disk_write_bytes,
        })
    }
}
