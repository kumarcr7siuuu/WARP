use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockState {
    Empty,
    InFlight,
    MissingNack,
    Piggybacked,
    Committed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkPhase {
    Phase1Tcp,
    Cooldown,
    Phase2Warp,
    Finished,
}

#[derive(Debug, Clone, Default)]
pub struct BenchmarkHistory {
    pub tcp_samples: Vec<(f64, f64)>,  // (time_elapsed_secs, throughput_mbps)
    pub warp_samples: Vec<(f64, f64)>, // (time_elapsed_secs, throughput_mbps)
    pub tcp_total_time: f64,
    pub tcp_avg_mbps: f64,
    pub tcp_hol_stalls_ms: u64,
    pub warp_total_time: f64,
    pub warp_avg_mbps: f64,
    pub warp_hol_stalls_ms: u64,
}

pub struct SharedState {
    pub is_running: AtomicBool,
    pub phase: RwLock<BenchmarkPhase>,
    pub phase_elapsed_secs: RwLock<f64>,
    pub total_file_bytes: AtomicU64,

    // Live Metrics for active phase
    pub bytes_transferred: AtomicU64,
    pub current_mbps: RwLock<f64>,
    pub active_batch: AtomicU32,
    pub nacks_count: AtomicU32,
    pub piggybacked_count: AtomicU32,
    pub hol_stalls_ms: AtomicU64,

    // Sliding Window Matrix buffer states (e.g. 70 blocks for terminal visualization)
    pub buffer_blocks: RwLock<Vec<BlockState>>,

    // Benchmark comparison history
    pub history: RwLock<BenchmarkHistory>,
}

impl SharedState {
    pub const MATRIX_SIZE: usize = 70;

    pub fn new(total_bytes: u64) -> Arc<Self> {
        let initial_blocks = vec![BlockState::Empty; Self::MATRIX_SIZE];
        Arc::new(Self {
            is_running: AtomicBool::new(true),
            phase: RwLock::new(BenchmarkPhase::Phase1Tcp),
            phase_elapsed_secs: RwLock::new(0.0),
            total_file_bytes: AtomicU64::new(total_bytes),

            bytes_transferred: AtomicU64::new(0),
            current_mbps: RwLock::new(0.0),
            active_batch: AtomicU32::new(1),
            nacks_count: AtomicU32::new(0),
            piggybacked_count: AtomicU32::new(0),
            hol_stalls_ms: AtomicU64::new(0),

            buffer_blocks: RwLock::new(initial_blocks),
            history: RwLock::new(BenchmarkHistory::default()),
        })
    }

    pub fn reset_phase_metrics(&self) {
        self.bytes_transferred.store(0, Ordering::Relaxed);
        self.active_batch.store(1, Ordering::Relaxed);
        self.nacks_count.store(0, Ordering::Relaxed);
        self.piggybacked_count.store(0, Ordering::Relaxed);
        self.hol_stalls_ms.store(0, Ordering::Relaxed);
    }
}
