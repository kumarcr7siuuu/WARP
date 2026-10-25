use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use crate::metrics::{BenchmarkPhase, BlockState, SharedState};

pub struct BenchmarkSimulator {
    shared_state: Arc<SharedState>,
}

impl BenchmarkSimulator {
    pub fn new(shared_state: Arc<SharedState>) -> Self {
        Self { shared_state }
    }

    pub async fn run_sequential_simulation(&self) {
        let total_bytes = self.shared_state.total_file_bytes.load(Ordering::Relaxed);

        // ==========================================
        // PHASE 1: TCP BASELINE RUN
        // ==========================================
        *self.shared_state.phase.write().await = BenchmarkPhase::Phase1Tcp;
        self.shared_state.reset_phase_metrics();

        let tcp_duration = Duration::from_secs_f64(10.82);
        let sample_interval = Duration::from_millis(50);
        let start_time = tokio::time::Instant::now();

        let mut current_bytes;
        let base_speed_mbps = 550.0;
        let mut tcp_samples = Vec::new();

        while start_time.elapsed() < tcp_duration {
            if !self.shared_state.is_running.load(Ordering::Relaxed) {
                return;
            }

            let elapsed_secs = start_time.elapsed().as_secs_f64();
            let progress = (elapsed_secs / tcp_duration.as_secs_f64()).min(1.0);

            // Simulate TCP CUBIC sawtooth: periodic packet drop causing congestion collapse
            let cycle = (elapsed_secs * 1.8) % 3.0;
            let sawtooth_factor = if cycle > 2.5 {
                // Packet loss detected: Halve window and simulate HoL stall!
                self.shared_state.hol_stalls_ms.fetch_add(45, Ordering::Relaxed);
                0.45
            } else {
                0.70 + (cycle / 2.5) * 0.40
            };

            let current_mbps = (base_speed_mbps * sawtooth_factor) + (fastrand_float() * 30.0 - 15.0);
            *self.shared_state.current_mbps.write().await = current_mbps;
            *self.shared_state.phase_elapsed_secs.write().await = elapsed_secs;

            current_bytes = ((total_bytes as f64) * progress) as u64;
            self.shared_state.bytes_transferred.store(current_bytes, Ordering::Relaxed);

            tcp_samples.push((elapsed_secs, current_mbps));

            sleep(sample_interval).await;
        }

        // Finish Phase 1
        self.shared_state.bytes_transferred.store(total_bytes, Ordering::Relaxed);
        let mut history = self.shared_state.history.write().await;
        history.tcp_samples = tcp_samples;
        history.tcp_total_time = 10.82;
        history.tcp_avg_mbps = 485.2;
        history.tcp_hol_stalls_ms = self.shared_state.hol_stalls_ms.load(Ordering::Relaxed);
        drop(history);

        // ==========================================
        // COOLDOWN PERIOD (2 Seconds)
        // ==========================================
        *self.shared_state.phase.write().await = BenchmarkPhase::Cooldown;
        *self.shared_state.current_mbps.write().await = 0.0;
        sleep(Duration::from_secs(2)).await;

        // ==========================================
        // PHASE 2: WARP CUSTOM UDP PROTOCOL RUN
        // ==========================================
        *self.shared_state.phase.write().await = BenchmarkPhase::Phase2Warp;
        self.shared_state.reset_phase_metrics();

        let warp_duration = Duration::from_secs_f64(5.41);
        let warp_start = tokio::time::Instant::now();
        let mut warp_samples = Vec::new();

        while warp_start.elapsed() < warp_duration {
            if !self.shared_state.is_running.load(Ordering::Relaxed) {
                return;
            }

            let elapsed_secs = warp_start.elapsed().as_secs_f64();
            let progress = (elapsed_secs / warp_duration.as_secs_f64()).min(1.0);

            // WARP stays at continuous line-rate: 935 - 950 MB/s!
            let current_mbps = 942.0 + (fastrand_float() * 16.0 - 8.0);
            *self.shared_state.current_mbps.write().await = current_mbps;
            *self.shared_state.phase_elapsed_secs.write().await = elapsed_secs;

            current_bytes = ((total_bytes as f64) * progress) as u64;
            self.shared_state.bytes_transferred.store(current_bytes, Ordering::Relaxed);

            // Update sliding window blocks
            let total_blocks = SharedState::MATRIX_SIZE;
            let committed_count = (progress * total_blocks as f64) as usize;
            let mut blocks = self.shared_state.buffer_blocks.write().await;

            for i in 0..total_blocks {
                if i < committed_count {
                    blocks[i] = BlockState::Committed;
                } else if i == committed_count {
                    blocks[i] = BlockState::Piggybacked;
                } else if i == committed_count + 1 && i % 8 == 0 {
                    blocks[i] = BlockState::MissingNack;
                } else if i < committed_count + 6 && i < total_blocks {
                    blocks[i] = BlockState::InFlight;
                } else {
                    blocks[i] = BlockState::Empty;
                }
            }
            drop(blocks);

            // Occasional simulated NACK and piggybacked recovery
            if fastrand_float() < 0.15 {
                self.shared_state.nacks_count.fetch_add(1, Ordering::Relaxed);
                self.shared_state.piggybacked_count.fetch_add(1, Ordering::Relaxed);
            }

            warp_samples.push((elapsed_secs, current_mbps));
            sleep(sample_interval).await;
        }

        // Finalize Phase 2
        self.shared_state.bytes_transferred.store(total_bytes, Ordering::Relaxed);
        let mut blocks = self.shared_state.buffer_blocks.write().await;
        for block in blocks.iter_mut() {
            *block = BlockState::Committed;
        }
        drop(blocks);

        let mut history = self.shared_state.history.write().await;
        history.warp_samples = warp_samples;
        history.warp_total_time = 5.41;
        history.warp_avg_mbps = 942.8;
        history.warp_hol_stalls_ms = 0;
        drop(history);

        // ==========================================
        // PHASE 3: FINISHED SCORECARD
        // ==========================================
        *self.shared_state.phase.write().await = BenchmarkPhase::Finished;
    }
}

// Lightweight random helper for simulation jitter without heavyweight rand crate
fn fastrand_float() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    ((nanos % 1000) as f64) / 1000.0
}
