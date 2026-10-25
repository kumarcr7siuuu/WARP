#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::metrics::SharedState;

pub struct TcpSender {
    target_addr: SocketAddr,
    shared_state: Arc<SharedState>,
}

impl TcpSender {
    pub fn new(target_addr: SocketAddr, shared_state: Arc<SharedState>) -> Self {
        Self {
            target_addr,
            shared_state,
        }
    }

    pub async fn send_data(&self, data: &[u8]) -> anyhow::Result<()> {
        let mut stream = TcpStream::connect(self.target_addr).await?;
        let total_bytes = data.len();
        self.shared_state.total_file_bytes.store(total_bytes as u64, Ordering::Relaxed);

        let chunk_size = 32 * 1024; // 32 KB write chunks
        let mut sent_bytes: u64 = 0;
        let start_time = Instant::now();
        let mut last_sample_time = Instant::now();

        for chunk in data.chunks(chunk_size) {
            let write_start = Instant::now();
            stream.write_all(chunk).await?;
            let write_duration = write_start.elapsed();

            // Track potential TCP write buffer blockages / stalls
            if write_duration > Duration::from_millis(50) {
                self.shared_state
                    .hol_stalls_ms
                    .fetch_add(write_duration.as_millis() as u64, Ordering::Relaxed);
            }

            sent_bytes += chunk.len() as u64;
            self.shared_state.bytes_transferred.store(sent_bytes, Ordering::Relaxed);

            if last_sample_time.elapsed() >= Duration::from_millis(50) {
                let elapsed = start_time.elapsed().as_secs_f64();
                let mbps = (sent_bytes as f64 * 8.0) / (elapsed.max(0.001) * 1_000_000.0);
                *self.shared_state.current_mbps.write().await = mbps;
                last_sample_time = Instant::now();
            }
        }

        stream.shutdown().await?;
        Ok(())
    }
}

pub struct TcpReceiver {
    bind_addr: String,
    shared_state: Arc<SharedState>,
}

impl TcpReceiver {
    pub fn new(bind_addr: String, shared_state: Arc<SharedState>) -> Self {
        Self {
            bind_addr,
            shared_state,
        }
    }

    pub async fn receive_stream(&self) -> anyhow::Result<Vec<u8>> {
        let listener = TcpListener::bind(&self.bind_addr).await?;
        let (mut socket, _) = listener.accept().await?;

        let mut received = Vec::new();
        let mut buf = vec![0u8; 64 * 1024];
        let start_time = Instant::now();

        loop {
            let n = socket.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            received.extend_from_slice(&buf[..n]);
            self.shared_state
                .bytes_transferred
                .store(received.len() as u64, Ordering::Relaxed);

            let elapsed = start_time.elapsed().as_secs_f64();
            let mbps = (received.len() as f64 * 8.0) / (elapsed.max(0.001) * 1_000_000.0);
            *self.shared_state.current_mbps.write().await = mbps;
        }

        Ok(received)
    }
}
