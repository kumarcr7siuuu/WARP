use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::time::timeout;

use crate::metrics::SharedState;
use crate::protocol::*;

pub struct WarpSender {
    socket: Arc<UdpSocket>,
    target_addr: SocketAddr,
    shared_state: Arc<SharedState>,
    batch_size: u32,
}

impl WarpSender {
    pub async fn new(
        bind_addr: &str,
        target_addr: SocketAddr,
        shared_state: Arc<SharedState>,
        batch_size: u32,
    ) -> anyhow::Result<Self> {
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
        Ok(Self {
            socket,
            target_addr,
            shared_state,
            batch_size,
        })
    }

    pub async fn send_data(&self, data: &[u8]) -> anyhow::Result<()> {
        let total_bytes = data.len();
        self.shared_state.total_file_bytes.store(total_bytes as u64, Ordering::Relaxed);

        let chunks: Vec<&[u8]> = data.chunks(MAX_PAYLOAD_SIZE).collect();
        let total_packets = chunks.len() as u32;

        let mut packet_store: HashMap<u32, Vec<u8>> = HashMap::new();
        let mut piggyback_queue: VecDeque<u32> = VecDeque::new();
        let mut unconfirmed_batches: HashMap<u32, HashSet<u32>> = HashMap::new();

        // Listen for NACKs in background
        let nack_socket = self.socket.clone();
        let (nack_tx, mut nack_rx) = tokio::sync::mpsc::unbounded_channel::<(u32, Vec<u32>)>();
        let is_running = self.shared_state.is_running.load(Ordering::Relaxed);

        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            while is_running {
                if let Ok((len, _)) = nack_socket.recv_from(&mut buf).await {
                    if let Some((header, payload)) = Header::parse(&buf[..len]) {
                        if header.app_id == DEFAULT_APP_ID && header.is_nack() {
                            let missing = decode_nack_payload(payload);
                            let _ = nack_tx.send((header.sequence, missing));
                        }
                    }
                }
            }
        });

        let mut seq: u32 = 0;
        let mut batch_id: u32 = 1;
        let start_time = Instant::now();
        let mut bytes_sent_acc: u64 = 0;
        let mut last_sample_time = Instant::now();

        while seq < total_packets {
            let batch_end_seq = (seq + self.batch_size).min(total_packets);
            let mut current_batch_seqs = HashSet::new();

            self.shared_state.active_batch.store(batch_id, Ordering::Relaxed);

            for current_seq in seq..batch_end_seq {
                // Check if any NACKs arrived: evict confirmed packets and queue missing sequences
                while let Ok((ack_seq, missing_list)) = nack_rx.try_recv() {
                    // Cumulative pruning to prevent RAM bloat on large transfers
                    let prune_up_to = if let Some(&first_missing) = missing_list.first() {
                        first_missing.saturating_sub(1)
                    } else {
                        ack_seq
                    };
                    packet_store.retain(|&k, _| k > prune_up_to);

                    for missing_seq in missing_list {
                        if !piggyback_queue.contains(&missing_seq) {
                            piggyback_queue.push_back(missing_seq);
                            self.shared_state.nacks_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // Inject piggybacked retransmission if available (Zero Radio Idle Strategy)
                if let Some(retransmit_seq) = piggyback_queue.pop_front() {
                    if let Some(packet_bytes) = packet_store.get(&retransmit_seq) {
                        let mut retransmit_pkt = packet_bytes.clone();
                        if retransmit_pkt.len() >= HEADER_SIZE {
                            retransmit_pkt[2] |= FLAG_PIGGYBACK; // Fix: Explicitly mark FLAG_PIGGYBACK
                        }
                        let _ = self.socket.send_to(&retransmit_pkt, self.target_addr).await;
                        self.shared_state.piggybacked_count.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Build fresh packet
                let is_end = (current_seq == batch_end_seq - 1) || (current_seq == total_packets - 1);
                let is_eof = current_seq == total_packets - 1;

                let mut flags = 0u8;
                if is_end {
                    flags |= FLAG_END_OF_BATCH;
                }
                if is_eof {
                    flags |= FLAG_EOF;
                }

                let header = Header::new(DEFAULT_APP_ID, flags, current_seq);
                let payload = chunks[current_seq as usize];
                let mut packet = vec![0u8; HEADER_SIZE + payload.len()];
                header.encode(&mut packet[..HEADER_SIZE]);
                packet[HEADER_SIZE..].copy_from_slice(payload);

                packet_store.insert(current_seq, packet.clone());
                current_batch_seqs.insert(current_seq);

                // Blast packet to NIC
                self.socket.send_to(&packet, self.target_addr).await?;
                bytes_sent_acc += payload.len() as u64;
                self.shared_state.bytes_transferred.store(bytes_sent_acc, Ordering::Relaxed);

                // Update metrics throughput sample periodically
                if last_sample_time.elapsed() >= Duration::from_millis(50) {
                    let elapsed_secs = start_time.elapsed().as_secs_f64();
                    let mbps = (bytes_sent_acc as f64 * 8.0) / (elapsed_secs.max(0.001) * 1_000_000.0);
                    *self.shared_state.current_mbps.write().await = mbps;
                    last_sample_time = Instant::now();
                }
            }

            unconfirmed_batches.insert(batch_id, current_batch_seqs);
            seq = batch_end_seq;
            batch_id += 1;
        }

        // Drain any remaining piggybacked retransmissions before exiting
        let drain_timeout = Instant::now();
        while !piggyback_queue.is_empty() && drain_timeout.elapsed() < Duration::from_millis(500) {
            if let Some(retransmit_seq) = piggyback_queue.pop_front() {
                if let Some(packet_bytes) = packet_store.get(&retransmit_seq) {
                    let mut retransmit_pkt = packet_bytes.clone();
                    if retransmit_pkt.len() >= HEADER_SIZE {
                        retransmit_pkt[2] |= FLAG_PIGGYBACK;
                    }
                    let _ = self.socket.send_to(&retransmit_pkt, self.target_addr).await;
                    self.shared_state.piggybacked_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        Ok(())
    }
}

pub struct WarpReceiver {
    socket: Arc<UdpSocket>,
    shared_state: Arc<SharedState>,
}

impl WarpReceiver {
    pub async fn new(bind_addr: &str, shared_state: Arc<SharedState>) -> anyhow::Result<Self> {
        let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
        Ok(Self {
            socket,
            shared_state,
        })
    }

    pub async fn receive_stream(&self) -> anyhow::Result<Vec<u8>> {
        let mut buf = vec![0u8; 65535];
        let mut received_packets: HashMap<u32, Vec<u8>> = HashMap::new();
        let mut highest_seq: u32 = 0;
        let mut received_bytes: u64 = 0;
        let mut eof_reached = false;
        let mut last_peer_addr: Option<SocketAddr> = None;
        let start_time = Instant::now();

        while !eof_reached {
            // 20ms Radio Silence Timeout watchdog
            let recv_res = timeout(Duration::from_millis(20), self.socket.recv_from(&mut buf)).await;

            match recv_res {
                Ok(Ok((len, peer_addr))) => {
                    last_peer_addr = Some(peer_addr);
                    if let Some((header, payload)) = Header::parse(&buf[..len]) {
                        if header.app_id != DEFAULT_APP_ID {
                            continue;
                        }

                        if !received_packets.contains_key(&header.sequence) {
                            received_packets.insert(header.sequence, payload.to_vec());
                            received_bytes += payload.len() as u64;
                            self.shared_state.bytes_transferred.store(received_bytes, Ordering::Relaxed);
                        }

                        if header.sequence > highest_seq {
                            highest_seq = header.sequence;
                        }

                        if header.is_piggyback() {
                            self.shared_state.piggybacked_count.fetch_add(1, Ordering::Relaxed);
                        }

                        if header.is_end_of_batch() {
                            // Scan for gaps
                            let mut missing = Vec::new();
                            for s in 0..=highest_seq {
                                if !received_packets.contains_key(&s) {
                                    missing.push(s);
                                }
                            }

                            if !missing.is_empty() {
                                let nack_header = Header::new(DEFAULT_APP_ID, FLAG_NACK, highest_seq);
                                let nack_payload = encode_nack_payload(&missing);
                                let mut nack_pkt = vec![0u8; HEADER_SIZE + nack_payload.len()];
                                nack_header.encode(&mut nack_pkt[..HEADER_SIZE]);
                                nack_pkt[HEADER_SIZE..].copy_from_slice(&nack_payload);

                                let _ = self.socket.send_to(&nack_pkt, peer_addr).await;
                                self.shared_state.nacks_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        if header.is_eof() {
                            eof_reached = true;
                        }
                    }
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    // Watchdog fired: 20ms radio silence (potential lost END_OF_BATCH packet)
                    if let Some(peer_addr) = last_peer_addr {
                        if highest_seq > 0 || !received_packets.is_empty() {
                            let mut missing = Vec::new();
                            for s in 0..=highest_seq {
                                if !received_packets.contains_key(&s) {
                                    missing.push(s);
                                }
                            }

                            if !missing.is_empty() {
                                let nack_header = Header::new(DEFAULT_APP_ID, FLAG_NACK, highest_seq);
                                let nack_payload = encode_nack_payload(&missing);
                                let mut nack_pkt = vec![0u8; HEADER_SIZE + nack_payload.len()];
                                nack_header.encode(&mut nack_pkt[..HEADER_SIZE]);
                                nack_pkt[HEADER_SIZE..].copy_from_slice(&nack_payload);

                                let _ = self.socket.send_to(&nack_pkt, peer_addr).await;
                                self.shared_state.nacks_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        }

        let total_secs = start_time.elapsed().as_secs_f64();
        let avg_mbps = (received_bytes as f64 * 8.0) / (total_secs.max(0.001) * 1_000_000.0);
        *self.shared_state.current_mbps.write().await = avg_mbps;

        // Reassemble in sequence
        let mut assembled = Vec::new();
        for s in 0..=highest_seq {
            if let Some(pkt) = received_packets.get(&s) {
                assembled.extend_from_slice(pkt);
            }
        }

        Ok(assembled)
    }
}
