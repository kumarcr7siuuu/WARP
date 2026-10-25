"""
WARP (Wireless Activation Ring Protocol) - Lossy Channel Simulator
Simulates Wi-Fi interference:
- Random packet loss
- Burst drops
- Dropped NACKs
- Network latency/jitter
Used for verification and benchmarking without needing physical Wi-Fi degradation.
"""

import socket
import asyncio
import random
from typing import Optional
from .protocol import WarpHeader, HEADER_SIZE


class SimulatedLossyChannel:
    def __init__(
        self,
        listen_host: str = "127.0.0.1",
        listen_port: int = 9998,
        target_host: str = "127.0.0.1",
        target_port: int = 9999,
        packet_loss_rate: float = 0.05,     # 5% data packet drop
        nack_loss_rate: float = 0.10,       # 10% NACK drop (test dropped NACK recovery)
        burst_loss_chance: float = 0.01,    # 1% chance of a burst drop
        burst_loss_length: int = 4,         # Drop 4 packets in a row
    ):
        self.listen_addr = (listen_host, listen_port)
        self.target_addr = (target_host, target_port)
        self.client_addr: Optional[tuple] = None

        self.packet_loss_rate = packet_loss_rate
        self.nack_loss_rate = nack_loss_rate
        self.burst_loss_chance = burst_loss_chance
        self.burst_loss_length = burst_loss_length
        self.current_burst_countdown = 0

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(self.listen_addr)
        self.sock.setblocking(False)
        self.is_running = False

        self.stats = {
            "forward_sent": 0,
            "forward_dropped": 0,
            "reverse_sent": 0,
            "reverse_dropped": 0,
        }

    async def run(self):
        self.is_running = True
        loop = asyncio.get_running_loop()

        while self.is_running:
            try:
                data, addr = await loop.sock_recvfrom(self.sock, 65535)
                if len(data) < HEADER_SIZE:
                    continue

                header = WarpHeader.unpack(data)

                if addr == self.target_addr:
                    # Reverse path: from Receiver back to Sender (NACKs)
                    if self.client_addr:
                        if header.is_nack and random.random() < self.nack_loss_rate:
                            # Drop NACK!
                            self.stats["reverse_dropped"] += 1
                            continue

                        await loop.sock_sendto(self.sock, data, self.client_addr)
                        self.stats["reverse_sent"] += 1
                else:
                    # Forward path: from Sender to Receiver (Data)
                    self.client_addr = addr

                    # Check for burst drop
                    if self.current_burst_countdown > 0:
                        self.current_burst_countdown -= 1
                        self.stats["forward_dropped"] += 1
                        continue

                    if random.random() < self.burst_loss_chance:
                        self.current_burst_countdown = self.burst_loss_length - 1
                        self.stats["forward_dropped"] += 1
                        continue

                    # Check for normal random drop
                    # (Don't drop SYN_CTRL handshake packets in simulator so test initializes reliably)
                    if not header.is_syn_ctrl and random.random() < self.packet_loss_rate:
                        self.stats["forward_dropped"] += 1
                        continue

                    await loop.sock_sendto(self.sock, data, self.target_addr)
                    self.stats["forward_sent"] += 1

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.001)

    async def stop(self):
        self.is_running = False
        # Send a tiny ping to self to unblock sock_recvfrom cleanly
        try:
            wake_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            wake_sock.sendto(b"", self.listen_addr)
            wake_sock.close()
        except Exception:
            pass
        await asyncio.sleep(0.01)
        self.sock.close()
