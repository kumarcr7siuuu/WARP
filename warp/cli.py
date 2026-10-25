"""
WARP (Wireless Activation Ring Protocol) - Command Line Interface
Usage:
    python -m warp.cli send <file> --host 192.168.1.50 --port 9999
    python -m warp.cli receive --port 9999 --output-dir ./downloads
    python -m warp.cli benchmark --size-mb 50 --loss 0.05 --nack-loss 0.10
"""

import argparse
import asyncio
import os
import sys
import tempfile
import hashlib
import time

from warp.sender import WarpSender
from warp.receiver import WarpReceiver
from warp.channel import SimulatedLossyChannel

# Ensure UTF-8 output on Windows consoles
if sys.platform == "win32":
    try:
        if sys.stdout.encoding.lower() != "utf-8":
            sys.stdout.reconfigure(encoding="utf-8")
        if sys.stderr.encoding.lower() != "utf-8":
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TransferSpeedColumn
    from rich.panel import Panel
    from rich.table import Table
    HAS_RICH = True
except ImportError:
    HAS_RICH = False


def format_bytes(num: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}"
        num /= 1024.0
    return f"{num:.1f} TB"


def get_local_ip() -> str:
    """Finds the primary local Wi-Fi / LAN IP address of this machine."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


async def run_send(args):
    file_path = args.file
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' does not exist.")
        sys.exit(1)

    file_size = os.path.getsize(file_path)
    print(f"\n[WARP Sender] Starting transfer of '{os.path.basename(file_path)}' ({format_bytes(file_size)})")
    print(f"   Target: {args.host}:{args.port}")
    print(f"   Batch Size: {args.batch_size} packets | Header: 7 Bytes\n")

    sender = WarpSender(
        dest_host=args.host,
        dest_port=args.port,
        batch_size=args.batch_size,
        rate_mbps=args.rate_limit if args.rate_limit > 0 else None,
        piggyback_ratio=args.piggyback_ratio,
    )

    pbar = None
    if HAS_RICH:
        progress = Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TransferSpeedColumn(),
            TimeRemainingColumn()
        )
        task_id = progress.add_task("Transmitting", total=file_size)
        progress.start()

        def progress_cb(current_packet, total_packets):
            approx_bytes = min(file_size, current_packet * sender.payload_size)
            progress.update(task_id, completed=approx_bytes)
    else:
        def progress_cb(current_packet, total_packets):
            pct = (current_packet / total_packets) * 100
            sys.stdout.write(f"\rTransmitting: {pct:5.1f}% ({current_packet}/{total_packets} pkts)")
            sys.stdout.flush()

    try:
        stats = await sender.send_file(file_path, progress_callback=progress_cb)
    except (TimeoutError, ConnectionRefusedError, ConnectionResetError) as e:
        if HAS_RICH and 'progress' in locals():
            progress.stop()
        print(f"\n[!] Connection Failed: {e}\n")
        sys.exit(1)
    finally:
        if HAS_RICH and 'progress' in locals():
            progress.stop()
        else:
            print()

    print("\n" + "=" * 50)
    print("  WARP SEND COMPLETE")
    print("=" * 50)
    print(f"  Duration:              {stats['duration_sec']:.2f} seconds")
    print(f"  Data Sent:             {format_bytes(stats['bytes_sent'])}")
    print(f"  Throughput:            {stats['throughput_mbps']:.2f} Mbps")
    print(f"  Fresh Packets:         {stats['fresh_packets_sent']}")
    print(f"  Piggybacked Packets:   {stats['piggybacked_packets_sent']}")
    print(f"  NACKs Processed:       {stats['nacks_received']}")
    print(f"  Packets Pruned (RAM):  {stats['packets_pruned']}")
    print("=" * 50 + "\n")


async def run_receive(args):
    local_ip = get_local_ip()
    print("\n" + "=" * 68)
    print("  [WARP RECEIVER ACTIVE - WAITING FOR SENDER]")
    print("=" * 68)
    print(f"  This Machine's Wi-Fi IP : {local_ip}")
    print(f"  Listening Port          : {args.port}")
    print(f"  Save Directory          : {os.path.abspath(args.output_dir)}")
    print("-" * 68)
    print("  To send a file from your other machine, run this on Machine 1:")
    print(f'  python -m warp.cli send "<file_path>" --host {local_ip} --port {args.port}')
    print("=" * 68 + "\n")

    receiver = WarpReceiver(
        listen_host=args.host,
        listen_port=args.port,
        output_dir=args.output_dir,
        silence_timeout_ms=args.silence_timeout
    )

    if HAS_RICH:
        progress = Progress(
            TextColumn("[bold green]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TransferSpeedColumn(),
            TimeRemainingColumn()
        )
        task_id = None

        def progress_cb(current_packet, total_packets):
            nonlocal task_id
            if task_id is None:
                progress.start()
                task_id = progress.add_task("Receiving", total=total_packets)
            progress.update(task_id, completed=current_packet)
    else:
        def progress_cb(current_packet, total_packets):
            pct = (current_packet / total_packets) * 100
            sys.stdout.write(f"\rReceiving: {pct:5.1f}% ({current_packet}/{total_packets} pkts)")
            sys.stdout.flush()

    try:
        stats = await receiver.receive_file(progress_callback=progress_cb)
    finally:
        if HAS_RICH and 'progress' in locals() and task_id is not None:
            progress.stop()
        else:
            print()

    print("\n" + "=" * 50)
    print("  WARP RECEIVE COMPLETE")
    print("=" * 50)
    print(f"  Saved File:            {stats['file_path']}")
    print(f"  Total Data:            {format_bytes(stats['bytes_received'])}")
    print(f"  Duration:              {stats['duration_sec']:.2f} seconds")
    print(f"  Throughput:            {stats['throughput_mbps']:.2f} Mbps")
    print(f"  Fresh Packets:         {stats['fresh_packets_received']}")
    print(f"  Piggybacked Packets:   {stats['piggybacked_packets_received']}")
    print(f"  NACKs Emitted:         {stats['nacks_sent']}")
    print(f"  Watchdog Triggers:     {stats['watchdog_triggers']} (Recovered dropped NACKs)")
    status_str = "[OK] PASSED (Bit-for-bit identical)" if stats["verified"] else "[FAIL] CORRUPT"
    print(f"  SHA-256 Checksum:      {status_str}")
    print("=" * 50 + "\n")


async def run_benchmark(args):
    size_mb = args.size_mb
    loss_rate = args.loss
    nack_loss = args.nack_loss

    print("\n" + "=" * 60)
    print(f"  WARP BENCHMARK: {size_mb} MB Transfer Test")
    print(f"  Packet Loss Rate:     {loss_rate * 100:.1f}%")
    print(f"  Dropped NACK Rate:    {nack_loss * 100:.1f}%")
    print(f"  Header Size:          7 Bytes")
    print("=" * 60 + "\n")

    with tempfile.TemporaryDirectory() as tmp_dir:
        src_path = os.path.join(tmp_dir, "bench_input.bin")
        dst_dir = os.path.join(tmp_dir, "output")
        os.makedirs(dst_dir, exist_ok=True)

        # Generate payload
        print(f"[*] Generating {size_mb} MB test payload with crypto-random bytes...")
        hasher = hashlib.sha256()
        with open(src_path, "wb") as f:
            chunk = os.urandom(64 * 1024)
            for _ in range((size_mb * 1024 * 1024) // len(chunk)):
                f.write(chunk)
                hasher.update(chunk)
        expected_digest = hasher.digest()

        recv_port = 10101
        proxy_port = 10102

        # Start simulated lossy channel
        channel = SimulatedLossyChannel(
            listen_host="127.0.0.1",
            listen_port=proxy_port,
            target_host="127.0.0.1",
            target_port=recv_port,
            packet_loss_rate=loss_rate,
            nack_loss_rate=nack_loss,
            burst_loss_chance=0.01 if loss_rate > 0 else 0.0,
        )
        channel_task = asyncio.create_task(channel.run())

        receiver = WarpReceiver(
            listen_host="127.0.0.1",
            listen_port=recv_port,
            output_dir=dst_dir,
            silence_timeout_ms=15.0
        )
        sender = WarpSender(
            dest_host="127.0.0.1",
            dest_port=proxy_port,
            batch_size=args.batch_size,
            piggyback_ratio=2,
        )

        recv_task = asyncio.create_task(receiver.receive_file())
        await asyncio.sleep(0.02)

        print("[*] Transfer in progress...")
        send_stats = await sender.send_file(src_path)
        recv_stats = await recv_task

        await channel.stop()
        channel_task.cancel()
        try:
            await channel_task
        except asyncio.CancelledError:
            pass

        # Verify hash
        with open(recv_stats["file_path"], "rb") as f:
            actual_digest = hashlib.sha256(f.read()).digest()
        verified = (actual_digest == expected_digest)

        print("\n" + "=" * 60)
        print("  BENCHMARK RESULTS")
        print("=" * 60)
        print(f"  File Size:              {size_mb} MB")
        print(f"  Transfer Duration:      {recv_stats['duration_sec']:.2f} s")
        print(f"  Effective Throughput:   {recv_stats['throughput_mbps']:.2f} Mbps ({recv_stats['throughput_mbps']/8:.2f} MB/s)")
        print(f"  Forward Packets Dropped:{channel.stats['forward_dropped']}")
        print(f"  Reverse NACKs Dropped:  {channel.stats['reverse_dropped']}")
        print(f"  Piggybacked Packets:    {recv_stats['piggybacked_packets_received']}")
        print(f"  Watchdog Silence Pings: {recv_stats['watchdog_triggers']}")
        print(f"  Data Integrity (SHA256):{'[OK] VERIFIED 100% BIT-FOR-BIT IDENTICAL' if verified else '[FAIL] CORRUPT'}")
        print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="WARP: Wireless Activation Ring Protocol CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # SEND command
    send_parser = subparsers.add_parser("send", help="Send a file over WARP")
    send_parser.add_argument("file", help="Path of the file to send")
    send_parser.add_argument("--host", default="127.0.0.1", help="Target receiver host/IP (default: 127.0.0.1)")
    send_parser.add_argument("--port", type=int, default=9999, help="Target receiver port (default: 9999)")
    send_parser.add_argument("--batch-size", type=int, default=64, help="Packets per batch (default: 64)")
    send_parser.add_argument("--rate-limit", type=float, default=0.0, help="Rate limit in Mbps (0 = max un-paced)")
    send_parser.add_argument("--piggyback-ratio", type=int, default=3, help="Fresh:Piggybacked ratio (default: 3)")

    # RECEIVE command
    recv_parser = subparsers.add_parser("receive", help="Receive files over WARP")
    recv_parser.add_argument("--host", default="0.0.0.0", help="Listen host/IP (default: 0.0.0.0)")
    recv_parser.add_argument("--port", type=int, default=9999, help="Listen UDP port (default: 9999)")
    recv_parser.add_argument("--output-dir", default="./downloads", help="Output directory (default: ./downloads)")
    recv_parser.add_argument("--silence-timeout", type=float, default=20.0, help="Watchdog silence in ms (default: 20)")

    # BENCHMARK command
    bench_parser = subparsers.add_parser("benchmark", help="Run local benchmark with loss simulation")
    bench_parser.add_argument("--size-mb", type=int, default=10, help="Test payload size in MB (default: 10)")
    bench_parser.add_argument("--loss", type=float, default=0.05, help="Simulated data packet loss rate (default: 0.05)")
    bench_parser.add_argument("--nack-loss", type=float, default=0.10, help="Simulated NACK drop rate (default: 0.10)")
    bench_parser.add_argument("--batch-size", type=int, default=64, help="Batch size (default: 64)")

    args = parser.parse_args()

    if args.command == "send":
        asyncio.run(run_send(args))
    elif args.command == "receive":
        asyncio.run(run_receive(args))
    elif args.command == "benchmark":
        asyncio.run(run_benchmark(args))


if __name__ == "__main__":
    main()
