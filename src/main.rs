use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;

mod engine;
mod metrics;
mod protocol;
mod tui;

use engine::{BenchmarkSimulator, WarpReceiver, WarpSender};
use metrics::SharedState;
use tui::TuiApp;

#[derive(Parser, Debug)]
#[command(name = "warp", version = "1.0.0", about = "WARP Custom High-Speed UDP Protocol & Benchmark Suite")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the sequential benchmark: TCP Baseline -> Cooldown -> WARP Custom UDP
    Benchmark {
        /// Target machine address (e.g. 192.168.1.188:9000). If omitted, runs simulated demo mode.
        #[arg(short, long)]
        target: Option<SocketAddr>,

        /// Listen port if running as benchmark receiver
        #[arg(short, long)]
        listen: Option<u16>,

        /// Payload size in Gigabytes for the benchmark test
        #[arg(short, long, default_value = "5.0")]
        size_gb: f64,

        /// Force demo/simulation mode for immediate local visualization
        #[arg(short, long, default_value_t = true)]
        demo: bool,
    },

    /// Send a file to a remote machine using the WARP custom UDP protocol
    Send {
        /// Path to the file to send
        file: PathBuf,

        /// Target receiver address (e.g. 192.168.1.188:9000)
        #[arg(short, long)]
        target: SocketAddr,
    },

    /// Receive data from a remote machine using the WARP custom UDP protocol
    Recv {
        /// UDP port to listen on
        #[arg(short, long, default_value_t = 9000)]
        port: u16,

        /// Output path for received file
        #[arg(short, long)]
        out: Option<PathBuf>,
    },
}

fn get_local_ip() -> String {
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                return addr.ip().to_string();
            }
        }
    }
    "127.0.0.1".to_string()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Default to benchmark demo mode if no subcommand is passed
    let command = cli.command.unwrap_or(Commands::Benchmark {
        target: None,
        listen: None,
        size_gb: 5.0,
        demo: true,
    });

    match command {
        Commands::Benchmark { size_gb, .. } => {
            let total_bytes = (size_gb * 1_000_000_000.0) as u64;
            let shared_state = SharedState::new(total_bytes);

            // Spawn benchmark worker thread
            let sim_state = shared_state.clone();
            tokio::spawn(async move {
                let simulator = BenchmarkSimulator::new(sim_state);
                simulator.run_sequential_simulation().await;
            });

            // Run interactive TUI
            let mut app = TuiApp::new(shared_state);
            app.run().await?;
        }

        Commands::Send { file, target } => {
            println!("Reading file {:?}...", file);
            let data = tokio::fs::read(&file).await?;
            let shared_state = SharedState::new(data.len() as u64);

            let sender_state = shared_state.clone();
            tokio::spawn(async move {
                if let Ok(sender) = WarpSender::new("0.0.0.0:0", target, sender_state, 64).await {
                    let _ = sender.send_data(&data).await;
                }
            });

            let mut app = TuiApp::new(shared_state);
            app.run().await?;
        }

        Commands::Recv { port, out } => {
            let local_ip = get_local_ip();
            println!("\n============================================================");
            println!("  [WARP RECEIVER ACTIVE - WAITING FOR SENDER]");
            println!("============================================================");
            println!("  This Machine's IP : {}", local_ip);
            println!("  Listening Port    : {}", port);
            println!("------------------------------------------------------------");
            println!("  Run this command on your sender machine:");
            println!("  cargo run -- send <FILE> --target {}:{}", local_ip, port);
            println!("============================================================\n");

            let shared_state = SharedState::new(1); // placeholder size until headers arrive

            let recv_state = shared_state.clone();
            let recv_handle = tokio::spawn(async move {
                if let Ok(receiver) = WarpReceiver::new(&format!("0.0.0.0:{port}"), recv_state).await {
                    if let Ok(data) = receiver.receive_stream().await {
                        if let Some(out_path) = out {
                            let _ = tokio::fs::write(out_path, data).await;
                        }
                    }
                }
            });

            let mut app = TuiApp::new(shared_state);
            app.run().await?;
            recv_handle.await?;
        }
    }

    Ok(())
}
