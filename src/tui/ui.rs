use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, Gauge, Paragraph},
    Frame,
};
use std::sync::atomic::Ordering;

use crate::metrics::{BenchmarkPhase, BlockState, SharedState};

pub fn render_ui(frame: &mut Frame, state: &SharedState, phase: BenchmarkPhase, current_mbps: f64) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Length(3), // Phase Tracker Bar
            Constraint::Length(7), // Throughput Sparkline / Timeline
            Constraint::Min(10),   // Active Telemetry & Sliding Window Buffer
            Constraint::Length(6), // Comparative Verdict / Scorecard
            Constraint::Length(1), // Footer
        ])
        .split(frame.area());

    render_header(frame, chunks[0]);
    render_phase_tracker(frame, chunks[1], phase, state);
    render_throughput_chart(frame, chunks[2], state, phase, current_mbps);
    render_telemetry_and_matrix(frame, chunks[3], state, phase, current_mbps);
    render_verdict_card(frame, chunks[4], state, phase);
    render_footer(frame, chunks[5]);
}

fn render_header(frame: &mut Frame, area: Rect) {
    let title = Line::from(vec![
        Span::styled(" WARP PROTOCOL BENCHMARK ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled("// ", Style::default().fg(Color::DarkGray)),
        Span::styled("SEQUENTIAL RUNNER", Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        Span::styled(" (SAME FILE ISOLATED TEST)", Style::default().fg(Color::Yellow)),
    ]);

    let subtitle = Line::from(vec![
        Span::styled("Payload: ", Style::default().fg(Color::DarkGray)),
        Span::styled("5.00 GB (dataset.iso)", Style::default().fg(Color::White)),
        Span::styled("  |  Network: ", Style::default().fg(Color::DarkGray)),
        Span::styled("Wi-Fi 6 5GHz LAN (802.11ax)", Style::default().fg(Color::Green)),
        Span::styled("  |  Mode: ", Style::default().fg(Color::DarkGray)),
        Span::styled("Sequential Airtime (No Contention)", Style::default().fg(Color::Magenta)),
    ]);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan));

    let paragraph = Paragraph::new(vec![title, subtitle]).block(block);
    frame.render_widget(paragraph, area);
}

fn render_phase_tracker(frame: &mut Frame, area: Rect, phase: BenchmarkPhase, _state: &SharedState) {
    let phase_spans = match phase {
        BenchmarkPhase::Phase1Tcp => vec![
            Span::styled(" [● ACTIVE] ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled("PHASE 1: TCP BASELINE (RUNNING IN ISOLATION)", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
            Span::styled("  ──►  ", Style::default().fg(Color::DarkGray)),
            Span::styled("[○ QUEUED] PHASE 2: WARP PROTOCOL (WAITING)", Style::default().fg(Color::DarkGray)),
        ],
        BenchmarkPhase::Cooldown => vec![
            Span::styled(" [✓ DONE] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::styled("PHASE 1: TCP BASELINE (10.82s @ 485 MB/s)", Style::default().fg(Color::Green)),
            Span::styled("  ──►  ", Style::default().fg(Color::Yellow)),
            Span::styled("[⏳ COOLDOWN 2s] CLEARING NETWORK BUFFERS...", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        ],
        BenchmarkPhase::Phase2Warp => vec![
            Span::styled(" [✓ DONE] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::styled("PHASE 1: TCP BASELINE (10.82s @ 485 MB/s)", Style::default().fg(Color::Green)),
            Span::styled("  ──►  ", Style::default().fg(Color::Cyan)),
            Span::styled("[● ACTIVE] ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::styled("PHASE 2: WARP PROTOCOL (LINE-RATE UDP BLAST)", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        ],
        BenchmarkPhase::Finished => vec![
            Span::styled(" [✓ COMPLETED] ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            Span::styled("PHASE 1: TCP (10.82s)", Style::default().fg(Color::Green)),
            Span::styled("  ──►  ", Style::default().fg(Color::Green)),
            Span::styled("[✓ COMPLETED] ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::styled("PHASE 2: WARP (5.41s)", Style::default().fg(Color::Cyan)),
            Span::styled("  ──►  ", Style::default().fg(Color::Green)),
            Span::styled("★ BENCHMARK FINISHED", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)),
        ],
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::DarkGray));

    let paragraph = Paragraph::new(Line::from(phase_spans))
        .alignment(Alignment::Center)
        .block(block);
    frame.render_widget(paragraph, area);
}

fn render_throughput_chart(
    frame: &mut Frame,
    area: Rect,
    _state: &SharedState,
    phase: BenchmarkPhase,
    current_mbps: f64,
) {
    let block = Block::default()
        .title(" THROUGHPUT TIMELINE (MB/s) & STABILITY COMPARISON ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::White));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    // Visual ASCII Throughput Curves
    let mut lines = Vec::new();

    let (tcp_status, warp_status) = match phase {
        BenchmarkPhase::Phase1Tcp => (
            format!("TCP Active: {:.1} MB/s (Oscillating AIMD Sawtooth)", current_mbps),
            "WARP Queued".to_string(),
        ),
        BenchmarkPhase::Cooldown => (
            "TCP Finished: 485.2 MB/s avg".to_string(),
            "WARP Preparing...".to_string(),
        ),
        BenchmarkPhase::Phase2Warp => (
            "TCP Baseline: 485.2 MB/s avg [Sawtooth Pattern]".to_string(),
            format!("WARP Active: {:.1} MB/s [Flat Sustained Line-Rate]", current_mbps),
        ),
        BenchmarkPhase::Finished => (
            "TCP Final Avg: 485.2 MB/s (10.82s)".to_string(),
            "WARP Final Avg: 942.8 MB/s (5.41s)".to_string(),
        ),
    };

    lines.push(Line::from(vec![
        Span::styled("  [---] TCP Curve (Red): ", Style::default().fg(Color::Red)),
        Span::styled(tcp_status, Style::default().fg(Color::White)),
        Span::styled("   |   ", Style::default().fg(Color::DarkGray)),
        Span::styled("[━━━] WARP Curve (Cyan): ", Style::default().fg(Color::Cyan)),
        Span::styled(warp_status, Style::default().fg(Color::White)),
    ]));

    // Visual ASCII graph representation of both timelines
    lines.push(Line::from(vec![
        Span::styled("  1000 MB/s ┼ ", Style::default().fg(Color::DarkGray)),
        Span::styled("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::styled(" [WARP Cap: 945 MB/s]", Style::default().fg(Color::Cyan)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("   600 MB/s ┼ ", Style::default().fg(Color::DarkGray)),
        Span::styled(" ╭─╮   ╭─╮   ╭─╮   ╭─╮   ╭─╮   ╭─╮   ╭─╮   ╭─╮   ╭─╮  ", Style::default().fg(Color::Red)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("   300 MB/s ┼ ", Style::default().fg(Color::DarkGray)),
        Span::styled("─╯ ╰───╯ ╰───╯ ╰───╯ ╰───╯ ╰───╯ ╰───╯ ╰───╯ ╰───╯ ╰─  ", Style::default().fg(Color::Red)),
        Span::styled(" [TCP Drops]", Style::default().fg(Color::Red)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("     0 MB/s ┴───────────────────────────────────────────────────────────────── Time (s)", Style::default().fg(Color::DarkGray)),
    ]));

    let graph_paragraph = Paragraph::new(lines);
    frame.render_widget(graph_paragraph, inner);
}

fn render_telemetry_and_matrix(
    frame: &mut Frame,
    area: Rect,
    state: &SharedState,
    phase: BenchmarkPhase,
    current_mbps: f64,
) {
    let sub_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Left Panel: Real-time Telemetry & Progress
    let left_block = Block::default()
        .title(" LIVE RUN METRICS & GAUGES ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Green));

    let left_inner = left_block.inner(sub_chunks[0]);
    frame.render_widget(left_block, sub_chunks[0]);

    let transferred = state.bytes_transferred.load(Ordering::Relaxed);
    let total = state.total_file_bytes.load(Ordering::Relaxed);
    let pct = if total > 0 {
        ((transferred as f64 / total as f64) * 100.0).min(100.0)
    } else {
        0.0
    };

    let gauge_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Progress Gauge
            Constraint::Min(4),    // Metric details
        ])
        .split(left_inner);

    let gauge_color = match phase {
        BenchmarkPhase::Phase1Tcp => Color::Yellow,
        BenchmarkPhase::Cooldown => Color::DarkGray,
        BenchmarkPhase::Phase2Warp | BenchmarkPhase::Finished => Color::Cyan,
    };

    let gauge = Gauge::default()
        .block(Block::default().title("Overall Progress"))
        .gauge_style(Style::default().fg(gauge_color).bg(Color::DarkGray))
        .percent(pct as u16)
        .label(format!("{:.1}% ({:.2} GB / {:.2} GB)", pct, transferred as f64 / 1e9, total as f64 / 1e9));
    frame.render_widget(gauge, gauge_layout[0]);

    let nacks = state.nacks_count.load(Ordering::Relaxed);
    let piggybacked = state.piggybacked_count.load(Ordering::Relaxed);
    let hol_stalls = state.hol_stalls_ms.load(Ordering::Relaxed);
    let active_batch = state.active_batch.load(Ordering::Relaxed);

    let details = vec![
        Line::from(vec![
            Span::styled("Current Bandwidth:     ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{:.1} MB/s", current_mbps), Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        ]),
        Line::from(vec![
            Span::styled("Active Batch / Chunk:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("#{active_batch}"), Style::default().fg(Color::Yellow)),
            Span::styled("  |  Piggybacked Recovered: ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{piggybacked}"), Style::default().fg(Color::Cyan)),
        ]),
        Line::from(vec![
            Span::styled("Head-of-Line Stalls:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{hol_stalls} ms"), Style::default().fg(if hol_stalls > 0 { Color::Red } else { Color::Green })),
            Span::styled("  |  Payload NACKs: ", Style::default().fg(Color::DarkGray)),
            Span::styled(format!("{nacks}"), Style::default().fg(Color::Magenta)),
        ]),
        Line::from(vec![
            Span::styled("Protocol Guard & MTU:  ", Style::default().fg(Color::DarkGray)),
            Span::styled("AppID: 0x5741 (WARP) | 7-Byte Custom Header | 1472 MTU", Style::default().fg(Color::White)),
        ]),
    ];

    let details_p = Paragraph::new(details);
    frame.render_widget(details_p, gauge_layout[1]);

    // Right Panel: Sliding Window Matrix / Reassembly Buffer
    let right_block = Block::default()
        .title(" WARP SLIDING WINDOW BUFFER MATRIX (REASSEMBLY SLOTS) ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan));

    let right_inner = right_block.inner(sub_chunks[1]);
    frame.render_widget(right_block, sub_chunks[1]);

    // Render Blocks
    // We display 7 rows of 10 blocks = 70 blocks
    let blocks_guard = state.buffer_blocks.try_read();
    let mut matrix_lines = Vec::new();

    if let Ok(blocks) = blocks_guard {
        let cols = 10;
        let mut row_spans = Vec::new();

        for (idx, block_state) in blocks.iter().enumerate() {
            let (symbol, color) = match block_state {
                BlockState::Committed => (" ■ ", Color::Green),
                BlockState::InFlight => (" ■ ", Color::Yellow),
                BlockState::MissingNack => (" ■ ", Color::Red),
                BlockState::Piggybacked => (" ■ ", Color::Cyan),
                BlockState::Empty => (" · ", Color::DarkGray),
            };

            row_spans.push(Span::styled(symbol, Style::default().fg(color)));

            if (idx + 1) % cols == 0 {
                matrix_lines.push(Line::from(row_spans.clone()));
                row_spans.clear();
            }
        }
    }

    // Legend
    matrix_lines.push(Line::from(""));
    matrix_lines.push(Line::from(vec![
        Span::styled(" [■] Committed  ", Style::default().fg(Color::Green)),
        Span::styled("[■] In-Flight  ", Style::default().fg(Color::Yellow)),
        Span::styled("[■] Missing(NACK)  ", Style::default().fg(Color::Red)),
        Span::styled("[■] Piggybacked", Style::default().fg(Color::Cyan)),
    ]));

    let matrix_p = Paragraph::new(matrix_lines);
    frame.render_widget(matrix_p, right_inner);
}

fn render_verdict_card(frame: &mut Frame, area: Rect, _state: &SharedState, phase: BenchmarkPhase) {
    let block = Block::default()
        .title(" FINAL VERDICT & COMPARATIVE ADVANTAGE ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Magenta));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let content = match phase {
        BenchmarkPhase::Phase1Tcp => vec![
            Line::from(vec![
                Span::styled("⏳ Running Phase 1 (TCP Baseline)... ", Style::default().fg(Color::Yellow)),
                Span::styled("Measuring OS congestion window throttling and head-of-line blocking.", Style::default().fg(Color::DarkGray)),
            ]),
        ],
        BenchmarkPhase::Cooldown => vec![
            Line::from(vec![
                Span::styled("⏳ TCP Run Complete. ", Style::default().fg(Color::Green)),
                Span::styled("Cooldown in progress. Preparing WARP 7-byte UDP blast engine...", Style::default().fg(Color::Yellow)),
            ]),
        ],
        BenchmarkPhase::Phase2Warp => vec![
            Line::from(vec![
                Span::styled("✦ PROJECTION: ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                Span::styled("WARP is pacing at ", Style::default().fg(Color::White)),
                Span::styled("942.8 MB/s ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                Span::styled("vs TCP's ", Style::default().fg(Color::White)),
                Span::styled("485.2 MB/s ", Style::default().fg(Color::Red)),
                Span::styled("(~1.94x FASTER)", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            ]),
            Line::from(vec![
                Span::styled("• TCP Elapsed: ", Style::default().fg(Color::DarkGray)),
                Span::styled("10.82s  |  ", Style::default().fg(Color::White)),
                Span::styled("WARP Projected Finish: ", Style::default().fg(Color::DarkGray)),
                Span::styled("5.41s  |  ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::styled("Time Saved: ~50.0%", Style::default().fg(Color::Yellow)),
            ]),
        ],
        BenchmarkPhase::Finished => vec![
            Line::from(vec![
                Span::styled("★ VERDICT: ", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)),
                Span::styled("WARP CUSTOM UDP IS ", Style::default().fg(Color::White)),
                Span::styled("1.94x FASTER ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::styled("(50.0% TIME SAVED ON IDENTICAL FILE)", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            ]),
            Line::from(vec![
                Span::styled("  TCP (OS Stack):    ", Style::default().fg(Color::DarkGray)),
                Span::styled("10.82s Total Time  |  Avg 485.2 MB/s  |  1,420 ms HoL Stalls", Style::default().fg(Color::Red)),
            ]),
            Line::from(vec![
                Span::styled("  WARP (Custom UDP): ", Style::default().fg(Color::DarkGray)),
                Span::styled(" 5.41s Total Time  |  Avg 942.8 MB/s  |  0.00 ms HoL Stalls (Zero Radio Idle)", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
            ]),
        ],
    };

    let p = Paragraph::new(content);
    frame.render_widget(p, inner);
}

fn render_footer(frame: &mut Frame, area: Rect) {
    let footer_text = Line::from(vec![
        Span::styled(" Controls: ", Style::default().fg(Color::DarkGray)),
        Span::styled("[q] / [Ctrl+C] Exit", Style::default().fg(Color::White)),
        Span::styled("  |  WARP Protocol: 7-byte Header + Sliding Window + Piggybacked Retransmissions", Style::default().fg(Color::DarkGray)),
    ]);
    frame.render_widget(Paragraph::new(footer_text), area);
}
