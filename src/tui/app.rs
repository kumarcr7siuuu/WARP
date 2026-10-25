use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io::stdout;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::metrics::SharedState;
use crate::tui::ui::render_ui;

pub struct TuiApp {
    shared_state: Arc<SharedState>,
}

impl TuiApp {
    pub fn new(shared_state: Arc<SharedState>) -> Self {
        Self { shared_state }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        enable_raw_mode()?;
        let mut stdout = stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        let tick_rate = Duration::from_millis(33); // ~30 FPS

        while self.shared_state.is_running.load(Ordering::Relaxed) {
            let phase = *self.shared_state.phase.read().await;
            let current_mbps = *self.shared_state.current_mbps.read().await;

            terminal.draw(|f| {
                render_ui(f, &self.shared_state, phase, current_mbps);
            })?;

            if event::poll(tick_rate)? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => {
                            self.shared_state.is_running.store(false, Ordering::Relaxed);
                            break;
                        }
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            self.shared_state.is_running.store(false, Ordering::Relaxed);
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;

        Ok(())
    }
}
