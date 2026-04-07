//! Interactive TUI for Routines — browse, run, and monitor routines.

use std::collections::HashMap;
use std::io;
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Terminal;

use routines_core::parser::Routine;
use routines_core::resolve::resolve_routine_path;
use routines_protocol::DaemonClient;
use routines_protocol::types::{RunSnapshot, RunState};

use crate::routines_dir;

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

struct App {
    /// All discovered routines: (display_name, file_path)
    routines: Vec<(String, std::path::PathBuf)>,
    routine_state: ListState,

    /// Active runs from daemon
    runs: Vec<RunSnapshot>,
    run_state: ListState,

    /// Output log lines
    output_lines: Vec<OutputLine>,
    output_scroll: u16,

    /// Current focus panel
    focus: Panel,

    /// Daemon connected?
    daemon_status: DaemonStatus,

    /// Search filter
    search: Option<String>,
    search_input: String,

    /// Should quit
    quit: bool,

    /// Input form state (when launching a routine)
    input_form: Option<InputForm>,
}

struct InputForm {
    routine_name: String,
    #[allow(dead_code)]
    routine: Routine,
    fields: Vec<InputField>,
    cursor: usize,
}

struct InputField {
    name: String,
    description: Option<String>,
    required: bool,
    value: String,
}

#[derive(Clone)]
struct OutputLine {
    style: Style,
    text: String,
}

#[derive(PartialEq)]
enum Panel {
    Library,
    Runs,
    Output,
}

#[derive(PartialEq)]
enum DaemonStatus {
    Connected,
    Disconnected,
}

impl App {
    fn new() -> Self {
        let mut app = App {
            routines: Vec::new(),
            routine_state: ListState::default(),
            runs: Vec::new(),
            run_state: ListState::default(),
            output_lines: Vec::new(),
            output_scroll: 0,
            focus: Panel::Library,
            daemon_status: DaemonStatus::Disconnected,
            search: None,
            search_input: String::new(),
            quit: false,
            input_form: None,
        };
        app.load_routines();
        if !app.routines.is_empty() {
            app.routine_state.select(Some(0));
        }
        app
    }

    fn load_routines(&mut self) {
        let rdir = routines_dir();
        let hub = rdir.join("hub");
        self.routines.clear();
        if hub.exists() {
            self.collect_routines(&hub, "");
        }
        // Registries
        let reg_dir = rdir.join("registries");
        if reg_dir.exists()
            && let Ok(dirs) = std::fs::read_dir(&reg_dir)
        {
            for entry in dirs.flatten() {
                if entry.path().is_dir() {
                    let reg_name = entry.file_name().to_string_lossy().to_string();
                    self.collect_routines_registry(&entry.path(), "", &reg_name);
                }
            }
        }
        self.routines.sort_by(|a, b| a.0.cmp(&b.0));
    }

    fn collect_routines(&mut self, dir: &std::path::Path, prefix: &str) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        let mut entries: Vec<_> = entries.flatten().collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_dir() {
                let ns = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}:{name}")
                };
                self.collect_routines(&path, &ns);
            } else if name.ends_with(".yml") || name.ends_with(".yaml") {
                let stem = name.trim_end_matches(".yml").trim_end_matches(".yaml");
                let full = if prefix.is_empty() {
                    stem.to_string()
                } else {
                    format!("{prefix}:{stem}")
                };
                self.routines.push((full, path));
            }
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn collect_routines_registry(
        &mut self,
        dir: &std::path::Path,
        prefix: &str,
        reg_name: &str,
    ) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        let mut entries: Vec<_> = entries.flatten().collect();
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_dir() && name != ".git" {
                let ns = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };
                self.collect_routines_registry(&path, &ns, reg_name);
            } else if name.ends_with(".yml") || name.ends_with(".yaml") {
                let stem = name.trim_end_matches(".yml").trim_end_matches(".yaml");
                let full_path = if prefix.is_empty() {
                    format!("@{reg_name}/{stem}")
                } else {
                    format!("@{reg_name}/{prefix}/{stem}")
                };
                self.routines.push((full_path, path));
            }
        }
    }

    fn filtered_routines(&self) -> Vec<(usize, &(String, std::path::PathBuf))> {
        self.routines
            .iter()
            .enumerate()
            .filter(|(_, (name, _))| {
                if let Some(ref q) = self.search {
                    name.to_lowercase().contains(&q.to_lowercase())
                } else {
                    true
                }
            })
            .collect()
    }

    fn selected_routine_name(&self) -> Option<&str> {
        let filtered = self.filtered_routines();
        let idx = self.routine_state.selected()?;
        filtered.get(idx).map(|(_, (name, _))| name.as_str())
    }

    fn push_output(&mut self, style: Style, text: String) {
        self.output_lines.push(OutputLine { style, text });
        // Auto-scroll to bottom
        if self.output_lines.len() > 500 {
            self.output_lines.drain(0..100);
        }
    }

    fn start_run(&mut self, name: &str) {
        // Try to parse the routine to check for inputs
        let rdir = routines_dir();
        let path = resolve_routine_path(name, &rdir);
        match Routine::from_file(&path) {
            Ok(routine) => {
                let required_inputs: Vec<_> = routine
                    .inputs
                    .iter()
                    .filter(|i| i.required && i.default.is_none())
                    .collect();

                if required_inputs.is_empty() {
                    // No required inputs — run directly
                    self.submit_run(name, HashMap::new());
                } else {
                    // Show input form
                    let fields = routine
                        .inputs
                        .iter()
                        .map(|i| InputField {
                            name: i.name.clone(),
                            description: i.description.clone(),
                            required: i.required,
                            value: i.default.clone().unwrap_or_default(),
                        })
                        .collect();
                    self.input_form = Some(InputForm {
                        routine_name: name.to_string(),
                        routine,
                        fields,
                        cursor: 0,
                    });
                }
            }
            Err(e) => {
                self.push_output(
                    Style::default().fg(Color::Red),
                    format!("Parse error: {e}"),
                );
            }
        }
    }

    fn submit_run(&mut self, name: &str, inputs: HashMap<String, String>) {
        self.push_output(
            Style::default().fg(Color::Cyan),
            format!("▶ Submitting {name}..."),
        );

        let name = name.to_string();
        let rt = tokio::runtime::Runtime::new().unwrap();
        match rt.block_on(async {
            let mut client = DaemonClient::connect().await?;
            client.submit(&name, inputs).await
        }) {
            Ok(run_id) => {
                self.push_output(
                    Style::default().fg(Color::Green),
                    format!("Submitted: {}", &run_id[..8]),
                );
                self.daemon_status = DaemonStatus::Connected;
            }
            Err(e) => {
                self.push_output(
                    Style::default().fg(Color::Red),
                    format!("Submit failed: {e}"),
                );
            }
        }
    }

    fn refresh_runs(&mut self) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        match rt.block_on(async {
            let client = DaemonClient::try_connect().await;
            match client {
                Ok(Some(mut c)) => {
                    let runs = c.list_active().await.unwrap_or_default();
                    Ok((true, runs))
                }
                _ => Ok::<_, String>((false, Vec::new())),
            }
        }) {
            Ok((connected, runs)) => {
                self.daemon_status = if connected {
                    DaemonStatus::Connected
                } else {
                    DaemonStatus::Disconnected
                };
                self.runs = runs;
            }
            Err(_) => {
                self.daemon_status = DaemonStatus::Disconnected;
                self.runs.clear();
            }
        }
    }

    fn submit_input_form(&mut self) {
        if let Some(form) = self.input_form.take() {
            let inputs: HashMap<String, String> = form
                .fields
                .into_iter()
                .filter(|f| !f.value.is_empty())
                .map(|f| (f.name, f.value))
                .collect();
            self.submit_run(&form.routine_name, inputs);
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn cmd_tui() -> routines_core::error::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();
    app.refresh_runs();

    let mut tick_count = 0u32;

    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        // Poll for events with 250ms timeout
        if event::poll(Duration::from_millis(250))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            handle_key(&mut app, key.code, key.modifiers);
        }

        // Periodic refresh
        tick_count += 1;
        if tick_count.is_multiple_of(8) {
            // Every ~2 seconds
            app.refresh_runs();
        }

        if app.quit {
            break;
        }
    }

    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Key handling
// ---------------------------------------------------------------------------

fn handle_key(app: &mut App, code: KeyCode, modifiers: KeyModifiers) {
    // Input form mode
    if let Some(ref mut form) = app.input_form {
        match code {
            KeyCode::Esc => {
                app.input_form = None;
            }
            KeyCode::Enter => {
                if form.cursor >= form.fields.len() {
                    // Submit button focused
                    app.submit_input_form();
                } else {
                    form.cursor += 1;
                }
            }
            KeyCode::Tab => {
                form.cursor = (form.cursor + 1).min(form.fields.len());
            }
            KeyCode::BackTab => {
                form.cursor = form.cursor.saturating_sub(1);
            }
            KeyCode::Char(c) => {
                if form.cursor < form.fields.len() {
                    form.fields[form.cursor].value.push(c);
                }
            }
            KeyCode::Backspace => {
                if form.cursor < form.fields.len() {
                    form.fields[form.cursor].value.pop();
                }
            }
            _ => {}
        }
        return;
    }

    // Search mode
    if app.search.is_some() {
        match code {
            KeyCode::Esc => {
                app.search = None;
                app.search_input.clear();
            }
            KeyCode::Enter => {
                app.search = if app.search_input.is_empty() {
                    None
                } else {
                    Some(app.search_input.clone())
                };
                // Reset selection
                let filtered = app.filtered_routines();
                if !filtered.is_empty() {
                    app.routine_state.select(Some(0));
                }
            }
            KeyCode::Char(c) => {
                app.search_input.push(c);
                app.search = Some(app.search_input.clone());
                app.routine_state.select(Some(0));
            }
            KeyCode::Backspace => {
                app.search_input.pop();
                app.search = if app.search_input.is_empty() {
                    None
                } else {
                    Some(app.search_input.clone())
                };
                app.routine_state.select(Some(0));
            }
            _ => {}
        }
        return;
    }

    // Normal mode
    match code {
        KeyCode::Char('q') | KeyCode::Char('c')
            if modifiers.contains(KeyModifiers::CONTROL) || code == KeyCode::Char('q') =>
        {
            app.quit = true;
        }
        KeyCode::Tab => {
            app.focus = match app.focus {
                Panel::Library => Panel::Runs,
                Panel::Runs => Panel::Output,
                Panel::Output => Panel::Library,
            };
        }
        KeyCode::BackTab => {
            app.focus = match app.focus {
                Panel::Library => Panel::Output,
                Panel::Runs => Panel::Library,
                Panel::Output => Panel::Runs,
            };
        }
        KeyCode::Char('/') => {
            app.search = Some(String::new());
            app.search_input.clear();
            app.focus = Panel::Library;
        }
        KeyCode::Up | KeyCode::Char('k') => match app.focus {
            Panel::Library => {
                let filtered = app.filtered_routines();
                if !filtered.is_empty() {
                    let i = app.routine_state.selected().unwrap_or(0);
                    let new = if i == 0 { filtered.len() - 1 } else { i - 1 };
                    app.routine_state.select(Some(new));
                }
            }
            Panel::Runs => {
                if !app.runs.is_empty() {
                    let i = app.run_state.selected().unwrap_or(0);
                    let new = if i == 0 { app.runs.len() - 1 } else { i - 1 };
                    app.run_state.select(Some(new));
                }
            }
            Panel::Output => {
                app.output_scroll = app.output_scroll.saturating_sub(3);
            }
        },
        KeyCode::Down | KeyCode::Char('j') => match app.focus {
            Panel::Library => {
                let filtered = app.filtered_routines();
                if !filtered.is_empty() {
                    let i = app.routine_state.selected().unwrap_or(0);
                    let new = if i >= filtered.len() - 1 { 0 } else { i + 1 };
                    app.routine_state.select(Some(new));
                }
            }
            Panel::Runs => {
                if !app.runs.is_empty() {
                    let i = app.run_state.selected().unwrap_or(0);
                    let new = if i >= app.runs.len() - 1 { 0 } else { i + 1 };
                    app.run_state.select(Some(new));
                }
            }
            Panel::Output => {
                app.output_scroll = app.output_scroll.saturating_add(3);
            }
        },
        KeyCode::Enter => {
            if app.focus == Panel::Library
                && let Some(name) = app.selected_routine_name()
            {
                let name = name.to_string();
                app.start_run(&name);
            }
        }
        KeyCode::Char('r') => {
            app.refresh_runs();
            app.load_routines();
        }
        KeyCode::Char('c') => {
            // Cancel selected run
            if app.focus == Panel::Runs
                && let Some(idx) = app.run_state.selected()
                && let Some(run) = app.runs.get(idx)
            {
                let run_id = run.run_id.clone();
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    if let Ok(Some(mut client)) = DaemonClient::try_connect().await {
                        let _ = client.cancel(&run_id).await;
                    }
                });
                app.push_output(
                    Style::default().fg(Color::Yellow),
                    format!("Cancelled: {}", &run_id[..8]),
                );
                app.refresh_runs();
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// UI rendering
// ---------------------------------------------------------------------------

fn ui(f: &mut ratatui::Frame, app: &mut App) {
    // If input form is active, render it as overlay
    if app.input_form.is_some() {
        render_main(f, app);
        render_input_form(f, app);
        return;
    }

    render_main(f, app);
}

fn render_main(f: &mut ratatui::Frame, app: &mut App) {
    let size = f.area();

    // Main layout: top (panels) + bottom (status bar)
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(1)])
        .split(size);

    // Top: left (library) | right (runs + output)
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(outer[0]);

    // Right: runs (top) + output (bottom)
    let right_rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(columns[1]);

    render_library(f, app, columns[0]);
    render_runs(f, app, right_rows[0]);
    render_output(f, app, right_rows[1]);
    render_status_bar(f, app, outer[1]);
}

fn render_library(f: &mut ratatui::Frame, app: &mut App, area: Rect) {
    let focused = app.focus == Panel::Library;
    let border_style = if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let title = if let Some(ref q) = app.search {
        format!(" Library [/{}] ", q)
    } else {
        format!(" Library ({}) ", app.routines.len())
    };

    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(border_style);

    // Collect names to avoid borrow conflict with render_stateful_widget
    let names: Vec<String> = app
        .filtered_routines()
        .iter()
        .map(|(_, (name, _))| name.clone())
        .collect();

    let items: Vec<ListItem> = names
        .iter()
        .map(|name| ListItem::new(Line::from(Span::raw(name.as_str()))))
        .collect();

    let list = List::new(items)
        .block(block)
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");

    f.render_stateful_widget(list, area, &mut app.routine_state);
}

fn render_runs(f: &mut ratatui::Frame, app: &mut App, area: Rect) {
    let focused = app.focus == Panel::Runs;
    let border_style = if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let block = Block::default()
        .title(format!(" Active Runs ({}) ", app.runs.len()))
        .borders(Borders::ALL)
        .border_style(border_style);

    if app.runs.is_empty() {
        let msg = Paragraph::new("  No active runs")
            .style(Style::default().fg(Color::DarkGray))
            .block(block);
        f.render_widget(msg, area);
        return;
    }

    let items: Vec<ListItem> = app
        .runs
        .iter()
        .map(|run| {
            let status_style = match run.status {
                RunState::Running => Style::default().fg(Color::Green),
                RunState::Queued => Style::default().fg(Color::Yellow),
                RunState::Failed => Style::default().fg(Color::Red),
                RunState::Completed => Style::default().fg(Color::Blue),
                _ => Style::default().fg(Color::DarkGray),
            };
            let status = format!("{:?}", run.status);
            let line = Line::from(vec![
                Span::styled(
                    format!("{} ", &run.run_id[..8.min(run.run_id.len())]),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(format!("{:<10}", status), status_style),
                Span::raw(format!(
                    "{} {}/{}",
                    run.routine, run.steps_completed, run.steps_total
                )),
            ]);
            ListItem::new(line)
        })
        .collect();

    let list = List::new(items)
        .block(block)
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▸ ");

    f.render_stateful_widget(list, area, &mut app.run_state);
}

fn render_output(f: &mut ratatui::Frame, app: &mut App, area: Rect) {
    let focused = app.focus == Panel::Output;
    let border_style = if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let block = Block::default()
        .title(" Output ")
        .borders(Borders::ALL)
        .border_style(border_style);

    if app.output_lines.is_empty() {
        let msg = Paragraph::new("  Run a routine to see output here")
            .style(Style::default().fg(Color::DarkGray))
            .block(block);
        f.render_widget(msg, area);
        return;
    }

    let lines: Vec<Line> = app
        .output_lines
        .iter()
        .map(|ol| Line::from(Span::styled(ol.text.clone(), ol.style)))
        .collect();

    let paragraph = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false })
        .scroll((app.output_scroll, 0));

    f.render_widget(paragraph, area);
}

fn render_status_bar(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let daemon_indicator = if app.daemon_status == DaemonStatus::Connected {
        Span::styled("● daemon", Style::default().fg(Color::Green))
    } else {
        Span::styled("○ daemon", Style::default().fg(Color::DarkGray))
    };

    let help = if app.search.is_some() {
        "ESC close  ↵ confirm"
    } else {
        "↵ run  /search  r refresh  c cancel  Tab switch  q quit"
    };

    let line = Line::from(vec![
        Span::styled(
            format!(" {help}"),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw("  "),
        daemon_indicator,
        Span::raw(" "),
    ]);

    let bar = Paragraph::new(line).style(Style::default().bg(Color::Black));
    f.render_widget(bar, area);
}

fn render_input_form(f: &mut ratatui::Frame, app: &App) {
    let Some(ref form) = app.input_form else {
        return;
    };

    let area = f.area();
    let width = (area.width as f32 * 0.6).min(60.0) as u16;
    let height = (form.fields.len() as u16 * 3 + 6).min(area.height - 4);
    let x = (area.width.saturating_sub(width)) / 2;
    let y = (area.height.saturating_sub(height)) / 2;
    let popup = Rect::new(x, y, width, height);

    f.render_widget(Clear, popup);

    let block = Block::default()
        .title(format!(" Run: {} ", form.routine_name))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = block.inner(popup);
    f.render_widget(block, popup);

    let mut lines = Vec::new();
    for (i, field) in form.fields.iter().enumerate() {
        let is_focused = i == form.cursor;
        let label_style = if field.required {
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        };
        let req = if field.required { "*" } else { "" };
        let desc = field
            .description
            .as_deref()
            .map(|d| format!(" ({d})"))
            .unwrap_or_default();

        lines.push(Line::from(vec![
            Span::styled(format!("{}{req}{desc}", field.name), label_style),
        ]));

        let cursor_char = if is_focused { "▏" } else { "" };
        let value_style = if is_focused {
            Style::default().fg(Color::Cyan).bg(Color::DarkGray)
        } else {
            Style::default().fg(Color::White)
        };
        lines.push(Line::from(vec![Span::styled(
            format!("  {}{cursor_char}", field.value),
            value_style,
        )]));
        lines.push(Line::from(""));
    }

    // Submit button
    let submit_focused = form.cursor >= form.fields.len();
    let submit_style = if submit_focused {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Green)
    };
    lines.push(Line::from(Span::styled("  [ Submit ]", submit_style)));

    let para = Paragraph::new(lines).wrap(Wrap { trim: false });
    f.render_widget(para, inner);
}
