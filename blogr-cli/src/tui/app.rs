use crate::content::{Post, PostManager};
use crate::tui::editor::Editor;
use crate::tui::preview::Preview;
use crate::tui::theme::TuiTheme;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph},
    Frame,
};

pub type AppResult<T> = anyhow::Result<T>;

/// Application state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppMode {
    Normal,
    Insert,
    Preview,
    Help,
}

/// Application focus
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Editor,
    Preview,
}

/// Main application state
pub struct App {
    /// Current running state
    pub running: bool,
    /// Current mode
    pub mode: AppMode,
    /// Current focus
    pub focus: Focus,
    /// The post being edited
    pub post: Post,
    /// Markdown editor
    pub editor: Editor,
    /// Preview pane
    pub preview: Preview,
    /// Current theme
    pub theme: TuiTheme,
    /// Whether the post has been modified
    pub modified: bool,
    /// Status message
    pub status_message: String,
    /// Show help overlay
    pub show_help: bool,
    /// Post manager for saving posts
    post_manager: PostManager,
}

impl App {
    /// Constructs a new instance of [`App`].
    pub fn new(post: Post, theme: TuiTheme, post_manager: PostManager) -> Self {
        let content = post.content.clone();
        let editor = Editor::new(content);
        let preview = Preview::new();

        Self {
            running: true,
            mode: AppMode::Normal,
            focus: Focus::Editor,
            post,
            editor,
            preview,
            theme,
            modified: false,
            status_message: "Press 'i' to enter insert mode, 'q' to quit".to_string(),
            show_help: false,
            post_manager,
        }
    }

    /// Handles the tick event of the terminal.
    pub fn tick(&self) {}

    /// Set running to false to quit the application.
    pub fn quit(&mut self) {
        self.running = false;
    }

    /// Handle key events
    pub fn handle_key_event(&mut self, key: KeyEvent) -> AppResult<()> {
        match self.mode {
            AppMode::Normal => self.handle_normal_mode(key),
            AppMode::Insert => self.handle_insert_mode(key),
            AppMode::Preview => self.handle_preview_mode(key),
            AppMode::Help => self.handle_help_mode(key),
        }
    }

    fn handle_normal_mode(&mut self, key: KeyEvent) -> AppResult<()> {
        match key.code {
            KeyCode::Char('q') => {
                if self.modified {
                    self.status_message =
                        "Press 's' to save, 'Q' to quit without saving".to_string();
                } else {
                    self.quit();
                }
            }
            KeyCode::Char('Q') => self.quit(),
            KeyCode::Char('i') => {
                self.mode = AppMode::Insert;
                self.status_message =
                    "INSERT MODE - Press Esc to return to normal mode".to_string();
            }
            KeyCode::Char('p') => {
                self.mode = AppMode::Preview;
                self.focus = Focus::Preview;
                self.update_preview();
                self.status_message =
                    "PREVIEW MODE - Press Esc to return to normal mode".to_string();
            }
            KeyCode::Char('s') => {
                self.save_post()?;
                self.status_message = "Post saved successfully!".to_string();
            }
            KeyCode::Char('h') | KeyCode::F(1) => {
                self.show_help = true;
                self.mode = AppMode::Help;
            }
            KeyCode::Tab => {
                self.focus = match self.focus {
                    Focus::Editor => Focus::Preview,
                    Focus::Preview => Focus::Editor,
                };
                self.update_preview();
            }
            KeyCode::Char('r') => {
                self.update_preview();
                self.status_message = "Preview refreshed".to_string();
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_insert_mode(&mut self, key: KeyEvent) -> AppResult<()> {
        match key.code {
            KeyCode::Esc => {
                self.mode = AppMode::Normal;
                self.focus = Focus::Editor;
                self.status_message = "NORMAL MODE - Press 'h' for help".to_string();
            }
            _ => {
                if self.editor.handle_key_event(key) {
                    self.modified = true;
                    self.post.content = self.editor.get_content();
                    // Auto-refresh preview in background
                    if matches!(self.focus, Focus::Preview) {
                        self.update_preview();
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_preview_mode(&mut self, key: KeyEvent) -> AppResult<()> {
        match key.code {
            KeyCode::Esc => {
                self.mode = AppMode::Normal;
                self.focus = Focus::Editor;
                self.status_message = "NORMAL MODE - Press 'h' for help".to_string();
            }
            KeyCode::Char('i') => {
                self.mode = AppMode::Insert;
                self.focus = Focus::Editor;
                self.status_message =
                    "INSERT MODE - Press Esc to return to normal mode".to_string();
            }
            _ => {
                self.preview.handle_key_event(key);
            }
        }
        Ok(())
    }

    fn handle_help_mode(&mut self, key: KeyEvent) -> AppResult<()> {
        match key.code {
            KeyCode::Esc | KeyCode::Char('h') | KeyCode::F(1) => {
                self.show_help = false;
                self.mode = AppMode::Normal;
                self.status_message = "NORMAL MODE - Press 'h' for help".to_string();
            }
            _ => {}
        }
        Ok(())
    }

    fn save_post(&mut self) -> AppResult<()> {
        // Update post content
        self.post.content = self.editor.get_content();

        // Save the post using PostManager
        match self.post_manager.save_post(&self.post) {
            Ok(file_path) => {
                self.modified = false;
                self.status_message = format!("Saved to: {}", file_path.display());
            }
            Err(e) => {
                self.status_message = format!("Save failed: {}", e);
            }
        }

        Ok(())
    }

    fn update_preview(&mut self) {
        let content = self.editor.get_content();
        self.preview.update_content(content, &self.theme);
    }

    /// Renders the user interface widgets.
    pub fn render(&mut self, frame: &mut Frame) {
        // Create the main layout
        let main_chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Header
                Constraint::Min(0),    // Main content
                Constraint::Length(3), // Status bar
            ])
            .split(frame.area());

        // Render header
        self.render_header(frame, main_chunks[0]);

        // Render main content
        self.render_main_content(frame, main_chunks[1]);

        // Render status bar
        self.render_status_bar(frame, main_chunks[2]);

        // Render help overlay if needed
        if self.show_help {
            self.render_help_overlay(frame);
        }
    }

    fn render_header(&self, frame: &mut Frame, area: Rect) {
        let post_title = self.post.metadata.title.clone();
        let status_indicator = if self.modified { "*" } else { "" };

        let title = format!("Blogr Editor - {}{}", post_title, status_indicator);

        let header = Paragraph::new(title)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(self.theme.border_style())
                    .title("Blogr TUI Editor")
                    .title_style(self.theme.title_style()),
            )
            .style(self.theme.text_style());

        frame.render_widget(header, area);
    }

    fn render_main_content(&mut self, frame: &mut Frame, area: Rect) {
        // Create layout for editor and preview
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);

        // Render editor
        let editor_block = Block::default()
            .borders(Borders::ALL)
            .title("Editor")
            .title_style(self.theme.title_style())
            .border_style(if matches!(self.focus, Focus::Editor) {
                self.theme.focused_border_style()
            } else {
                self.theme.border_style()
            });

        self.editor
            .render(frame, main_chunks[0], editor_block, &self.theme);

        // Render preview
        let preview_block = Block::default()
            .borders(Borders::ALL)
            .title("Preview")
            .title_style(self.theme.title_style())
            .border_style(if matches!(self.focus, Focus::Preview) {
                self.theme.focused_border_style()
            } else {
                self.theme.border_style()
            });

        self.preview
            .render(frame, main_chunks[1], preview_block, &self.theme);
    }

    fn render_status_bar(&self, frame: &mut Frame, area: Rect) {
        let mode_text = match self.mode {
            AppMode::Normal => "NORMAL",
            AppMode::Insert => "INSERT",
            AppMode::Preview => "PREVIEW",
            AppMode::Help => "HELP",
        };

        let status_line = vec![Line::from(vec![
            Span::styled(
                format!(" {} ", mode_text),
                Style::default()
                    .bg(self.theme.mode_color(self.mode))
                    .fg(Color::Black)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::raw(&self.status_message),
        ])];

        let status_bar = Paragraph::new(status_line)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(self.theme.border_style()),
            )
            .style(self.theme.text_style());

        frame.render_widget(status_bar, area);
    }

    fn render_help_overlay(&self, frame: &mut Frame) {
        let area = frame.area();

        // Create a centered rectangle
        let popup_area = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(20),
                Constraint::Percentage(60),
                Constraint::Percentage(20),
            ])
            .split(area)[1];

        let popup_area = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(20),
                Constraint::Percentage(60),
                Constraint::Percentage(20),
            ])
            .split(popup_area)[1];

        // Clear the area
        frame.render_widget(Clear, popup_area);

        let help_text = vec![
            Line::from(""),
            Line::from("Keyboard Shortcuts"),
            Line::from("=================="),
            Line::from(""),
            Line::from("Normal Mode:"),
            Line::from("  i      - Enter insert mode"),
            Line::from("  p      - Enter preview mode"),
            Line::from("  s      - Save post"),
            Line::from("  q      - Quit (with save prompt)"),
            Line::from("  Q      - Force quit"),
            Line::from("  Tab    - Switch focus"),
            Line::from("  r      - Refresh preview"),
            Line::from("  h/F1   - Show this help"),
            Line::from(""),
            Line::from("Insert Mode:"),
            Line::from("  Esc    - Return to normal mode"),
            Line::from("  Type to edit content"),
            Line::from(""),
            Line::from("Editor Shortcuts (Insert Mode):"),
            Line::from("  Ctrl+K - Delete current line"),
            Line::from("  Ctrl+D - Delete word"),
            Line::from("  Ctrl+U - Delete to line start"),
            Line::from("  Ctrl+A - Move to line start"),
            Line::from("  Ctrl+E - Move to line end"),
            Line::from("  Arrow keys - Navigate"),
            Line::from("  Home/End - Line start/end"),
            Line::from("  PgUp/PgDn - Page up/down"),
            Line::from(""),
            Line::from("Preview Mode:"),
            Line::from("  Esc    - Return to normal mode"),
            Line::from("  i      - Enter insert mode"),
            Line::from(""),
        ];

        let help_popup = Paragraph::new(help_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Help")
                    .title_style(self.theme.title_style())
                    .border_style(self.theme.focused_border_style()),
            )
            .style(self.theme.text_style());

        frame.render_widget(help_popup, popup_area);
    }
}
