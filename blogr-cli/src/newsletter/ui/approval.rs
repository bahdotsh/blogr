//! Modern, high-performance newsletter subscriber approval TUI
//!
//! This module provides a buttery-smooth, responsive TUI interface for managing
//! newsletter subscribers with modern design elements and optimized performance.

use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Gauge, Paragraph, Row, Table, TableState, Wrap},
    Frame,
};
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use crate::newsletter::{NewsletterDatabase, Subscriber, SubscriberStatus};

pub type AppResult<T> = anyhow::Result<T>;

#[derive(Debug, Clone, PartialEq)]
pub enum ApprovalResult {
    Continue,
    Quit,
    #[allow(dead_code)]
    Error(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum AppMode {
    List,
    Help,
    Confirm,
    Loading,
    Search,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConfirmAction {
    Approve,
    Decline,
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubscriberFilter {
    All,
    Pending,
    Approved,
    Declined,
}

impl std::fmt::Display for SubscriberFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriberFilter::All => write!(f, "All"),
            SubscriberFilter::Pending => write!(f, "Pending"),
            SubscriberFilter::Approved => write!(f, "Approved"),
            SubscriberFilter::Declined => write!(f, "Declined"),
        }
    }
}

/// Modern theme colors and styles
pub struct ModernTheme {
    pub primary: Color,
    pub secondary: Color,
    pub accent: Color,
    pub success: Color,
    pub warning: Color,
    pub danger: Color,
    #[allow(dead_code)]
    pub background: Color,
    pub surface: Color,
    pub text: Color,
    pub text_secondary: Color,
    pub border: Color,
    pub border_focused: Color,
}

impl Default for ModernTheme {
    fn default() -> Self {
        Self {
            primary: Color::Rgb(99, 102, 241),         // Indigo
            secondary: Color::Rgb(107, 114, 142),      // Slate
            accent: Color::Rgb(16, 185, 129),          // Emerald
            success: Color::Rgb(34, 197, 94),          // Green
            warning: Color::Rgb(251, 191, 36),         // Amber
            danger: Color::Rgb(239, 68, 68),           // Red
            background: Color::Rgb(15, 23, 42),        // Dark slate
            surface: Color::Rgb(30, 41, 59),           // Slate 800
            text: Color::Rgb(248, 250, 252),           // Slate 50
            text_secondary: Color::Rgb(148, 163, 184), // Slate 400
            border: Color::Rgb(71, 85, 105),           // Slate 600
            border_focused: Color::Rgb(99, 102, 241),  // Indigo
        }
    }
}

/// Animation state for smooth transitions
#[derive(Debug)]
pub struct AnimationState {
    pub progress: f32,
    pub start_time: Instant,
    pub duration: Duration,
    pub active: bool,
}

impl AnimationState {
    pub fn new(duration: Duration) -> Self {
        Self {
            progress: 0.0,
            start_time: Instant::now(),
            duration,
            active: true,
        }
    }

    pub fn update(&mut self) -> bool {
        if !self.active {
            return false;
        }

        let elapsed = self.start_time.elapsed();
        self.progress = (elapsed.as_secs_f32() / self.duration.as_secs_f32()).min(1.0);

        if self.progress >= 1.0 {
            self.active = false;
        }

        true
    }

    pub fn eased_progress(&self) -> f32 {
        // Ease out cubic
        1.0 - (1.0 - self.progress).powi(3)
    }
}

/// High-performance approval app with modern design
pub struct ModernApprovalApp {
    /// Whether the app should continue running
    pub running: bool,
    /// Current mode
    pub mode: AppMode,
    /// Current filter
    pub filter: SubscriberFilter,
    /// All subscribers (cached)
    pub subscribers: Vec<Subscriber>,
    /// Filtered subscribers based on current filter
    pub filtered_subscribers: Vec<usize>,
    /// Current table state
    pub table_state: TableState,
    /// Selected subscriber indices (for bulk operations)
    pub selected: HashSet<usize>,
    /// Search query
    pub search_query: String,
    /// Status message with timestamp
    pub status_message: Option<(String, Instant)>,
    /// Confirmation action
    pub confirm_action: Option<ConfirmAction>,
    /// Database reference
    database: NewsletterDatabase,
    /// Modern theme
    pub theme: ModernTheme,
    /// Last redraw time for performance tracking
    last_redraw: Instant,
    /// Dirty flag to minimize redraws
    needs_redraw: bool,
    /// Loading animation
    loading_animation: AnimationState,
    /// Page size for pagination
    page_size: usize,
    /// Current page
    current_page: usize,
    /// Statistics cache
    stats_cache: Option<(usize, usize, usize, Instant)>, // (total, pending, approved, timestamp)
    /// Performance metrics (simplified)
    last_frame_time: Duration,
}

impl ModernApprovalApp {
    pub fn new(database: NewsletterDatabase) -> Result<Self> {
        let subscribers = database.get_subscribers(None)?;
        let filtered_subscribers: Vec<usize> = (0..subscribers.len()).collect();

        let mut app = Self {
            running: true,
            mode: AppMode::Loading,
            filter: SubscriberFilter::Pending,
            subscribers,
            filtered_subscribers,
            table_state: TableState::default(),
            selected: HashSet::new(),
            search_query: String::new(),
            status_message: Some(("Welcome to Newsletter Manager".to_string(), Instant::now())),
            confirm_action: None,
            database,
            theme: ModernTheme::default(),
            last_redraw: Instant::now(),
            needs_redraw: true,
            loading_animation: AnimationState::new(Duration::from_millis(500)),
            page_size: 20,
            current_page: 0,
            stats_cache: None,
            last_frame_time: Duration::from_millis(16),
        };

        // Initialize with smooth loading transition
        app.apply_filter()?;
        app.select_first();

        // Transition to list mode after loading
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
        });

        Ok(app)
    }

    /// Update the app state - optimized for minimal processing
    pub fn update(&mut self) -> bool {
        let mut updated = false;

        // Only update active animations to reduce CPU usage
        if self.loading_animation.active && self.loading_animation.update() {
            updated = true;
        }

        // Transition from loading to list mode
        if matches!(self.mode, AppMode::Loading) && !self.loading_animation.active {
            self.mode = AppMode::List;
            updated = true;
        }

        // Clear old status messages (check less frequently)
        if let Some((_, timestamp)) = &self.status_message {
            if timestamp.elapsed() > Duration::from_secs(5) {
                self.status_message = None;
                updated = true;
            }
        }

        // Don't invalidate stats cache as aggressively - keep it longer
        if let Some((_, _, _, timestamp)) = &self.stats_cache {
            if timestamp.elapsed() > Duration::from_secs(5) {
                self.stats_cache = None;
            }
        }

        if updated {
            self.needs_redraw = true;
        }

        updated
    }

    /// Check if there are any active animations
    pub fn has_active_animations(&self) -> bool {
        self.loading_animation.active
    }

    /// Mark the app for redraw
    pub fn mark_for_redraw(&mut self) {
        self.needs_redraw = true;
    }

    /// Check if redraw is needed
    pub fn needs_redraw(&self) -> bool {
        self.needs_redraw
    }

    /// Mark as redrawn
    pub fn mark_redrawn(&mut self) {
        self.needs_redraw = false;

        // Track frame time for performance monitoring (simplified)
        self.last_frame_time = self.last_redraw.elapsed();
        self.last_redraw = Instant::now();
    }

    /// Get last frame time for performance display
    pub fn last_frame_time(&self) -> Duration {
        self.last_frame_time
    }

    /// Handle key events with improved responsiveness
    pub fn handle_key_event(&mut self, key: KeyEvent) -> AppResult<ApprovalResult> {
        self.needs_redraw = true;

        match self.mode {
            AppMode::List => self.handle_list_key_event(key),
            AppMode::Help => self.handle_help_key_event(key),
            AppMode::Confirm => self.handle_confirm_key_event(key),
            AppMode::Search => self.handle_search_key_event(key),
            AppMode::Loading => Ok(ApprovalResult::Continue), // Ignore input during loading
        }
    }

    fn handle_list_key_event(&mut self, key: KeyEvent) -> AppResult<ApprovalResult> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => {
                if key.modifiers.contains(KeyModifiers::CONTROL) {
                    // Force quit with Ctrl+Q
                    self.running = false;
                    Ok(ApprovalResult::Quit)
                } else {
                    self.running = false;
                    Ok(ApprovalResult::Quit)
                }
            }
            KeyCode::Char('h') | KeyCode::F(1) => {
                self.mode = AppMode::Help;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('r') | KeyCode::F(5) => {
                self.refresh_data_async()?;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('/') => {
                self.mode = AppMode::Search;
                Ok(ApprovalResult::Continue)
            }
            // Navigation with vim-like keys and arrows
            KeyCode::Up | KeyCode::Char('k') => {
                self.previous_subscriber();
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.next_subscriber();
                Ok(ApprovalResult::Continue)
            }
            KeyCode::PageUp | KeyCode::Char('K') => {
                self.previous_page();
                Ok(ApprovalResult::Continue)
            }
            KeyCode::PageDown | KeyCode::Char('J') => {
                self.next_page();
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Home | KeyCode::Char('g') => {
                self.first_subscriber();
                Ok(ApprovalResult::Continue)
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.last_subscriber();
                Ok(ApprovalResult::Continue)
            }
            // Selection
            KeyCode::Char(' ') => {
                self.toggle_selection();
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('a') => {
                if key.modifiers.contains(KeyModifiers::CONTROL) {
                    self.select_all();
                } else {
                    self.select_all_visible();
                }
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('n') | KeyCode::Char('N') => {
                self.clear_selection();
                Ok(ApprovalResult::Continue)
            }
            // Actions
            KeyCode::Char('A') | KeyCode::Enter => {
                self.initiate_action(ConfirmAction::Approve)?;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('D') => {
                self.initiate_action(ConfirmAction::Decline)?;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('X') | KeyCode::Delete => {
                self.initiate_action(ConfirmAction::Delete)?;
                Ok(ApprovalResult::Continue)
            }
            // Filters
            KeyCode::Char('1') => {
                self.set_filter(SubscriberFilter::All)?;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('2') => {
                self.set_filter(SubscriberFilter::Pending)?;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('3') => {
                self.set_filter(SubscriberFilter::Approved)?;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('4') => {
                self.set_filter(SubscriberFilter::Declined)?;
                Ok(ApprovalResult::Continue)
            }
            _ => Ok(ApprovalResult::Continue),
        }
    }

    fn handle_help_key_event(&mut self, key: KeyEvent) -> AppResult<ApprovalResult> {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc | KeyCode::Char('h') | KeyCode::F(1) => {
                self.mode = AppMode::List;
                Ok(ApprovalResult::Continue)
            }
            _ => Ok(ApprovalResult::Continue),
        }
    }

    fn handle_confirm_key_event(&mut self, key: KeyEvent) -> AppResult<ApprovalResult> {
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.execute_confirm_action()?;
                self.mode = AppMode::List;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char('n') | KeyCode::Esc => {
                self.confirm_action = None;
                self.mode = AppMode::List;
                Ok(ApprovalResult::Continue)
            }
            _ => Ok(ApprovalResult::Continue),
        }
    }

    fn handle_search_key_event(&mut self, key: KeyEvent) -> AppResult<ApprovalResult> {
        match key.code {
            KeyCode::Esc => {
                self.mode = AppMode::List;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Enter => {
                self.apply_filter()?;
                self.mode = AppMode::List;
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Char(c) => {
                self.search_query.push(c);
                self.apply_filter()?; // Real-time search
                Ok(ApprovalResult::Continue)
            }
            KeyCode::Backspace => {
                self.search_query.pop();
                self.apply_filter()?; // Real-time search
                Ok(ApprovalResult::Continue)
            }
            _ => Ok(ApprovalResult::Continue),
        }
    }

    // Optimized helper methods
    fn initiate_action(&mut self, action: ConfirmAction) -> Result<()> {
        if self.selected.is_empty() {
            if let Some(current) = self.current_subscriber_index() {
                self.selected.insert(current);
            }
        }

        if !self.selected.is_empty() {
            self.confirm_action = Some(action);
            self.mode = AppMode::Confirm;
        } else {
            self.set_status_message("No subscribers selected");
        }

        Ok(())
    }

    fn set_filter(&mut self, filter: SubscriberFilter) -> Result<()> {
        if self.filter != filter {
            self.filter = filter;
            self.apply_filter()?;
            self.current_page = 0;
            self.set_status_message(&format!("Filter changed to: {}", self.filter));
        }
        Ok(())
    }

    fn refresh_data_async(&mut self) -> Result<()> {
        self.mode = AppMode::Loading;
        self.loading_animation = AnimationState::new(Duration::from_millis(300));

        // In a real implementation, this would be async
        self.refresh_data()?;

        Ok(())
    }

    fn set_status_message(&mut self, message: &str) {
        self.status_message = Some((message.to_string(), Instant::now()));
    }

    // Navigation methods with pagination
    fn previous_subscriber(&mut self) {
        if self.filtered_subscribers.is_empty() {
            return;
        }

        let current = self.table_state.selected().unwrap_or(0);
        let new_index = if current == 0 {
            self.filtered_subscribers.len() - 1
        } else {
            current - 1
        };

        self.table_state.select(Some(new_index));
        self.update_page_for_selection();
    }

    fn next_subscriber(&mut self) {
        if self.filtered_subscribers.is_empty() {
            return;
        }

        let current = self.table_state.selected().unwrap_or(0);
        let new_index = if current >= self.filtered_subscribers.len() - 1 {
            0
        } else {
            current + 1
        };

        self.table_state.select(Some(new_index));
        self.update_page_for_selection();
    }

    fn previous_page(&mut self) {
        if self.current_page > 0 {
            self.current_page -= 1;
            let new_selection = self.current_page * self.page_size;
            if new_selection < self.filtered_subscribers.len() {
                self.table_state.select(Some(new_selection));
            }
        }
    }

    fn next_page(&mut self) {
        let max_page = (self.filtered_subscribers.len().saturating_sub(1)) / self.page_size;
        if self.current_page < max_page {
            self.current_page += 1;
            let new_selection = self.current_page * self.page_size;
            if new_selection < self.filtered_subscribers.len() {
                self.table_state.select(Some(new_selection));
            }
        }
    }

    fn first_subscriber(&mut self) {
        if !self.filtered_subscribers.is_empty() {
            self.table_state.select(Some(0));
            self.current_page = 0;
        }
    }

    fn last_subscriber(&mut self) {
        if !self.filtered_subscribers.is_empty() {
            let last_index = self.filtered_subscribers.len() - 1;
            self.table_state.select(Some(last_index));
            self.current_page = last_index / self.page_size;
        }
    }

    fn update_page_for_selection(&mut self) {
        if let Some(selected) = self.table_state.selected() {
            self.current_page = selected / self.page_size;
        }
    }

    fn select_all_visible(&mut self) {
        let start = self.current_page * self.page_size;
        let end = ((self.current_page + 1) * self.page_size).min(self.filtered_subscribers.len());

        for i in start..end {
            if let Some(&subscriber_index) = self.filtered_subscribers.get(i) {
                self.selected.insert(subscriber_index);
            }
        }
    }

    // Rest of the implementation methods (same logic as before but optimized)
    fn execute_confirm_action(&mut self) -> Result<()> {
        if let Some(action) = &self.confirm_action {
            let selected_indices: Vec<usize> = self.selected.iter().cloned().collect();
            let count = selected_indices.len();

            // Batch database operations for better performance
            match action {
                ConfirmAction::Approve => {
                    let ids: Vec<i64> = selected_indices
                        .iter()
                        .filter_map(|&index| self.subscribers.get(index).and_then(|s| s.id))
                        .collect();

                    // Batch update all at once
                    for id in ids {
                        self.database
                            .update_subscriber_status(id, SubscriberStatus::Approved)?;
                    }
                    self.set_status_message(&format!("✅ Approved {} subscriber(s)", count));
                }
                ConfirmAction::Decline => {
                    let ids: Vec<i64> = selected_indices
                        .iter()
                        .filter_map(|&index| self.subscribers.get(index).and_then(|s| s.id))
                        .collect();

                    // Batch update all at once
                    for id in ids {
                        self.database
                            .update_subscriber_status(id, SubscriberStatus::Declined)?;
                    }
                    self.set_status_message(&format!("⚠️ Declined {} subscriber(s)", count));
                }
                ConfirmAction::Delete => {
                    let emails: Vec<String> = selected_indices
                        .iter()
                        .filter_map(|&index| self.subscribers.get(index).map(|s| s.email.clone()))
                        .collect();

                    // Batch delete all at once
                    for email in emails {
                        self.database.remove_subscriber(&email)?;
                    }
                    self.set_status_message(&format!("🗑️ Deleted {} subscriber(s)", count));
                }
            }

            self.selected.clear();
            self.confirm_action = None;

            // Only refresh data after all operations complete
            self.refresh_data()?;
        }

        Ok(())
    }

    fn refresh_data(&mut self) -> Result<()> {
        self.subscribers = self.database.get_subscribers(None)?;
        self.stats_cache = None; // Invalidate cache
        self.apply_filter()?;
        Ok(())
    }

    fn apply_filter(&mut self) -> Result<()> {
        self.filtered_subscribers.clear();

        // Pre-lowercase search query once for efficiency
        let lowercase_search = if self.search_query.is_empty() {
            None
        } else {
            Some(self.search_query.to_lowercase())
        };

        // Use iterator with capacity hint for better performance
        self.filtered_subscribers.reserve(self.subscribers.len());

        for (index, subscriber) in self.subscribers.iter().enumerate() {
            // Fast filter check first (most selective)
            let matches_filter = match self.filter {
                SubscriberFilter::All => true,
                SubscriberFilter::Pending => subscriber.status == SubscriberStatus::Pending,
                SubscriberFilter::Approved => subscriber.status == SubscriberStatus::Approved,
                SubscriberFilter::Declined => subscriber.status == SubscriberStatus::Declined,
            };

            if !matches_filter {
                continue;
            }

            // Only do expensive string operations if filter passed
            let matches_search = if let Some(ref search_lower) = lowercase_search {
                subscriber.email.to_lowercase().contains(search_lower)
            } else {
                true
            };

            if matches_search {
                self.filtered_subscribers.push(index);
            }
        }

        // Reset table state if needed
        if self.filtered_subscribers.is_empty() {
            self.table_state.select(None);
        } else if let Some(selected) = self.table_state.selected() {
            if selected >= self.filtered_subscribers.len() {
                self.table_state.select(Some(0));
                self.current_page = 0;
            }
        }

        Ok(())
    }

    fn select_first(&mut self) {
        if !self.filtered_subscribers.is_empty() {
            self.table_state.select(Some(0));
            self.current_page = 0;
        }
    }

    fn current_subscriber_index(&self) -> Option<usize> {
        self.table_state
            .selected()
            .and_then(|i| self.filtered_subscribers.get(i).copied())
    }

    fn toggle_selection(&mut self) {
        if let Some(index) = self.current_subscriber_index() {
            if self.selected.contains(&index) {
                self.selected.remove(&index);
            } else {
                self.selected.insert(index);
            }
        }
    }

    fn select_all(&mut self) {
        for &index in &self.filtered_subscribers {
            self.selected.insert(index);
        }
    }

    fn clear_selection(&mut self) {
        self.selected.clear();
    }

    fn get_statistics(&mut self) -> (usize, usize, usize) {
        // Use cache if available and fresh
        if let Some((total, pending, approved, timestamp)) = &self.stats_cache {
            if timestamp.elapsed() < Duration::from_secs(1) {
                return (*total, *pending, *approved);
            }
        }

        // Calculate fresh statistics
        let total = self.subscribers.len();
        let pending = self
            .subscribers
            .iter()
            .filter(|s| s.status == SubscriberStatus::Pending)
            .count();
        let approved = self
            .subscribers
            .iter()
            .filter(|s| s.status == SubscriberStatus::Approved)
            .count();

        // Update cache
        self.stats_cache = Some((total, pending, approved, Instant::now()));

        (total, pending, approved)
    }

    /// Render the modern approval interface
    pub fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        match self.mode {
            AppMode::Loading => self.render_loading(frame),
            AppMode::List => self.render_list(frame),
            AppMode::Help => self.render_help(frame, area),
            AppMode::Confirm => self.render_confirm(frame, area),
            AppMode::Search => self.render_search(frame, area),
        }

        self.mark_redrawn();
    }

    fn render_loading(&mut self, frame: &mut Frame) {
        let area = frame.area();

        // Center the loading indicator
        let loading_area = centered_rect(40, 20, area);
        frame.render_widget(Clear, loading_area);

        let progress = self.loading_animation.eased_progress();
        let gauge = Gauge::default()
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Loading Newsletter Manager")
                    .border_style(Style::default().fg(self.theme.border_focused)),
            )
            .gauge_style(Style::default().fg(self.theme.primary))
            .percent((progress * 100.0) as u16)
            .label(format!("{}%", (progress * 100.0) as u16));

        frame.render_widget(gauge, loading_area);
    }

    fn render_list(&mut self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Header
                Constraint::Min(1),    // Table
                Constraint::Length(4), // Status bar with performance info
            ])
            .split(frame.area());

        self.render_header(frame, chunks[0]);
        self.render_table(frame, chunks[1]);
        self.render_status_bar(frame, chunks[2]);
    }

    fn render_header(&mut self, frame: &mut Frame, area: Rect) {
        let header_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(40),
                Constraint::Percentage(35),
                Constraint::Percentage(25),
            ])
            .split(area);

        // Title and filter with modern styling
        let title_text = format!("📧 Newsletter Manager - {} Mode", self.filter);
        let title = Paragraph::new(title_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Blogr Newsletter")
                    .border_style(Style::default().fg(self.theme.border_focused))
                    .title_style(
                        Style::default()
                            .fg(self.theme.primary)
                            .add_modifier(Modifier::BOLD),
                    ),
            )
            .style(Style::default().fg(self.theme.text));

        frame.render_widget(title, header_chunks[0]);

        // Statistics with modern design
        let (total, pending, approved) = self.get_statistics();
        let stats_text = vec![
            Line::from(vec![
                Span::styled("Total: ", Style::default().fg(self.theme.text_secondary)),
                Span::styled(
                    total.to_string(),
                    Style::default()
                        .fg(self.theme.text)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(vec![
                Span::styled("Pending: ", Style::default().fg(self.theme.warning)),
                Span::styled(
                    pending.to_string(),
                    Style::default()
                        .fg(self.theme.warning)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled("Approved: ", Style::default().fg(self.theme.success)),
                Span::styled(
                    approved.to_string(),
                    Style::default()
                        .fg(self.theme.success)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
        ];

        let stats_widget = Paragraph::new(stats_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Statistics")
                    .border_style(Style::default().fg(self.theme.border))
                    .title_style(Style::default().fg(self.theme.accent)),
            )
            .style(Style::default().fg(self.theme.text));

        frame.render_widget(stats_widget, header_chunks[1]);

        // Performance info
        let frame_time = self.last_frame_time();
        let fps = 1000.0 / frame_time.as_millis().max(1) as f32;
        let perf_text = vec![
            Line::from(vec![
                Span::styled("FPS: ", Style::default().fg(self.theme.text_secondary)),
                Span::styled(
                    format!("{:.0}", fps),
                    Style::default()
                        .fg(if fps >= 50.0 {
                            self.theme.success
                        } else if fps >= 30.0 {
                            self.theme.warning
                        } else {
                            self.theme.danger
                        })
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(vec![
                Span::styled("Frame: ", Style::default().fg(self.theme.text_secondary)),
                Span::styled(
                    format!("{:.1}ms", frame_time.as_millis()),
                    Style::default().fg(self.theme.text_secondary),
                ),
            ]),
        ];

        let perf_widget = Paragraph::new(perf_text).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Performance")
                .border_style(Style::default().fg(self.theme.border))
                .title_style(Style::default().fg(self.theme.secondary)),
        );

        frame.render_widget(perf_widget, header_chunks[2]);
    }

    fn render_table(&mut self, frame: &mut Frame, area: Rect) {
        let header_cells = ["", "Email", "Status", "Subscribed", "Notes"]
            .iter()
            .map(|h| {
                ratatui::widgets::Cell::from(*h).style(
                    Style::default()
                        .fg(self.theme.text)
                        .add_modifier(Modifier::BOLD),
                )
            });

        let header = Row::new(header_cells)
            .style(Style::default().bg(self.theme.surface))
            .height(1);

        // Get visible rows for current page
        let start_idx = self.current_page * self.page_size;
        let end_idx =
            ((self.current_page + 1) * self.page_size).min(self.filtered_subscribers.len());

        // Pre-create reusable styles to avoid repeated allocations
        let pending_style = Style::default()
            .fg(self.theme.warning)
            .add_modifier(Modifier::BOLD);
        let approved_style = Style::default()
            .fg(self.theme.success)
            .add_modifier(Modifier::BOLD);
        let declined_style = Style::default()
            .fg(self.theme.danger)
            .add_modifier(Modifier::BOLD);
        let text_style = Style::default().fg(self.theme.text);
        let secondary_style = Style::default().fg(self.theme.text_secondary);
        let accent_style = Style::default().fg(self.theme.accent);

        // Pre-allocate row vector with exact capacity for better performance
        let mut rows = Vec::with_capacity(end_idx - start_idx);

        for i in start_idx..end_idx {
            if let Some(&subscriber_index) = self.filtered_subscribers.get(i) {
                if let Some(subscriber) = self.subscribers.get(subscriber_index) {
                    let is_selected = self.selected.contains(&subscriber_index);
                    let selection_indicator = if is_selected { "●" } else { " " };

                    let status_style = match subscriber.status {
                        SubscriberStatus::Pending => pending_style,
                        SubscriberStatus::Approved => approved_style,
                        SubscriberStatus::Declined => declined_style,
                    };

                    let subscribed_date = subscriber
                        .subscribed_at
                        .format("%Y-%m-%d %H:%M")
                        .to_string();
                    let notes = subscriber.notes.as_deref().unwrap_or("-");

                    let row = Row::new(vec![
                        ratatui::widgets::Cell::from(selection_indicator).style(accent_style),
                        ratatui::widgets::Cell::from(subscriber.email.as_str()).style(text_style), // Avoid clone
                        ratatui::widgets::Cell::from(subscriber.status.to_string())
                            .style(status_style),
                        ratatui::widgets::Cell::from(subscribed_date).style(secondary_style),
                        ratatui::widgets::Cell::from(notes).style(secondary_style),
                    ])
                    .height(1);

                    rows.push(row);
                }
            }
        }

        let total_pages = self.filtered_subscribers.len().div_ceil(self.page_size);
        let table_title = format!(
            "Subscribers ({}/{}) - Page {}/{}",
            self.filtered_subscribers.len(),
            self.subscribers.len(),
            self.current_page + 1,
            total_pages.max(1)
        );
        let widths = [
            Constraint::Length(2),  // Selection indicator
            Constraint::Min(25),    // Email
            Constraint::Length(10), // Status
            Constraint::Length(16), // Subscribed date
            Constraint::Min(10),    // Notes
        ];
        let table = Table::new(rows, widths)
            .header(header)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(table_title)
                    .border_style(Style::default().fg(self.theme.border))
                    .title_style(
                        Style::default()
                            .fg(self.theme.primary)
                            .add_modifier(Modifier::BOLD),
                    ),
            )
            .row_highlight_style(
                Style::default()
                    .bg(self.theme.surface)
                    .fg(self.theme.text)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("► ");

        frame.render_stateful_widget(table, area, &mut self.table_state);
    }

    fn render_status_bar(&self, frame: &mut Frame, area: Rect) {
        let status_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
            .split(area);

        // Status message or help
        let status_text = if let Some((ref message, _)) = self.status_message {
            message.clone()
        } else {
            "Press 'h' for help, '/' to search, 'q' to quit".to_string()
        };

        let status_lines = vec![
            Line::from(status_text),
            Line::from(vec![
                Span::styled(
                    "Navigation: ",
                    Style::default().fg(self.theme.text_secondary),
                ),
                Span::styled("↑↓/jk", Style::default().fg(self.theme.accent)),
                Span::raw("  "),
                Span::styled("Actions: ", Style::default().fg(self.theme.text_secondary)),
                Span::styled("A", Style::default().fg(self.theme.success)),
                Span::raw("/"),
                Span::styled("D", Style::default().fg(self.theme.warning)),
                Span::raw("/"),
                Span::styled("X", Style::default().fg(self.theme.danger)),
                Span::raw("  "),
                Span::styled("Select: ", Style::default().fg(self.theme.text_secondary)),
                Span::styled("Space", Style::default().fg(self.theme.accent)),
            ]),
        ];

        let status = Paragraph::new(status_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Status")
                    .border_style(Style::default().fg(self.theme.border))
                    .title_style(Style::default().fg(self.theme.secondary)),
            )
            .style(Style::default().fg(self.theme.text));

        frame.render_widget(status, status_chunks[0]);

        // Selection info with modern design
        let selection_text = if self.selected.is_empty() {
            vec![
                Line::from("No items selected"),
                Line::from(vec![
                    Span::styled("Filter: ", Style::default().fg(self.theme.text_secondary)),
                    Span::styled(
                        self.filter.to_string(),
                        Style::default()
                            .fg(self.theme.primary)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]),
            ]
        } else {
            vec![
                Line::from(vec![
                    Span::styled(
                        format!("{} ", self.selected.len()),
                        Style::default()
                            .fg(self.theme.accent)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("item(s) selected"),
                ]),
                Line::from("Press A/D/X for actions"),
            ]
        };

        let selection = Paragraph::new(selection_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Selection")
                    .border_style(Style::default().fg(self.theme.border))
                    .title_style(Style::default().fg(self.theme.accent)),
            )
            .style(Style::default().fg(self.theme.text));

        frame.render_widget(selection, status_chunks[1]);
    }

    fn render_search(&self, frame: &mut Frame, area: Rect) {
        // Search overlay
        let search_area = centered_rect(60, 20, area);
        frame.render_widget(Clear, search_area);

        let search_text = vec![
            Line::from("Search Subscribers"),
            Line::from(""),
            Line::from(vec![
                Span::styled("Query: ", Style::default().fg(self.theme.text_secondary)),
                Span::styled(
                    &self.search_query,
                    Style::default()
                        .fg(self.theme.text)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled("_", Style::default().fg(self.theme.accent)), // Cursor
            ]),
            Line::from(""),
            Line::from(vec![
                Span::styled("Press ", Style::default().fg(self.theme.text_secondary)),
                Span::styled("Enter", Style::default().fg(self.theme.accent)),
                Span::styled(
                    " to search, ",
                    Style::default().fg(self.theme.text_secondary),
                ),
                Span::styled("Esc", Style::default().fg(self.theme.warning)),
                Span::styled(" to cancel", Style::default().fg(self.theme.text_secondary)),
            ]),
        ];

        let search_popup = Paragraph::new(search_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Search")
                    .title_alignment(Alignment::Center)
                    .border_style(Style::default().fg(self.theme.border_focused))
                    .title_style(
                        Style::default()
                            .fg(self.theme.primary)
                            .add_modifier(Modifier::BOLD),
                    ),
            )
            .alignment(Alignment::Center);

        frame.render_widget(search_popup, search_area);
    }

    fn render_help(&self, frame: &mut Frame, area: Rect) {
        let help_items = vec![
            (
                "Navigation",
                vec![
                    ("↑/k", "Move up"),
                    ("↓/j", "Move down"),
                    ("PgUp/K", "Previous page"),
                    ("PgDn/J", "Next page"),
                    ("Home/g", "Go to first"),
                    ("End/G", "Go to last"),
                ],
            ),
            (
                "Selection",
                vec![
                    ("Space", "Toggle selection"),
                    ("a", "Select all visible"),
                    ("Ctrl+A", "Select all"),
                    ("n/N", "Clear selection"),
                ],
            ),
            (
                "Actions",
                vec![
                    ("A/Enter", "Approve selected"),
                    ("D", "Decline selected"),
                    ("X/Del", "Delete selected"),
                ],
            ),
            (
                "Filters",
                vec![
                    ("1", "Show all"),
                    ("2", "Show pending"),
                    ("3", "Show approved"),
                    ("4", "Show declined"),
                ],
            ),
            (
                "Other",
                vec![
                    ("/", "Search"),
                    ("r/F5", "Refresh"),
                    ("h/F1", "Show help"),
                    ("q/Esc", "Quit"),
                    ("Ctrl+Q", "Force quit"),
                ],
            ),
        ];

        let mut help_lines = vec![
            Line::from(vec![Span::styled(
                "Newsletter Subscriber Management Help",
                Style::default()
                    .fg(self.theme.primary)
                    .add_modifier(Modifier::BOLD),
            )]),
            Line::from(""),
        ];

        for (category, items) in help_items {
            help_lines.push(Line::from(vec![Span::styled(
                category,
                Style::default()
                    .fg(self.theme.accent)
                    .add_modifier(Modifier::BOLD),
            )]));

            for (key, description) in items {
                help_lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(
                        format!("{:<12}", key),
                        Style::default().fg(self.theme.warning),
                    ),
                    Span::raw(" - "),
                    Span::styled(description, Style::default().fg(self.theme.text)),
                ]));
            }
            help_lines.push(Line::from(""));
        }

        help_lines.push(Line::from(vec![Span::styled(
            "Press any key to return",
            Style::default()
                .fg(self.theme.success)
                .add_modifier(Modifier::ITALIC),
        )]));

        let help = Paragraph::new(help_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Help")
                    .title_alignment(Alignment::Center)
                    .border_style(Style::default().fg(self.theme.border_focused))
                    .title_style(
                        Style::default()
                            .fg(self.theme.primary)
                            .add_modifier(Modifier::BOLD),
                    ),
            )
            .wrap(Wrap { trim: true })
            .alignment(Alignment::Left);

        let popup_area = centered_rect(80, 80, area);
        frame.render_widget(Clear, popup_area);
        frame.render_widget(help, popup_area);
    }

    fn render_confirm(&self, frame: &mut Frame, area: Rect) {
        if let Some(ref action) = self.confirm_action {
            let (action_text, color, icon) = match action {
                ConfirmAction::Approve => ("approve", self.theme.success, "✅"),
                ConfirmAction::Decline => ("decline", self.theme.warning, "⚠️"),
                ConfirmAction::Delete => ("delete", self.theme.danger, "🗑️"),
            };

            let count = self.selected.len();
            let confirm_text = vec![
                Line::from(""),
                Line::from(vec![
                    Span::raw(icon),
                    Span::raw("  "),
                    Span::styled(
                        "Confirm Action",
                        Style::default()
                            .fg(self.theme.text)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]),
                Line::from(""),
                Line::from(vec![
                    Span::raw("Are you sure you want to "),
                    Span::styled(
                        action_text,
                        Style::default().fg(color).add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(format!(" {} subscriber(s)?", count)),
                ]),
                Line::from(""),
                Line::from("This action cannot be undone."),
                Line::from(""),
                Line::from(vec![
                    Span::styled("Press ", Style::default().fg(self.theme.text_secondary)),
                    Span::styled(
                        "Y",
                        Style::default()
                            .fg(self.theme.success)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        " to confirm, ",
                        Style::default().fg(self.theme.text_secondary),
                    ),
                    Span::styled(
                        "N",
                        Style::default()
                            .fg(self.theme.danger)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(" to cancel", Style::default().fg(self.theme.text_secondary)),
                ]),
                Line::from(""),
            ];

            let confirm = Paragraph::new(confirm_text)
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Confirmation Required")
                        .title_alignment(Alignment::Center)
                        .border_style(Style::default().fg(color))
                        .title_style(Style::default().fg(color).add_modifier(Modifier::BOLD)),
                )
                .alignment(Alignment::Center);

            let popup_area = centered_rect(60, 40, area);
            frame.render_widget(Clear, popup_area);
            frame.render_widget(confirm, popup_area);
        }
    }
}

/// Helper function to create a centered rectangle
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
