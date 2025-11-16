use std::collections::HashMap;

use crate::config::Config;
use crate::project::Project;
use crate::tui::theme::TuiTheme;
use anyhow::Ok;
use blogr_themes::{get_all_themes, ThemeInfo};
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Modifier, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, Cell, Clear, HighlightSpacing, List, ListItem, ListState, Paragraph, Row,
        Table, TableState, Wrap,
    },
    Frame,
};
use serde::Deserialize;
use strum::{EnumIter, IntoEnumIterator};

pub type AppResult<T> = anyhow::Result<T>;

#[derive(PartialEq)]
enum HighLevelListItem {
    Field(ConfigField),
    Section(ConfigSection),
    BlankLine,
}

struct HighLevelConfigList(Vec<HighLevelListItem>);
impl HighLevelConfigList {
    fn new(config: &Config) -> Self {
        let inner = ConfigSection::iter()
            .map(|section| (section, section.get_section(config)))
            .map(|(section, fields)| {
                let mut list_section = vec![
                    HighLevelListItem::BlankLine,
                    HighLevelListItem::Section(section),
                ];
                list_section.append(
                    &mut fields
                        .iter()
                        .map(|field| HighLevelListItem::Field(field.clone()))
                        .collect(),
                );
                list_section
            })
            .fold(Vec::new(), |mut acc, mut list_section| {
                acc.append(&mut list_section);
                acc
            });

        Self(inner)
    }

    fn index_of(&self, field: &ConfigField) -> Option<usize> {
        self.0.iter().position(|item| match item {
            HighLevelListItem::Field(i) => i == field,
            _ => false,
        })
    }

    fn next(&self, index: usize) -> Option<(usize, &ConfigField)> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(i, item)| match item {
                HighLevelListItem::Field(field) => Some((i, field)),
                _ => None,
            })
            .find(|(i, _item)| *i > index)
    }

    fn prev(&self, index: usize) -> Option<(usize, &ConfigField)> {
        self.0
            .iter()
            .enumerate()
            .rev()
            .filter_map(|(i, item)| match item {
                HighLevelListItem::Field(field) => Some((i, field)),
                _ => None,
            })
            .find(|(i, _item)| *i < index)
    }
}

fn set_theme_option(
    config: &mut Config,
    option_name: String,
    old_value: &toml::Value,
    new_value: String,
) -> AppResult<()> {
    // if the field type was last String, don't try parsing the new value into anything but that.
    let new_value = match matches!(old_value, toml::Value::String(_)) {
        true => toml::Value::String(new_value),
        false => toml::Value::deserialize(toml::de::ValueDeserializer::parse(&new_value)?)?,
    };
    config
        .theme
        .config
        .entry(option_name)
        .insert_entry(new_value);
    Ok(())
}

fn set_primary_domain(config: &mut Config, new_value: String) {
    if config.blog.domains.is_none() {
        config.blog.domains = Some(crate::config::DomainConfig {
            primary: None,
            aliases: Vec::new(),
            subdomain: None,
            enforce_https: true,
            github_pages_domain: None,
        });
    }
    if let Some(domains) = &mut config.blog.domains {
        domains.primary = match new_value.is_empty() {
            true => None,
            false => Some(new_value.clone()),
        };
        domains.github_pages_domain = match new_value.is_empty() {
            true => None,
            false => Some(new_value),
        };
    }
}

fn set_domain_enforce_https(config: &mut Config, new_value: String) -> AppResult<()> {
    let enforce_https = new_value.parse()?;
    if config.blog.domains.is_none() {
        config.blog.domains = Some(crate::config::DomainConfig {
            primary: None,
            aliases: Vec::new(),
            subdomain: None,
            enforce_https,
            github_pages_domain: None,
        });
    }
    if let Some(domains) = &mut config.blog.domains {
        domains.enforce_https = enforce_https;
    }
    Ok(())
}

/// Configuration field types
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigField {
    BlogTitle,
    BlogAuthor,
    BlogDescription,
    BlogBaseUrl,
    BlogLanguage,
    BlogTimezone,
    ThemeName,
    ThemeOption { name: String, value: toml::Value },
    DomainPrimary,
    DomainEnforceHttps,
    BuildOutputDir,
    BuildDrafts,
    BuildFuturePosts,
    DevPort,
    DevAutoReload,
}

impl std::fmt::Display for ConfigField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::BlogTitle => "Blog Title",
            Self::BlogAuthor => "Blog Author",
            Self::BlogDescription => "Blog Description",
            Self::BlogBaseUrl => "Base URL",
            Self::BlogLanguage => "Language",
            Self::BlogTimezone => "Timezone",
            Self::ThemeName => "Theme Name",
            Self::ThemeOption { name, .. } => name,
            Self::DomainPrimary => "Primary Domain",
            Self::DomainEnforceHttps => "Enforce HTTPS",
            Self::BuildOutputDir => "Output Directory",
            Self::BuildDrafts => "Include Drafts",
            Self::BuildFuturePosts => "Include Future Posts",
            Self::DevPort => "Development Port",
            Self::DevAutoReload => "Auto Reload",
        };
        write!(f, "{name}")
    }
}

impl ConfigField {
    pub fn get_value(&self, config: &Config) -> String {
        match self {
            Self::BlogTitle => config.blog.title.clone(),
            Self::BlogAuthor => config.blog.author.clone(),
            Self::BlogDescription => config.blog.description.clone(),
            Self::BlogBaseUrl => config.blog.base_url.clone(),
            Self::BlogLanguage => config.blog.language.as_deref().unwrap_or("").to_string(),
            Self::BlogTimezone => config.blog.timezone.as_deref().unwrap_or("").to_string(),
            Self::ThemeName => config.theme.name.clone(),
            // don't render toml strings with added quotes
            Self::ThemeOption { value, .. } => match value {
                toml::Value::String(val) => val.clone(),
                _ => value.to_string(),
            },
            Self::DomainPrimary => {
                if let Some(domains) = &config.blog.domains {
                    domains.primary.as_deref().unwrap_or("").to_string()
                } else {
                    "".to_string()
                }
            }
            Self::DomainEnforceHttps => {
                if let Some(domains) = &config.blog.domains {
                    domains.enforce_https.to_string()
                } else {
                    "true".to_string()
                }
            }
            Self::BuildOutputDir => config
                .build
                .output_dir
                .as_deref()
                .unwrap_or("dist")
                .to_string(),
            Self::BuildDrafts => config.build.drafts.to_string(),
            Self::BuildFuturePosts => config.build.future_posts.to_string(),
            Self::DevPort => config.dev.port.to_string(),
            Self::DevAutoReload => config.dev.auto_reload.to_string(),
        }
    }

    fn set(&self, config: &mut Config, new_value: String) -> AppResult<()> {
        match self {
            Self::BlogTitle => config.blog.author = new_value,
            Self::BlogAuthor => config.blog.author = new_value,
            Self::BlogDescription => config.blog.description = new_value,
            Self::BlogBaseUrl => config.blog.base_url = new_value,
            Self::BlogLanguage => {
                config.blog.language = (!new_value.is_empty()).then_some(new_value)
            }
            Self::BlogTimezone => {
                config.blog.timezone = (!new_value.is_empty()).then_some(new_value)
            }
            Self::ThemeName => config.theme.name = new_value,
            Self::ThemeOption { name, value } => {
                set_theme_option(config, name.clone(), value, new_value)?
            }
            Self::DomainPrimary => set_primary_domain(config, new_value),
            Self::DomainEnforceHttps => set_domain_enforce_https(config, new_value)?,
            Self::BuildOutputDir => {
                config.build.output_dir = (!new_value.is_empty()).then_some(new_value)
            }
            Self::BuildDrafts => config.build.drafts = new_value.parse()?,
            Self::BuildFuturePosts => config.build.future_posts = new_value.parse()?,
            Self::DevPort => config.dev.port = new_value.parse()?,
            Self::DevAutoReload => config.dev.auto_reload = new_value.parse()?,
        }
        Ok(())
    }

    pub fn is_boolean(&self) -> bool {
        matches!(
            self,
            Self::DomainEnforceHttps
                | Self::BuildDrafts
                | Self::BuildFuturePosts
                | Self::DevAutoReload
                | Self::ThemeOption {
                    value: toml::Value::Boolean(_),
                    ..
                }
        )
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::DevPort
                | Self::ThemeOption {
                    value: toml::Value::Integer(_),
                    ..
                }
        )
    }
}

fn get_theme_specific_config_fields(config: &Config) -> Vec<ConfigField> {
    config
        .theme
        .config
        .clone()
        .into_iter()
        .map(|(name, value)| ConfigField::ThemeOption { name, value })
        .collect::<Vec<ConfigField>>()
}

fn get_all_theme_fields(config: &Config) -> Vec<ConfigField> {
    let mut fields = vec![ConfigField::ThemeName];
    fields.append(&mut get_theme_specific_config_fields(config));
    fields
}

#[derive(Debug, Clone, Copy, EnumIter, PartialEq, Eq, Hash)]
pub enum ConfigSection {
    Blog,
    Theme,
    Domain,
    Build,
    Development,
}

impl std::fmt::Display for ConfigSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Blog => "Blog Settings",
            Self::Theme => "Theme Settings",
            Self::Domain => "Domain Settings",
            Self::Build => "Build Settings",
            Self::Development => "Development Settings",
        };
        write!(f, "{name}")
    }
}

impl ConfigSection {
    fn get_section(&self, config: &Config) -> Vec<ConfigField> {
        match self {
            Self::Blog => vec![
                ConfigField::BlogTitle,
                ConfigField::BlogAuthor,
                ConfigField::BlogDescription,
                ConfigField::BlogBaseUrl,
                ConfigField::BlogLanguage,
                ConfigField::BlogTimezone,
            ],
            Self::Theme => get_all_theme_fields(config),
            Self::Domain => vec![ConfigField::DomainPrimary, ConfigField::DomainEnforceHttps],
            Self::Build => vec![
                ConfigField::BuildOutputDir,
                ConfigField::BuildDrafts,
                ConfigField::BuildFuturePosts,
            ],
            Self::Development => vec![ConfigField::DevPort, ConfigField::DevAutoReload],
        }
    }
}

/// public API of the application
pub struct ConfigApp {
    state: ConfigAppState,
    theme: TuiTheme,
}

impl ConfigApp {
    pub fn new(config: Config, project: Project, theme: TuiTheme) -> Self {
        let state = ConfigAppState::new(config, project);
        Self { state, theme }
    }
    /// Render the application
    pub fn render(&mut self, frame: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // Header
                Constraint::Min(0),    // Main content
                Constraint::Length(3), // Status bar
            ])
            .split(frame.area());

        self.state.render_header(frame, chunks[0], &self.theme);
        self.state
            .render_main_content(frame, chunks[1], &self.theme);
        self.state.render_status_bar(frame, chunks[2], &self.theme);
    }

    pub fn handle_key_event(mut self, key: KeyEvent) -> AppResult<Self> {
        self.state = self.state.handle_key_event(key)?;
        Ok(self)
    }

    /// Handle tick event
    pub fn tick(&self) {}

    pub fn is_stopped(&self) -> bool {
        matches!(self.state, ConfigAppState::Shutdown(_))
    }
}

/// Configuration editor mode.
/// The larger variants are boxed at Clippy's suggestion.
enum ConfigAppState {
    Browse(Box<Browse>),
    Edit(Box<Edit>),
    EditTheme(Box<EditTheme>),
    Help(Box<Help>),
    Shutdown(Shutdown),
}

impl ConfigAppState {
    pub fn new(config: Config, project: Project) -> Self {
        Self::Browse(Box::new(Browse::new(config, project)))
    }

    fn render_main_content(&mut self, frame: &mut Frame, area: Rect, theme: &TuiTheme) {
        match self {
            Self::Browse(app) => app.render_browse_mode(frame, area, theme),
            Self::Edit(app) => app.render_edit_mode(frame, area, theme),
            Self::EditTheme(app) => app.render_table(frame, area, theme),
            Self::Help(app) => app.render_help_overlay(frame, theme), // Help is rendered as overlay
            Self::Shutdown(_) => {}
        }
    }

    fn render_header(&self, frame: &mut Frame, area: Rect, theme: &TuiTheme) {
        let title = "Blogr Configuration Editor";

        let header = Paragraph::new(title)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme.border_style())
                    .title("Configuration")
                    .title_style(theme.title_style()),
            )
            .style(theme.text_style());

        frame.render_widget(header, area);
    }

    fn render_status_bar(&self, frame: &mut Frame, area: Rect, theme: &TuiTheme) {
        let status = Paragraph::new(self.get_status())
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(theme.border_style()),
            )
            .style(theme.text_style());

        frame.render_widget(status, area);
    }

    fn handle_key_event(self, key: KeyEvent) -> AppResult<Self> {
        match self {
            Self::Browse(app) => Ok(app.handle_key_event(key)),
            Self::Edit(app) => app.handle_key_event(key),
            Self::EditTheme(app) => app.handle_key_event(key),
            Self::Help(app) => Ok(app.handle_key_event(key)),
            Self::Shutdown(app) => Ok(app.into()),
        }
    }

    fn get_status(&self) -> String {
        match self {
            Self::Browse(app) => app.status_message.clone(),
            Self::Edit(app) => app.browse_data.status_message.clone(),
            Self::EditTheme(app) => app.browse_data.status_message.clone(),
            Self::Help(app) => app.browse_data.status_message.clone(),
            Self::Shutdown(_) => "Shutting down".to_string(),
        }
    }
}

impl From<Browse> for ConfigAppState {
    fn from(value: Browse) -> Self {
        ConfigAppState::Browse(Box::new(value))
    }
}

impl From<Edit> for ConfigAppState {
    fn from(value: Edit) -> Self {
        ConfigAppState::Edit(Box::new(value))
    }
}

impl From<EditTheme> for ConfigAppState {
    fn from(value: EditTheme) -> Self {
        ConfigAppState::EditTheme(Box::new(value))
    }
}

impl From<Help> for ConfigAppState {
    fn from(value: Help) -> Self {
        ConfigAppState::Help(Box::new(value))
    }
}

impl From<Shutdown> for ConfigAppState {
    fn from(value: Shutdown) -> Self {
        ConfigAppState::Shutdown(value)
    }
}

struct Browse {
    config: Config,
    project: Project,
    /// Current field selection
    selected_field: ConfigField,
    /// The index of the field in the ConfigField enum variant array
    config_index: usize,
    /// Lays out the ConfigField into sections with headers and blank lines.
    /// Maps the ConfigField index into the actual list layout so ListState can render the selection.
    list_layout: HighLevelConfigList,
    /// List state for field selection
    list_state: ListState,
    status_message: String,
}

impl Browse {
    fn new(config: Config, project: Project) -> Self {
        let mut list_state = ListState::default();
        let list_layout = HighLevelConfigList::new(&config);
        let selected_field = ConfigField::BlogTitle;
        let config_index = list_layout.index_of(&selected_field).unwrap_or(2);
        list_state.select(Some(config_index));
        Self {
            config,
            project,
            selected_field,
            config_index,
            list_layout,
            list_state,
            status_message: "Navigate with ↑/↓, Enter to edit, 'q' to quit".to_string(),
        }
    }

    pub fn handle_key_event(self, key: KeyEvent) -> ConfigAppState {
        match key.code {
            KeyCode::Char('q') | KeyCode::Char('Q') => self.enter_shutdown_mode().into(),
            KeyCode::Char('h') | KeyCode::F(1) => self.enter_help_mode().into(),
            KeyCode::Up => self.key_up().into(),
            KeyCode::Down => self.key_down().into(),
            KeyCode::Enter => match self.selected_field {
                ConfigField::ThemeName => self.enter_edit_theme_mode().into(),
                _ => self.enter_edit_mode().into(),
            },
            _ => self.into(),
        }
    }

    fn key_up(mut self) -> Self {
        if self.config_index == 0 {
            return self;
        }
        let prev: Option<(usize, &ConfigField)> = self.list_layout.prev(self.config_index);
        if let Some((index, prev)) = prev {
            self.selected_field = prev.clone();
            self.config_index = index;
            self.list_state.select(Some(index));
        }
        self
    }

    fn key_down(mut self) -> Self {
        let next = self.list_layout.next(self.config_index);
        if let Some((index, next)) = next {
            self.selected_field = next.clone();
            self.config_index = index;
            self.list_state.select(Some(index));
        }

        self
    }

    fn render_browse_mode(&mut self, frame: &mut Frame, area: Rect, theme: &TuiTheme) {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(area);

        // Render field list
        self.render_field_list(frame, chunks[0], theme);

        // Render field details
        self.render_field_details(frame, chunks[1], theme);
    }

    fn render_field_list(&mut self, frame: &mut Frame, area: Rect, theme: &TuiTheme) {
        let items = self
            .list_layout
            .0
            .iter()
            .map(|item| match item {
                HighLevelListItem::BlankLine => ListItem::new(""),
                HighLevelListItem::Field(field) => {
                    let value = field.get_value(&self.config);
                    let display_value = if value.len() > 20 {
                        format!("{}...", &value[..17])
                    } else {
                        value
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!("  {}: ", field),
                            Style::default().fg(theme.text_color),
                        ),
                        Span::styled(display_value, Style::default().fg(theme.text_color)),
                    ]))
                }
                HighLevelListItem::Section(section) => ListItem::new(Line::from(Span::styled(
                    section.to_string(),
                    Style::default()
                        .add_modifier(Modifier::BOLD)
                        .fg(theme.primary_color),
                ))),
            })
            .collect::<Vec<ListItem>>();

        let list = List::new(items)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Configuration Fields")
                    .border_style(theme.focused_border_style()),
            )
            .highlight_style(
                Style::default()
                    .bg(theme.primary_color)
                    .fg(theme.background_color)
                    .add_modifier(Modifier::BOLD),
            );

        frame.render_stateful_widget(list, area, &mut self.list_state);
    }

    fn render_field_details(&self, frame: &mut Frame, area: Rect, theme: &TuiTheme) {
        let value = self.selected_field.get_value(&self.config);
        let effective_url = if matches!(self.selected_field, ConfigField::BlogBaseUrl) {
            format!("\nEffective URL: {}", self.config.get_effective_base_url())
        } else {
            String::new()
        };

        let content = format!(
            "Field: {}\nCurrent Value: {}{}\n\nPress Enter to edit this field",
            self.selected_field, value, effective_url
        );

        let details = Paragraph::new(content)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Field Details")
                    .border_style(theme.border_style()),
            )
            .wrap(Wrap { trim: true })
            .style(theme.text_style());

        frame.render_widget(details, area);
    }

    fn enter_edit_mode(mut self) -> Edit {
        let edit_buffer = self.selected_field.get_value(&self.config);
        self.status_message = format!(
            "Editing {}: Press Enter to save, Esc to cancel",
            self.selected_field
        );
        Edit {
            new_config: self.config.clone(),
            target_field: self.selected_field.clone(),
            browse_data: self,
            edit_buffer,
        }
    }

    fn enter_edit_theme_mode(self) -> EditTheme {
        self.into()
    }

    fn enter_help_mode(self) -> Help {
        Help { browse_data: self }
    }

    fn enter_shutdown_mode(self) -> Shutdown {
        Shutdown
    }
}

struct Edit {
    browse_data: Browse,
    target_field: ConfigField,
    edit_buffer: String,
    new_config: Config,
}

struct Help {
    browse_data: Browse,
}

struct Shutdown;

impl Edit {
    pub fn handle_key_event(mut self, key: KeyEvent) -> AppResult<ConfigAppState> {
        match key.code {
            KeyCode::Esc => {
                let mut browse_data = self.enter_browse_mode();
                browse_data.status_message = "Edit cancelled".to_string();
                Ok(browse_data.into())
            }
            KeyCode::Enter => self.apply(),
            KeyCode::Backspace => {
                self.edit_buffer.pop();
                Ok(self.into())
            }
            KeyCode::Char(c) => {
                self.edit_buffer.push(c);
                Ok(self.into())
            }
            _ => Ok(self.into()),
        }
    }

    fn render_edit_mode(&self, frame: &mut Frame, area: Rect, theme: &TuiTheme) {
        let edit_area = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(5), Constraint::Min(0)])
            .split(area);

        let input_text = if self.browse_data.selected_field.is_boolean() {
            format!("{} (true/false)", self.edit_buffer)
        } else if self.browse_data.selected_field.is_numeric() {
            format!("{} (number)", self.edit_buffer)
        } else {
            self.edit_buffer.clone()
        };

        let input = Paragraph::new(input_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("Editing: {}", self.browse_data.selected_field))
                    .border_style(theme.focused_border_style()),
            )
            .style(theme.text_style());

        frame.render_widget(input, edit_area[0]);

        let help_text = if self.browse_data.selected_field.is_boolean() {
            "Enter 'true' or 'false'"
        } else if self.browse_data.selected_field.is_numeric() {
            "Enter a valid number"
        } else {
            "Enter the new value"
        };

        let help = Paragraph::new(format!(
            "{}\n\nPress Enter to save, Esc to cancel",
            help_text
        ))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Help")
                .border_style(theme.border_style()),
        )
        .wrap(Wrap { trim: true })
        .style(theme.text_style());

        frame.render_widget(help, edit_area[1]);
    }

    fn apply(mut self) -> AppResult<ConfigAppState> {
        let new_value = self.edit_buffer.trim().to_string();
        if new_value.is_empty() {
            self.browse_data.status_message = "Edit discarded".to_string();
            return Ok(self.browse_data.into());
        }

        if let Err(e) = self.target_field.set(&mut self.new_config, new_value) {
            self.browse_data.status_message = e.to_string();
            return Ok(self.into());
        };

        self.browse_data = save_and_refresh(self.browse_data, self.new_config.clone())?;
        Ok(self.enter_browse_mode().into())
    }

    fn enter_browse_mode(self) -> Browse {
        self.browse_data
    }
}

impl Help {
    pub fn handle_key_event(self, key: KeyEvent) -> ConfigAppState {
        match key.code {
            KeyCode::Esc | KeyCode::Char('h') | KeyCode::F(1) => self.enter_browse_mode().into(),
            _ => self.into(),
        }
    }

    fn render_help_overlay(&self, frame: &mut Frame, theme: &TuiTheme) {
        let area = frame.area();
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

        frame.render_widget(Clear, popup_area);

        let help_text = vec![
            "Blogr Configuration Editor Help",
            "",
            "Navigation:",
            "  ↑/↓       - Navigate fields",
            "  Enter     - Edit selected field",
            "  Esc       - Cancel edit",
            "",
            "Actions:",
            "  s         - Save configuration",
            "  q         - Quit",
            "  h/F1      - Toggle this help",
            "",
            "Field Types:",
            "  Text      - Enter any text",
            "  Boolean   - Enter 'true' or 'false'",
            "  Number    - Enter a valid number",
            "",
            "Press any key to close this help",
        ];

        let help = Paragraph::new(help_text.join("\n"))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Help")
                    .border_style(theme.focused_border_style()),
            )
            .wrap(Wrap { trim: true })
            .style(theme.text_style());

        frame.render_widget(help, popup_area);
    }

    fn enter_browse_mode(self) -> Browse {
        self.browse_data
    }
}

struct EditTheme {
    browse_data: Browse,
    options: Vec<ThemeInfo>,
    table_state: TableState,
    row_index: usize,
    new_config: Config,
    current_theme_index: Option<usize>,
}

impl From<Browse> for EditTheme {
    fn from(value: Browse) -> Self {
        let options = get_all_themes()
            .iter()
            .map(|theme| theme.info())
            .collect::<Vec<ThemeInfo>>();

        let current_theme_index = options
            .iter()
            .position(|theme| theme.name == value.config.theme.name);

        let row_index = current_theme_index.unwrap_or(0);
        let mut table_state = TableState::default();
        table_state.select(Some(row_index));

        let new_config = value.config.clone();
        Self {
            browse_data: value,
            new_config,
            options,
            row_index,
            table_state,
            current_theme_index,
        }
    }
}

impl EditTheme {
    fn render_table(&mut self, frame: &mut Frame, area: Rect, theme: &TuiTheme) {
        let header_style = Style::default()
            .fg(theme.primary_color)
            .add_modifier(Modifier::BOLD);
        let selected_row_style = Style::default()
            .add_modifier(Modifier::REVERSED)
            .fg(theme.focused_border_color);
        let selected_col_style = Style::default().fg(theme.cursor_color);
        let selected_cell_style = Style::default()
            .add_modifier(Modifier::REVERSED)
            .fg(theme.background_color);

        let header = ["Name", "Version", "Type", "Author", "Description"]
            .into_iter()
            .map(Cell::from)
            .collect::<Row>()
            .style(header_style)
            .height(1);
        let rows = self.options.iter().enumerate().map(|(i, data)| {
            let item = data.as_data_row();
            let style = match self.current_theme_index {
                Some(j) if j == i => Style::new()
                    .fg(theme.text_color)
                    .bg(theme.background_color)
                    .italic(),
                _ => Style::new().fg(theme.text_color),
            };
            item.into_iter()
                .map(|content| Cell::from(Text::from(format!("\n{content}\n"))))
                .collect::<Row>()
                .style(style)
                .height(4)
        });

        let bar = " █ ";
        let t = Table::new(
            rows,
            [
                Constraint::Length(20),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(20),
                Constraint::Min(40),
            ],
        )
        .block(
            Block::new()
                .borders(Borders::ALL)
                .title("Themes")
                .border_style(theme.focused_border_style()),
        )
        .header(header)
        .row_highlight_style(selected_row_style)
        .column_highlight_style(selected_col_style)
        .cell_highlight_style(selected_cell_style)
        .highlight_symbol(Text::from(vec![
            "".into(),
            bar.into(),
            bar.into(),
            "".into(),
        ]))
        .highlight_spacing(HighlightSpacing::Always);
        frame.render_stateful_widget(t, area, &mut self.table_state);
    }

    fn handle_key_event(self, key: KeyEvent) -> AppResult<ConfigAppState> {
        match key.code {
            KeyCode::Esc => Ok(self.enter_browse_mode().into()),
            KeyCode::Up => Ok(self.key_up().into()),
            KeyCode::Down => Ok(self.key_down().into()),
            KeyCode::Enter => Ok(self.set_theme()?.into()),
            _ => Ok(self.into()),
        }
    }

    fn key_up(mut self) -> Self {
        if self.row_index == 0 {
            return self;
        }
        self.row_index -= 1;
        self.table_state.select(Some(self.row_index));
        self
    }

    fn key_down(mut self) -> Self {
        if self.row_index >= self.options.len() - 1 {
            return self;
        }
        self.row_index += 1;
        self.table_state.select(Some(self.row_index));
        self
    }

    fn set_theme(mut self) -> AppResult<Browse> {
        let theme = self
            .options
            .get(self.row_index)
            .expect("Index out of bounds")
            .clone();
        let default_theme_config = theme
            .config_schema
            .into_iter()
            .map(|(field_name, config)| (field_name, config.value))
            .collect::<HashMap<String, toml::Value>>();
        self.new_config.set_theme(theme.name, default_theme_config);
        //save
        self.browse_data = save_and_refresh(self.browse_data, self.new_config.clone())?;
        Ok(self.enter_browse_mode())
    }

    fn enter_browse_mode(self) -> Browse {
        self.browse_data
    }
}

fn save_and_refresh(mut browse_data: Browse, new_config: Config) -> AppResult<Browse> {
    let config_path = browse_data.project.root.join("blogr.toml");
    browse_data.config = new_config;
    browse_data.config.save_to_file(&config_path)?;
    browse_data.list_layout = HighLevelConfigList::new(&browse_data.config);
    browse_data.status_message = "Configuration saved successfully!".to_string();
    Ok(browse_data)
}
