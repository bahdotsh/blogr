use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub mod dark_minimal;
pub mod minimal_retro;
pub mod musashi;
pub mod obsidian;
pub mod slate_portfolio;
pub mod terminal_candy;
pub mod typewriter;

pub use dark_minimal::DarkMinimalTheme;
pub use minimal_retro::MinimalRetroTheme;
pub use musashi::MusashiTheme;
pub use obsidian::ObsidianTheme;
pub use slate_portfolio::SlatePortfolioTheme;
pub use terminal_candy::TerminalCandyTheme;
pub use typewriter::TypewriterTheme;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SiteType {
    Blog,
    Personal,
}

impl fmt::Display for SiteType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SiteType::Blog => write!(f, "blog"),
            SiteType::Personal => write!(f, "personal"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemeInfo {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub config_schema: HashMap<String, ConfigOption>,
    /// Type of site this theme supports
    pub site_type: SiteType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigOption {
    pub option_type: String,
    pub default: String,
    pub description: String,
}

pub trait Theme: Send + Sync {
    fn info(&self) -> ThemeInfo;
    fn templates(&self) -> ThemeTemplates;
    fn assets(&self) -> HashMap<String, Vec<u8>>;
    fn preview_tui_style(&self) -> ratatui::style::Style;
}

pub struct ThemeTemplates {
    templates: Vec<(&'static str, &'static str)>,
}

impl ThemeTemplates {
    // Base template must be first. This ensure it's registered first with Tera when we iterate through the templates.
    pub fn new(base_template_name: &'static str, base_template: &'static str) -> Self {
        Self {
            templates: vec![(base_template_name, base_template)],
        }
    }

    pub fn with_template(mut self, name: &'static str, template: &'static str) -> Self {
        self.templates.push((name, template));
        self
    }
}

impl IntoIterator for ThemeTemplates {
    type Item = (&'static str, &'static str);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.templates.into_iter()
    }
}

#[must_use]
pub fn get_all_themes() -> HashMap<String, Box<dyn Theme>> {
    let mut themes: HashMap<String, Box<dyn Theme>> = HashMap::new();

    let minimal_retro = MinimalRetroTheme::new();
    themes.insert("minimal-retro".to_string(), Box::new(minimal_retro));

    let obsidian = ObsidianTheme::new();
    themes.insert("obsidian".to_string(), Box::new(obsidian));

    let terminal_candy = TerminalCandyTheme::new();
    themes.insert("terminal-candy".to_string(), Box::new(terminal_candy));

    let dark_minimal = DarkMinimalTheme::new();
    themes.insert("dark-minimal".to_string(), Box::new(dark_minimal));

    let musashi = MusashiTheme::new();
    themes.insert("musashi".to_string(), Box::new(musashi));

    let slate_portfolio = SlatePortfolioTheme::new();
    themes.insert("slate-portfolio".to_string(), Box::new(slate_portfolio));

    let typewriter = TypewriterTheme::new();
    themes.insert("typewriter".to_string(), Box::new(typewriter));

    themes
}

#[must_use]
pub fn get_theme(name: &str) -> Option<Box<dyn Theme>> {
    match name {
        "minimal-retro" => Some(Box::new(MinimalRetroTheme::new()) as Box<dyn Theme>),
        "obsidian" => Some(Box::new(ObsidianTheme::new()) as Box<dyn Theme>),
        "terminal-candy" => Some(Box::new(TerminalCandyTheme::new()) as Box<dyn Theme>),
        "dark-minimal" => Some(Box::new(DarkMinimalTheme::new()) as Box<dyn Theme>),
        "musashi" => Some(Box::new(MusashiTheme::new()) as Box<dyn Theme>),
        "slate-portfolio" => Some(Box::new(SlatePortfolioTheme::new()) as Box<dyn Theme>),
        "typewriter" => Some(Box::new(TypewriterTheme::new()) as Box<dyn Theme>),
        _ => None,
    }
}

#[must_use]
pub fn get_theme_by_name(name: &str) -> Option<Box<dyn Theme>> {
    get_theme(name)
}
