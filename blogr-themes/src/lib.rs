use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub mod brutja;
pub mod dark_minimal;
pub mod minimal_retro;
pub mod musashi;
pub mod obsidian;
pub mod slate_portfolio;
pub mod terminal_candy;
pub mod typewriter;

pub use brutja::BrutjaTheme;
pub use dark_minimal::DarkMinimalTheme;
pub use minimal_retro::MinimalRetroTheme;
pub use musashi::MusashiTheme;
pub use obsidian::ObsidianTheme;
pub use slate_portfolio::SlatePortfolioTheme;
pub use terminal_candy::TerminalCandyTheme;
pub use typewriter::TypewriterTheme;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SiteType {
    #[default]
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

impl ThemeInfo {
    pub fn as_data_row(&self) -> [String; 5] {
        [
            self.name.clone(),
            self.version.clone(),
            self.site_type.to_string(),
            self.author.clone(),
            self.description.clone(),
        ]
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigOption {
    pub value: toml::Value,
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
pub fn get_all_themes() -> Vec<Box<dyn Theme>> {
    vec![
        Box::new(MinimalRetroTheme::new()),
        Box::new(ObsidianTheme::new()),
        Box::new(TerminalCandyTheme::new()),
        Box::new(DarkMinimalTheme::new()),
        Box::new(MusashiTheme::new()),
        Box::new(SlatePortfolioTheme::new()),
        Box::new(TypewriterTheme::new()),
        Box::new(BrutjaTheme::new()),
    ]
}

#[must_use]
pub fn get_theme(name: &str) -> Option<Box<dyn Theme>> {
    get_all_themes()
        .into_iter()
        .find(|theme| theme.info().name.to_lowercase() == name.to_lowercase())
}

#[must_use]
pub fn get_theme_by_name(name: &str) -> Option<Box<dyn Theme>> {
    get_theme(name)
}

#[cfg(test)]
mod test {
    use crate::SiteType;
    use crate::ThemeTemplates;
    use std::collections::{HashMap, HashSet};

    use crate::get_all_themes;

    #[test]
    fn themes_have_unique_names() {
        let all_theme_names = get_all_themes()
            .iter()
            .map(|theme| theme.info().name)
            .collect::<Vec<String>>();

        let unique_theme_names = all_theme_names
            .clone()
            .into_iter()
            .collect::<HashSet<String>>();

        // we could assert and end the test here but really we want to know which name has been duplicated.
        if unique_theme_names.len() == all_theme_names.len() {
            return;
        }

        let duplicate_theme_name = all_theme_names
            .iter()
            .fold(HashMap::new(), |mut acc: HashMap<String, u32>, name| {
                acc.entry(name.clone())
                    .and_modify(|entry| *entry += 1)
                    .or_insert(1);
                acc
            })
            .into_iter()
            .find(|(_name, count)| *count > 1);

        if let Some((duplicate, count)) = duplicate_theme_name {
            panic!("Theme name {duplicate} occurs {count} times.");
        } else {
            panic!("Test working incorrectly. Unreachable statement reached.");
        }
    }

    impl SiteType {
        fn required_templates(&self) -> Vec<&'static str> {
            match self {
                Self::Blog => vec![
                    "archive.html",
                    "base.html",
                    "index.html",
                    "post.html",
                    "tag.html",
                    "tags.html",
                ],
                Self::Personal => vec!["base.html", "index.html"],
            }
        }
    }

    fn templates_satisfy_site_type(templates: &ThemeTemplates, site_type: &SiteType) -> bool {
        let mut expected = site_type.required_templates();
        let actual = templates
            .templates
            .iter()
            .map(|pair| pair.0)
            .collect::<Vec<&'static str>>();

        for template in actual {
            let found = expected.iter().position(|item| *item == template);
            if let Some(expected_template) = found {
                expected.swap_remove(expected_template);
            }
        }

        expected.is_empty()
    }

    #[test]
    fn themes_have_appropriate_templates() {
        get_all_themes().iter().for_each(|theme| {
            let info = theme.info();
            if !templates_satisfy_site_type(&theme.templates(), &info.site_type) {
                panic!(
                    "{} does not satisfy the minimum template requirements for {}",
                    info.name, info.site_type
                )
            }
        });
    }
}
