# Blogr Themes

The theme system for Blogr static site generator, providing beautiful and customizable themes for blogs.

## Overview

Blogr Themes is a library that provides the theme system for the Blogr static site generator. It includes built-in themes and a flexible architecture for creating custom themes.

## Version

**Current Version**: `0.4.1`

This crate uses independent versioning from the main Blogr CLI to allow for theme-specific updates and improvements.

## Features

-  **Multiple built-in themes** - Ready-to-use professional themes
-  **Flexible theme system** - Easy to extend and customize
-  **Responsive designs** - Mobile-first responsive layouts
-  **Dark/light mode support** - Automatic and manual theme switching
-  **TUI integration** - Terminal UI support for theme preview
-  **Asset bundling** - Embedded CSS, templates, and assets
-  **Performance optimized** - Minimal CSS and fast rendering

## Built-in Themes

### Blog Themes

#### Minimal Retro
- **Version**: 1.0.0
- **Style**: Clean, artistic design with retro typography
- **Features**: Warm color palette, serif fonts, minimalist layout
- **Best for**: Personal blogs, creative writing, photography

#### Obsidian
- **Version**: 1.0.0
- **Style**: Modern dark theme inspired by Obsidian
- **Features**: Dark/light mode, purple accents, clean typography
- **Best for**: Technical blogs, documentation, note-taking style content

#### Terminal Candy
- **Version**: 1.0.0
- **Style**: Quirky terminal-inspired design with pastel colors
- **Features**: Glitch effects, ASCII art, typewriter animations
- **Best for**: Creative personal blogs, tech enthusiasts

#### Brutja
- **Version**: 1.0.0
- **Style**: Minimal brutalist design with pops of color
- **Features**: Bold typography, high contrast, clean layout
- **Best for**: Minimalist blogs, design-forward content

### Personal Website Themes

#### Dark Minimal
- **Version**: 1.0.0
- **Style**: Dark minimalist theme with neon accents
- **Features**: Cyberpunk aesthetic, animated backgrounds, customizable status bar
- **Best for**: Personal portfolios, designer/developer showcases

#### Musashi
- **Version**: 1.0.0
- **Style**: Dynamic content-loading theme with modern aesthetics
- **Features**: Smooth animations, clean typography, responsive design
- **Best for**: Personal websites, project showcases

#### Slate Portfolio
- **Version**: 1.0.0
- **Style**: Modern glassmorphic portfolio theme
- **Features**: Frosted glass effects, elegant transitions, professional layout
- **Best for**: Professional portfolios, freelancers

#### Typewriter (NEW in v0.4.0)
- **Version**: 1.0.0
- **Style**: Vintage typewriter-inspired theme with nostalgic aesthetics
- **Features**:
  - Vintage cream paper background with subtle texture
  - Monospace Courier font family
  - Typewriter typing animation for title
  - Blinking cursor effect
  - Vintage date stamp
  - Typewriter-style line separators
  - Configurable paper texture, animations, and cursor
- **Best for**: Writers, bloggers, literary portfolios, creative professionals

## Usage

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
blogr-themes = "0.4.1"
```

### Basic Usage

```rust
use blogr_themes::{ThemeManager, get_theme};

// Get a theme by name
let theme = get_theme("minimal_retro")?;

// Get theme information
let info = theme.info();
println!("Theme: {} v{}", info.name, info.version);

// Get templates
let templates = theme.templates();
let base_template = templates.get("base.html").unwrap();

// Get assets (CSS, images, etc.)
let assets = theme.assets();
```

### Theme Manager

```rust
use blogr_themes::ThemeManager;

let mut manager = ThemeManager::new();

// List available themes
let themes = manager.list_themes();
for theme_name in themes {
    println!("Available theme: {}", theme_name);
}

// Load a specific theme
let theme = manager.load_theme("obsidian")?;
```

## Theme Architecture

### Theme Trait

Each theme implements the `Theme` trait:

```rust
pub trait Theme: Send + Sync {
    fn info(&self) -> ThemeInfo;
    fn templates(&self) -> ThemeTemplates;
    fn assets(&self) -> HashMap<String, Vec<u8>>;
    fn preview_tui_style(&self) -> ratatui::style::Style;
}
```

### ThemeTemplates

Templates are built using a builder pattern:

```rust
pub struct ThemeTemplates {
    templates: Vec<(&'static str, &'static str)>,
}

impl ThemeTemplates {
    pub fn new(base_template_name: &'static str, base_template: &'static str) -> Self;
    pub fn with_template(self, name: &'static str, template: &'static str) -> Self;
}
```

### Theme Info

```rust
pub struct ThemeInfo {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub config_schema: HashMap<String, ConfigOption>,
    pub site_type: SiteType,  // Blog or Personal
}
```

### Configuration Options

Themes can define configurable options:

```rust
pub struct ConfigOption {
    pub value: toml::Value,     // Configuration value (string, bool, integer, float, etc.)
    pub description: String,    // Help text
}
```

## Template System

Themes use the Tera templating engine with these standard templates:

- `base.html` - Base layout template
- `index.html` - Homepage template
- `post.html` - Individual post template
- `archive.html` - Archive page template
- `tags.html` - Tags index template
- `tag.html` - Individual tag page template

### Template Variables

Common variables available in templates:

```jinja2
<!-- Site configuration -->
{{ site.blog.title }}
{{ site.blog.description }}
{{ site.blog.author }}
{{ site.theme.config.* }}

<!-- Post data -->
{{ post.metadata.title }}
{{ post.metadata.date }}
{{ post.metadata.tags }}
{{ post.content }}

<!-- Collections -->
{{ posts }}           <!-- All posts -->
{{ tags }}            <!-- All tags with counts -->
```

### Template Functions

Built-in template functions:

```jinja2
<!-- Generate URLs -->
{{ url(path="posts/my-post.html") }}
{{ asset_url(path="css/style.css") }}

<!-- Date formatting -->
{{ post.metadata.date | date(format="%Y-%m-%d") }}
```

## Creating Custom Themes

### 1. Implement the Theme Trait

```rust
use blogr_themes::{Theme, ThemeInfo, ThemeTemplates, ConfigOption, SiteType};
use std::collections::HashMap;

pub struct MyCustomTheme;

impl Theme for MyCustomTheme {
    fn info(&self) -> ThemeInfo {
        let mut schema = HashMap::new();
        
        schema.insert("primary_color".to_string(), ConfigOption {
            value: toml::Value::String("#007acc".to_string()),
            description: "Primary theme color".to_string(),
        });

        ThemeInfo {
            name: "My Custom Theme".to_string(),
            version: "1.0.0".to_string(),
            author: "Your Name".to_string(),
            description: "A custom theme for my blog".to_string(),
            config_schema: schema,
            site_type: SiteType::Blog,
        }
    }

    fn templates(&self) -> ThemeTemplates {
        ThemeTemplates::new("base.html", include_str!("templates/base.html"))
            .with_template("index.html", include_str!("templates/index.html"))
            .with_template("post.html", include_str!("templates/post.html"))
            .with_template("archive.html", include_str!("templates/archive.html"))
            .with_template("tags.html", include_str!("templates/tags.html"))
            .with_template("tag.html", include_str!("templates/tag.html"))
    }

    fn assets(&self) -> HashMap<String, Vec<u8>> {
        let mut assets = HashMap::new();
        assets.insert(
            "css/style.css".to_string(),
            include_bytes!("assets/style.css").to_vec(),
        );
        // Add more assets...
        assets
    }

    fn preview_tui_style(&self) -> ratatui::style::Style {
        use ratatui::style::{Color, Style};
        Style::default()
            .fg(Color::Blue)
            .bg(Color::Black)
    }
}
```

### 2. Register Your Theme

```rust
use blogr_themes::ThemeManager;

let mut manager = ThemeManager::new();
manager.register_theme("my_custom", Box::new(MyCustomTheme));
```

## Asset Bundling

Themes can bundle assets (CSS, images, fonts) directly into the binary:

```rust
fn assets(&self) -> HashMap<String, Vec<u8>> {
    let mut assets = HashMap::new();
    
    // Bundle CSS
    assets.insert(
        "css/style.css".to_string(),
        include_bytes!("assets/style.css").to_vec(),
    );
    
    // Bundle images
    assets.insert(
        "images/logo.png".to_string(),
        include_bytes!("assets/logo.png").to_vec(),
    );
    
    assets
}
```

## TUI Integration

Themes can provide custom styling for the terminal user interface:

```rust
fn preview_tui_style(&self) -> ratatui::style::Style {
    use ratatui::style::{Color, Style};
    
    Style::default()
        .fg(Color::Rgb(167, 139, 250))  // Purple accent
        .bg(Color::Rgb(32, 32, 32))     // Dark background
}
```

## Development

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

### Adding a New Theme

1. Create a new module in `src/`
2. Implement the `Theme` trait
3. Add templates in `templates/`
4. Add assets in `assets/`
5. Register the theme in `lib.rs`
6. Update the theme manager

## Contributing

We welcome theme contributions! Please:

1. Follow the existing theme structure
2. Ensure responsive design
3. Test across different content types
4. Document configuration options
5. Include preview screenshots

See the main [CONTRIBUTING.md](../CONTRIBUTING.md) for detailed guidelines.

## Changelog

### v0.4.1
- **NEW**: External post support — posts can link to external URLs
- **IMPROVED**: Search indexing with better plain text extraction
- **NEW**: Newsletter API endpoints for creating and sending newsletters
- **IMPROVED**: Newsletter hardening with cleanup, rate limiting, and validation

### v0.4.0
- **NEW**: Typewriter theme for personal websites with vintage aesthetics
- **NEW**: Brutja theme — minimal brutalist blog theme with pops of color
- **NEW**: Blog link support in all personal website themes (Dark Minimal, Musashi, Slate Portfolio, Typewriter)
- **IMPROVED**: Personal mode now uses title and description from content.md instead of blogr.toml
- **IMPROVED**: Conditional separator lines in Typewriter theme based on section presence
- **ADDED**: Vintage typewriter effects including typing animation, blinking cursor, and paper texture

### v0.3.0
- **NEW**: Personal website themes (Dark Minimal, Musashi, Slate Portfolio)
- **NEW**: Terminal Candy theme for blogs
- **IMPROVED**: Better theme organization for blog vs personal modes
- **ADDED**: Personal mode support with section-based layouts

### v0.2.0
- **NEW**: Obsidian theme improvements
- **IMPROVED**: Better responsive design across all themes
- **FIXED**: Content centering and layout issues
- **ADDED**: Enhanced CSS bundling system

### v0.1.3
- Initial release with minimal_retro and obsidian themes
- Basic theme system architecture
- TUI integration support

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

## Support

- **Documentation**: [Full documentation](https://github.com/bahdotsh/blogr)
- **Issues**: [GitHub Issues](https://github.com/bahdotsh/blogr/issues)
- **Theme Requests**: [GitHub Discussions](https://github.com/bahdotsh/blogr/discussions)

---

Made with ❤️ by [bahdotsh](https://github.com/bahdotsh)
