# Blogr CLI

A modern, fast, and developer-friendly static site generator specifically designed for blogs.

## Overview

Blogr CLI is the command-line interface for the Blogr static site generator. It provides a complete toolkit for creating, managing, and deploying blog sites with ease. Built in Rust for performance and reliability.

## Features

-  **Fast builds** - Optimized for speed with incremental building
-  **Beautiful themes** - Built-in themes with easy customization
-  **Markdown support** - Full-featured Markdown with syntax highlighting
-  **Live reload** - Development server with hot reloading
-  **Responsive** - Mobile-first responsive designs
-  **Tags & Categories** - Organize content with tags and categories
-  **Archive pages** - Automatic archive and tag pages
-  **SEO friendly** - Built-in SEO optimization
-  **GitHub Pages ready** - Easy deployment to GitHub Pages
-  **TUI interface** - Interactive terminal user interface for configuration

## Installation

### From Cargo

```bash
cargo install blogr-cli
```

### From Source

```bash
git clone https://github.com/bahdotsh/blogr.git
cd blogr
cargo install --path blogr-cli
```

## Quick Start

### 1. Create a new blog

```bash
blogr init my-blog
cd my-blog
```

### 2. Create your first post

```bash
blogr new "My First Post"
```

### 3. Build and serve locally

```bash
blogr serve
```

Your blog will be available at `http://localhost:3000`

### 4. Build for production

```bash
blogr build
```

## Commands

### Core Commands

- `blogr init [name]` - Initialize a new blog project
- `blogr init --personal [name]` - Initialize a personal website
- `blogr new <title>` - Create a new blog post
- `blogr build` - Build the static site
- `blogr serve` - Start development server with live reload
- `blogr deploy` - Deploy to configured hosting

### Content Management

- `blogr list` - List all posts
- `blogr edit <slug>` - Edit an existing post
- `blogr delete <slug>` - Delete a post

### Configuration

- `blogr config edit` - Interactive configuration editor (TUI)
- `blogr config get <key>` - Get a configuration value
- `blogr config set <key> <value>` - Set a configuration value
- `blogr config domain` - Domain configuration commands
- `blogr theme list` - List available themes
- `blogr theme set <name>` - Change theme
- `blogr theme info <name>` - Show theme details
- `blogr theme preview <name>` - Preview theme in TUI

### Project Management

- `blogr project info` - Show project information
- `blogr project check` - Validate project structure
- `blogr project clean` - Clean build artifacts
- `blogr project stats` - Show project statistics

### Newsletter

- `blogr newsletter fetch-subscribers` - Fetch subscribers from email inbox
- `blogr newsletter approve` - Launch subscriber approval UI
- `blogr newsletter list` - List all subscribers
- `blogr newsletter remove <email>` - Remove a subscriber
- `blogr newsletter export` - Export subscribers to CSV/JSON
- `blogr newsletter send-latest` - Send newsletter with latest post
- `blogr newsletter send-custom` - Send custom newsletter
- `blogr newsletter draft-latest` - Preview latest post newsletter
- `blogr newsletter draft-custom` - Preview custom newsletter
- `blogr newsletter test <email>` - Send test email
- `blogr newsletter import` - Import subscribers from external services
- `blogr newsletter cleanup` - Clean up old send data
- `blogr newsletter tag` - Tag a subscriber
- `blogr newsletter tags` - List all tags
- `blogr newsletter plugin` - Plugin management
- `blogr newsletter api-server` - Start REST API server

## Configuration

Blogr uses a `blogr.toml` configuration file in your project root:

```toml
[blog]
title = "My Blog"
description = "A blog about my thoughts"
author = "Your Name"
base_url = "https://yourblog.com"

[theme]
name = "minimal_retro"

[build]
output_dir = "dist"

[github]
username = "yourusername"
repository = "yourblog"
```

## Themes

Blogr comes with 8 built-in themes:

**Blog Themes:**
- **minimal_retro** - Clean, artistic design with retro typography (default)
- **obsidian** - Obsidian-inspired dark theme with modern aesthetics
- **terminal_candy** - Quirky terminal-inspired theme with pastel colors
- **brutja** - Minimal brutalist theme with pops of color

**Personal Website Themes:**
- **dark_minimal** - Dark minimalist with cyberpunk aesthetics (default)
- **musashi** - Dynamic modern theme with smooth animations
- **slate_portfolio** - Glassmorphic professional portfolio theme
- **typewriter** - Vintage typewriter-inspired aesthetics

### Theme Configuration

Each theme can be customized through the `[theme.config]` section:

```toml
[theme.config]
color_mode = "auto"  # auto, light, dark
accent_color = "#7c3aed"
```

## Content Structure

```
my-blog/
├── blogr.toml          # Configuration
├── posts/              # Blog posts
│   └── my-post.md
├── static/             # Static assets
│   ├── images/
│   ├── css/
│   └── js/
└── dist/              # Generated site
```

## Markdown Features

Blogr supports enhanced Markdown with:

- **Syntax highlighting** for code blocks
- **Frontmatter** for post metadata
- **Tables, lists, and formatting**
- **Custom callouts and admonitions**

### Post Frontmatter

```yaml
---
title: "My Post Title"
date: "2024-01-15"
author: "Your Name"
description: "Post description for SEO"
tags: ["rust", "web", "blogging"]
status: "published"
slug: "my-post-title"
featured: false
external_url: ""  # Optional: link to an external post
---

Your content here...
```

## Development Server

The development server includes:

- **Live reload** - Automatic browser refresh on changes
- **Fast rebuilds** - Incremental building for speed
- **Error reporting** - Clear error messages in terminal and browser
- **Asset serving** - Serves static assets and generated content

```bash
blogr serve --port 3000 --host 0.0.0.0
```

## Deployment

### GitHub Pages

```bash
# Configure GitHub Pages
blogr config github.username yourusername
blogr config github.repository yourblog

# Deploy
blogr deploy
```

### Manual Deployment

Build your site and upload the `dist/` folder to your web server:

```bash
blogr build
# Upload dist/ folder to your hosting provider
```

## TUI Interface

Blogr includes an interactive terminal interface for configuration:

```bash
blogr config
```

The TUI provides:

- **Visual configuration** - Edit settings with a friendly interface
- **Theme preview** - Preview themes before applying
- **Validation** - Real-time validation of settings
- **Help system** - Built-in help and documentation

## Contributing

We welcome contributions! Please see the main [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

## Support

- **Documentation**: [Full documentation](https://github.com/bahdotsh/blogr)
- **Issues**: [GitHub Issues](https://github.com/bahdotsh/blogr/issues)
- **Discussions**: [GitHub Discussions](https://github.com/bahdotsh/blogr/discussions)

---

Made with ❤️ by [bahdotsh](https://github.com/bahdotsh)
