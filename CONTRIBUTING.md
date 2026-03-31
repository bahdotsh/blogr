# Contributing to Blogr

Thank you for your interest in contributing to Blogr! This document provides guidelines and information for contributors.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Contributing Areas](#contributing-areas)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Theme Development](#theme-development)
- [Documentation](#documentation)
- [Community](#community)

## Code of Conduct

This project follows the Rust Code of Conduct. Please be respectful and inclusive in all interactions.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/yourusername/blogr.git
   cd blogr
   ```
3. **Add the upstream remote**:
   ```bash
   git remote add upstream https://github.com/bahdotsh/blogr.git
   ```

## Development Setup

### Prerequisites

- **Rust 1.70+** - Install via [rustup](https://rustup.rs/)
- **Git** - For version control
- **GitHub CLI** (optional) - For easier PR management

### Building from Source

```bash
# Clone the repository
git clone https://github.com/bahdotsh/blogr.git
cd blogr

# Build the project
cargo build

# Run tests
cargo test

# Install locally for testing
cargo install --path blogr-cli
```

### Development Tools

Install recommended development tools:

```bash
# Code formatting
rustup component add rustfmt

# Linting
rustup component add clippy

# Coverage (optional)
cargo install cargo-tarpaulin

# Dependency optimization
cargo install cargo-chef
```

## Project Structure

Blogr is organized as a Rust workspace with two main crates:

```
blogr/
├── Cargo.toml              # Workspace configuration
├── blogr-cli/              # Main CLI application
│   ├── src/
│   │   ├── main.rs         # CLI entry point
│   │   ├── commands/       # CLI subcommand implementations
│   │   ├── tui/            # Terminal user interface
│   │   ├── generator/      # Static site generation
│   │   ├── newsletter/     # Email newsletter subsystem
│   │   ├── config.rs       # Configuration parsing
│   │   ├── content.rs      # Post/content models
│   │   ├── project.rs      # Project discovery/validation
│   │   └── utils.rs        # Shared utilities
│   └── templates/          # Project initialization templates
├── blogr-themes/           # Themes crate
│   └── src/
│       ├── lib.rs          # Theme registry & trait
│       ├── minimal_retro/  # Minimal Retro theme (blog)
│       ├── obsidian/       # Obsidian theme (blog)
│       ├── terminal_candy/ # Terminal Candy theme (blog)
│       ├── brutja/         # Brutja theme (blog)
│       ├── dark_minimal/   # Dark Minimal theme (personal)
│       ├── musashi/        # Musashi theme (personal)
│       ├── slate_portfolio/# Slate Portfolio theme (personal)
│       └── typewriter/     # Typewriter theme (personal)
└── README.md
```

### Key Components

- **CLI Commands** (`blogr-cli/src/commands/`): Command-line interface implementations
- **TUI System** (`blogr-cli/src/tui/`): Terminal user interface components
- **Site Generator** (`blogr-cli/src/generator/`): Static site generation logic
- **Newsletter** (`blogr-cli/src/newsletter/`): Email newsletter subsystem
- **Theme System** (`blogr-themes/src/`): Theme architecture and built-in themes

## Contributing Areas

### 🎨 Themes (High Priority)

We especially need help with themes! The current Minimal Retro theme is just the beginning.

**Theme Ideas Needed:**
- Dark themes
- Academic/research-focused themes
- Photography/portfolio themes
- Minimalist/brutalist themes
- Corporate/professional themes
- Technical documentation themes

### 🚀 Features

- New CLI commands
- TUI improvements
- Generator enhancements
- GitHub integration features
- Configuration options

### 🐛 Bug Fixes

- Performance optimizations
- Cross-platform compatibility
- Error handling improvements
- Edge case handling

### 📚 Documentation

- API documentation
- User guides
- Examples and tutorials
- Code comments

### 🧪 Testing

- Unit tests
- Integration tests
- Theme testing
- Cross-platform testing

## Development Workflow

### Branching Strategy

- `main` - Stable release branch
- `develop` - Development integration branch
- Feature branches: `feature/your-feature-name`
- Bug fixes: `fix/issue-description`
- Themes: `theme/theme-name`

### Making Changes

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the code style guidelines

3. **Test your changes**:
   ```bash
   cargo test
   cargo clippy
   cargo fmt
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

5. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create a Pull Request** on GitHub

### Commit Message Format

We follow conventional commits:

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation changes
- `style:` - Code style changes
- `refactor:` - Code refactoring
- `test:` - Test additions/changes
- `theme:` - Theme-related changes

Examples:
```
feat: add dark theme support
fix: resolve TUI editor cursor positioning
docs: update theme development guide
theme: add academic theme
```

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Run tests for specific crate
cargo test -p blogr-cli
cargo test -p blogr-themes

# Run tests with coverage
cargo tarpaulin --all-features
```

### Test Requirements

- All new features must include tests
- Bug fixes should include regression tests
- Themes should include visual/rendering tests
- Maintain or improve test coverage

## Code Style

### Rust Guidelines

- Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `cargo fmt` for formatting
- Address all `cargo clippy` warnings
- Write clear, self-documenting code
- Add documentation for public APIs

### Code Quality

```bash
# Format code
cargo fmt

# Check linting
cargo clippy -- -D warnings

# Check for common issues
cargo audit
```

### Documentation

- Document all public functions and structs
- Include examples in documentation
- Update README.md for user-facing changes
- Add inline comments for complex logic

## Submitting Changes

### Pull Request Process

1. **Ensure your PR**:
   - Passes all CI checks
   - Includes appropriate tests
   - Updates documentation if needed
   - Follows code style guidelines

2. **PR Description should include**:
   - Clear description of changes
   - Motivation and context
   - Screenshots for UI changes
   - Breaking changes (if any)

3. **Review Process**:
   - Maintainers will review your PR
   - Address feedback promptly
   - Be open to suggestions and changes

### CI/CD Pipeline

Our CI pipeline runs:
- Code formatting checks (`cargo fmt`)
- Linting (`cargo clippy`)
- Tests on multiple platforms (Ubuntu, macOS, Windows)
- Coverage reporting
- Build verification

All checks must pass before merging.

## Theme Development

### Creating a New Theme

1. **Create theme directory**:
   ```
   blogr-themes/src/your_theme_name/
   ├── mod.rs
   ├── templates/
   │   ├── base.html
   │   ├── index.html
   │   ├── post.html
   │   └── ...
   └── assets/
       ├── style.css
       └── ...
   ```

2. **Implement the Theme trait**:
   ```rust
   use crate::{Theme, ThemeInfo, ThemeTemplates, SiteType};
   
   pub struct YourTheme;
   
   impl Theme for YourTheme {
       fn info(&self) -> ThemeInfo { /* ... */ }
       fn templates(&self) -> ThemeTemplates { /* ... */ }
       fn assets(&self) -> HashMap<String, Vec<u8>> { /* ... */ }
       fn preview_tui_style(&self) -> ratatui::style::Style { /* ... */ }
   }
   ```

3. **Register your theme** in `blogr-themes/src/lib.rs`

4. **Test your theme**:
   ```bash
   blogr theme preview your-theme-name
   ```

### Theme Guidelines

- Follow responsive design principles
- Ensure good accessibility (contrast, font sizes)
- Test on different screen sizes
- Include comprehensive template coverage
- Provide configuration options
- Document theme-specific features

### Template System

Themes use Tera templating with these available variables:

- `site` - Site configuration and metadata
- `posts` - Collection of blog posts
- `post` - Current post (in post templates)
- `page` - Current page information
- `config` - Theme configuration

## Documentation

### API Documentation

```bash
# Generate and view documentation
cargo doc --open
```

### Contributing to Docs

- Update docstrings for code changes
- Add examples to complex functions
- Keep README.md current
- Update CHANGELOG.md for releases

## Community

### Getting Help

- **GitHub Discussions**: For questions and general discussion
- **Issues**: For bug reports and feature requests
- **Discord/Matrix**: (links to be added)

### Reporting Issues

When reporting bugs:

1. Use a clear, descriptive title
2. Provide steps to reproduce
3. Include system information (OS, Rust version)
4. Add relevant logs or error messages
5. Mention expected vs actual behavior

### Feature Requests

For new features:

1. Check existing issues first
2. Describe the problem you're solving
3. Propose a solution
4. Consider implementation complexity
5. Be open to alternative approaches

## Recognition

Contributors will be recognized in:

- CHANGELOG.md for releases
- README.md acknowledgments
- GitHub contributors page
- Special mentions for significant contributions

## Questions?

Don't hesitate to ask questions! You can:

- Open a GitHub Discussion
- Comment on relevant issues
- Reach out to maintainers

Thank you for contributing to Blogr! 🎉
