# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
cargo build                    # Build all workspace crates
cargo build --release          # Release build
cargo test                     # Run all tests
cargo test -p blogr-cli        # Tests for CLI crate only
cargo test -p blogr-themes     # Tests for themes crate only
cargo test --doc               # Doc tests only
cargo fmt --all -- --check     # Check formatting
cargo fmt                      # Auto-format
cargo clippy --all-targets --all-features -- -D warnings  # Lint (CI treats warnings as errors)
cargo install --path blogr-cli # Install locally as `blogr` binary
```

## Architecture

Rust workspace with two crates:

- **`blogr-cli`** — Main CLI application (binary: `blogr`). Entry point: `blogr-cli/src/main.rs`. Uses clap derive for CLI parsing, tokio async runtime.
- **`blogr-themes`** — Theme library. Entry point: `blogr-themes/src/lib.rs`. Defines the `Theme` trait and ships 8 built-in themes.

### blogr-cli modules

- `commands/` — One file per CLI subcommand (init, new, build, serve, deploy, etc.). Dispatched from `main.rs` match arms.
- `generator/` — Static site generation: `SiteBuilder` (site.rs), markdown rendering (markdown.rs), search index (search_index.rs), asset copying (assets.rs).
- `config.rs` — Blog configuration (blogr.toml parsing/writing).
- `content.rs` — Post/content model (frontmatter parsing, markdown files).
- `project.rs` — Project discovery and validation (finds blogr.toml, checks structure).
- `tui/` — Ratatui-based terminal UI: post editor, config editor, theme preview.
- `newsletter/` — Email newsletter subsystem: IMAP fetcher, SQLite subscriber DB, SMTP sender, REST API, plugin system.
- `utils.rs` — Shared utilities.

### blogr-themes

Each theme is a subdirectory (`minimal_retro/`, `obsidian/`, `brutja/`, etc.) implementing the `Theme` trait which provides:
- `info()` → metadata including `SiteType` (Blog or Personal)
- `templates()` → Tera HTML templates (blog themes need: base, index, post, archive, tag, tags; personal themes need: base, index)
- `assets()` → CSS/JS/images as embedded bytes
- `preview_tui_style()` → ratatui style for TUI preview

Themes are registered in `get_all_themes()` in `lib.rs`. Templates use the Tera engine.

## Conventions

- Conventional commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `theme:`
- Branch naming: `feature/`, `fix/`, `theme/` prefixes
- CI runs on Ubuntu, macOS, and Windows with stable + beta Rust
- Clippy warnings are treated as errors in CI (`-D warnings`)
