# Sample Blog

A sample blogr project that demonstrates various features including syntax
highlighting and MathJax rendering.

## What this shows

- **Syntax highlighting** — Fenced code blocks with a language (e.g. ` ```rust `)
  are highlighted via syntect using the base16-ocean.dark theme.
- **MathJax rendering** — Inline math with `$...$` and display math with `$$...$$`
  are rendered using MathJax 3.

## Usage

```bash
# From the repository root
cargo install --path blogr-cli

# Build the example site
cd examples/sample-blog
blogr build

# Preview locally
blogr serve
```

The generated HTML will be in `dist/`.
