# Syntax Highlighting Example

A sample blogr project that demonstrates the syntax highlighting feature for
fenced code blocks.

## What this shows

- Fenced code blocks with a language (e.g. ` ```rust `) are highlighted via
  syntect using the base16-ocean.dark theme.
- Blocks without a language fall back to plain HTML-escaped text.
- Indented code blocks are escaped and wrapped but not highlighted.
- HTML special characters inside code blocks are properly escaped.
- Multiple code blocks in a single post each get independent highlighting.

## Usage

```bash
# From the repository root
cargo install --path blogr-cli

# Build the example site
cd examples/syntax-highlighting
blogr build

# Preview locally
blogr serve
```

The generated HTML will be in `dist/`. Open `dist/posts/syntax-highlighting-showcase.html`
to see the highlighted output.
