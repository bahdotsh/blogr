use anyhow::Result;
use pulldown_cmark::{html, CodeBlockKind, Event, Options, Parser, Tag};
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::html::{styled_line_to_highlighted_html, IncludeBackground};
use syntect::parsing::SyntaxSet;
use syntect::util::LinesWithEndings;

/// Render markdown to HTML with syntax highlighting
pub fn render_markdown(markdown: &str) -> Result<String> {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_FOOTNOTES);
    options.insert(Options::ENABLE_TASKLISTS);

    let parser = Parser::new_ext(markdown, options);

    // Process events with stateful code block tracking for syntax highlighting
    let mut current_lang: Option<String> = None;
    let mut events: Vec<Event> = Vec::new();

    for event in parser {
        match event {
            Event::Start(Tag::CodeBlock(kind)) => {
                let lang = match &kind {
                    CodeBlockKind::Fenced(lang) => lang.to_string(),
                    CodeBlockKind::Indented => String::new(),
                };
                let class_attr = if !lang.is_empty() {
                    format!(" class=\"language-{}\"", html_escape(&lang))
                } else {
                    String::new()
                };
                current_lang = Some(lang);
                events.push(Event::Html(
                    format!("<pre class=\"highlight\"><code{}>", class_attr).into(),
                ));
            }
            Event::End(Tag::CodeBlock(_)) => {
                current_lang = None;
                events.push(Event::Html("</code></pre>".into()));
            }
            Event::Text(text) if current_lang.is_some() => {
                let lang = current_lang.as_deref().unwrap();
                if !lang.is_empty() {
                    match highlight_code(&text, lang) {
                        Ok(highlighted) => events.push(Event::Html(highlighted.into())),
                        Err(_) => events.push(Event::Html(html_escape(&text).into())),
                    }
                } else {
                    events.push(Event::Html(html_escape(&text).into()));
                }
            }
            _ => events.push(event),
        }
    }

    let mut html_output = String::new();
    html::push_html(&mut html_output, events.into_iter());

    Ok(html_output)
}

/// Highlight code using syntect, returning only `<span>` elements (no `<pre>` wrapper)
fn highlight_code(code: &str, language: &str) -> Result<String> {
    let syntax_set = SyntaxSet::load_defaults_newlines();
    let theme_set = ThemeSet::load_defaults();

    let syntax = syntax_set
        .find_syntax_by_token(language)
        .or_else(|| syntax_set.find_syntax_by_extension(language))
        .unwrap_or_else(|| syntax_set.find_syntax_plain_text());

    let theme = &theme_set.themes["base16-ocean.dark"];
    let mut highlighter = HighlightLines::new(syntax, theme);
    let mut output = String::new();
    for line in LinesWithEndings::from(code) {
        let ranges = highlighter.highlight_line(line, &syntax_set)?;
        let html = styled_line_to_highlighted_html(&ranges[..], IncludeBackground::No)?;
        output.push_str(&html);
    }
    Ok(output)
}

/// HTML escape text
fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

/// Convert markdown to plain text (for excerpts)
#[allow(dead_code)]
pub fn markdown_to_text(markdown: &str) -> String {
    let parser = Parser::new(markdown);
    let mut text = String::new();

    for event in parser {
        match event {
            Event::Text(t) | Event::Code(t) => text.push_str(&t),
            Event::SoftBreak | Event::HardBreak => text.push(' '),
            _ => {}
        }
    }

    text
}

/// Extract excerpt from markdown (first paragraph or first N words)
#[allow(dead_code)]
pub fn extract_excerpt(markdown: &str, word_limit: usize) -> String {
    let text = markdown_to_text(markdown);
    let words: Vec<&str> = text.split_whitespace().take(word_limit).collect();
    let excerpt = words.join(" ");

    if text.split_whitespace().count() > word_limit {
        format!("{}...", excerpt)
    } else {
        excerpt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fenced_code_block_has_syntax_highlighting() {
        let md = "```rust\nfn main() {}\n```";
        let html = render_markdown(md).unwrap();
        // syntect produces <span style="..."> for highlighted tokens
        assert!(
            html.contains("<span style="),
            "Expected syntax-highlighted spans, got: {}",
            html
        );
        // Should have our wrapper, not syntect's <pre style="...">
        assert!(html.contains("<pre class=\"highlight\">"));
        assert!(html.contains("language-rust"));
        // Must not have nested <pre> from syntect
        assert!(
            !html.contains("<pre style="),
            "Found nested <pre> from syntect: {}",
            html
        );
    }

    #[test]
    fn test_indented_code_block_is_escaped() {
        let md = "    let x = 1;\n    let y = 2;\n";
        let html = render_markdown(md).unwrap();
        assert!(html.contains("<pre class=\"highlight\"><code>"));
        assert!(html.contains("</code></pre>"));
    }

    #[test]
    fn test_unknown_language_falls_back_gracefully() {
        let md = "```nonexistentlang\nsome code\n```";
        let html = render_markdown(md).unwrap();
        // Should still produce highlighted output (syntect falls back to plain text)
        assert!(html.contains("<pre class=\"highlight\">"));
        assert!(html.contains("some code"));
    }

    #[test]
    fn test_empty_code_block() {
        let md = "```rust\n```";
        let html = render_markdown(md).unwrap();
        assert!(html.contains("<pre class=\"highlight\">"));
        assert!(html.contains("</code></pre>"));
    }

    #[test]
    fn test_non_code_text_unchanged() {
        let md = "Hello **world**";
        let html = render_markdown(md).unwrap();
        assert!(html.contains("<strong>world</strong>"));
        assert!(!html.contains("<pre"));
    }
}
