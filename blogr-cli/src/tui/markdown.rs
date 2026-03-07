use crate::tui::theme::TuiTheme;
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use ratatui::text::{Line, Span};

/// Markdown renderer for the TUI
pub struct MarkdownRenderer {
    /// Parser options
    options: Options,
}

impl MarkdownRenderer {
    /// Create a new markdown renderer
    pub fn new() -> Self {
        let mut options = Options::empty();
        options.insert(Options::ENABLE_STRIKETHROUGH);
        options.insert(Options::ENABLE_TABLES);
        options.insert(Options::ENABLE_FOOTNOTES);
        options.insert(Options::ENABLE_TASKLISTS);

        Self { options }
    }

    /// Render markdown to styled lines
    pub fn render_markdown(&self, markdown: &str, theme: &TuiTheme) -> Vec<Line<'static>> {
        let parser = Parser::new_ext(markdown, self.options);
        let mut lines = Vec::new();
        let mut current_line = Vec::new();
        let mut in_code_block = false;
        let mut in_header = false;
        let mut header_level = 0;
        let mut in_emphasis = false;
        let mut in_strong = false;
        let in_code = false;
        let mut in_blockquote = false;
        let mut list_level: usize = 0;

        for event in parser {
            match event {
                Event::Start(tag) => match tag {
                    Tag::Heading { level, .. } => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        in_header = true;
                        header_level = level as usize;
                    }
                    Tag::Emphasis => {
                        in_emphasis = true;
                    }
                    Tag::Strong => {
                        in_strong = true;
                    }
                    Tag::CodeBlock(_) => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        in_code_block = true;
                    }
                    Tag::BlockQuote(_) => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        in_blockquote = true;
                    }
                    Tag::List(_) => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        list_level += 1;
                    }
                    Tag::Item => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        // Add bullet point or number
                        let indent = "  ".repeat(list_level.saturating_sub(1));
                        current_line.push(Span::styled(
                            format!("{}• ", indent),
                            theme.markdown_list_style(),
                        ));
                    }
                    Tag::Paragraph => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                    }
                    Tag::Link { .. } => {
                        current_line
                            .push(Span::styled("[".to_string(), theme.markdown_link_style()));
                    }
                    _ => {}
                },
                Event::End(tag) => match tag {
                    TagEnd::Heading(_) => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        lines.push(Line::from("")); // Add spacing after headers
                        in_header = false;
                        header_level = 0;
                    }
                    TagEnd::Emphasis => {
                        in_emphasis = false;
                    }
                    TagEnd::Strong => {
                        in_strong = false;
                    }
                    TagEnd::CodeBlock => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        lines.push(Line::from("")); // Add spacing after code blocks
                        in_code_block = false;
                    }
                    TagEnd::BlockQuote(_) => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        lines.push(Line::from("")); // Add spacing after blockquotes
                        in_blockquote = false;
                    }
                    TagEnd::List(_) => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        lines.push(Line::from("")); // Add spacing after lists
                        list_level = list_level.saturating_sub(1);
                    }
                    TagEnd::Item => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                    }
                    TagEnd::Paragraph => {
                        if !current_line.is_empty() {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        lines.push(Line::from("")); // Add spacing after paragraphs
                    }
                    TagEnd::Link => {
                        current_line
                            .push(Span::styled("]".to_string(), theme.markdown_link_style()));
                    }
                    _ => {}
                },
                Event::Text(text) => {
                    let style = if in_code_block {
                        theme.markdown_code_block_style()
                    } else if in_header {
                        match header_level {
                            1 => theme.markdown_h1_style(),
                            2 => theme.markdown_h2_style(),
                            3 => theme.markdown_h3_style(),
                            _ => theme.markdown_header_style(),
                        }
                    } else if in_code {
                        theme.markdown_code_style()
                    } else if in_strong {
                        theme.markdown_bold_style()
                    } else if in_emphasis {
                        theme.markdown_italic_style()
                    } else if in_blockquote {
                        theme.markdown_blockquote_style()
                    } else {
                        theme.text_style()
                    };

                    let content = if (in_blockquote || in_code_block) && !text.starts_with("  ") {
                        format!("  {}", text)
                    } else {
                        text.to_string()
                    };

                    // Split text by newlines and create separate lines
                    for (i, line_text) in content.split('\n').enumerate() {
                        if i > 0 {
                            lines.push(Line::from(current_line));
                            current_line = Vec::new();
                        }
                        if !line_text.is_empty() || i == 0 {
                            current_line.push(Span::styled(line_text.to_string(), style));
                        }
                    }
                }
                Event::Code(code) => {
                    current_line.push(Span::styled(code.to_string(), theme.markdown_code_style()));
                }
                Event::Html(html) => {
                    // For now, just display HTML as-is
                    current_line.push(Span::styled(html.to_string(), theme.markdown_html_style()));
                }
                Event::SoftBreak => {
                    current_line.push(Span::raw(" "));
                }
                Event::HardBreak => {
                    lines.push(Line::from(current_line));
                    current_line = Vec::new();
                }
                Event::Rule => {
                    if !current_line.is_empty() {
                        lines.push(Line::from(current_line));
                        current_line = Vec::new();
                    }
                    lines.push(Line::from(Span::styled(
                        "─".repeat(80),
                        theme.markdown_rule_style(),
                    )));
                    lines.push(Line::from(""));
                }
                _ => {}
            }
        }

        // Add any remaining content
        if !current_line.is_empty() {
            lines.push(Line::from(current_line));
        }

        // Convert to owned data
        lines
            .into_iter()
            .map(|line| {
                let spans: Vec<Span<'static>> = line
                    .spans
                    .into_iter()
                    .map(|span| Span::styled(span.content.to_string(), span.style))
                    .collect();
                Line::from(spans)
            })
            .collect()
    }
}
