---
title: "Syntax Highlighting Showcase"
date: "2026-03-07"
author: "blogr"
description: "Demonstrates syntax highlighting across multiple languages"
tags: ["syntax-highlighting", "code", "demo"]
status: "published"
slug: "syntax-highlighting-showcase"
featured: true
---

# Syntax Highlighting Showcase

This post demonstrates the syntax highlighting feature added to blogr. Fenced
code blocks with a language identifier are highlighted using syntect with the
base16-ocean.dark theme. Blocks without a language gracefully fall back to
plain escaped text.

## Rust

```rust
use std::collections::HashMap;

fn main() {
    let mut scores: HashMap<&str, u32> = HashMap::new();
    scores.insert("Alice", 99);
    scores.insert("Bob", 87);

    for (name, score) in &scores {
        println!("{name}: {score}");
    }
}
```

## Python

```python
from dataclasses import dataclass

@dataclass
class Post:
    title: str
    tags: list[str]

    def slug(self) -> str:
        return self.title.lower().replace(" ", "-")

posts = [Post("Hello World", ["intro"]), Post("Rust Tips", ["rust"])]
for p in posts:
    print(f"{p.slug()}: {p.tags}")
```

## JavaScript

```javascript
async function fetchPosts(base) {
  const res = await fetch(`${base}/api/posts-page-1.json`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const { posts } = await res.json();
  return posts.filter((p) => p.status === "published");
}
```

## HTML with special characters

Code blocks that contain HTML entities are escaped correctly so they render as
source code rather than being interpreted by the browser.

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Blog &mdash; Home</title>
</head>
<body>
  <main class="container">
    <h1>Welcome</h1>
  </main>
</body>
</html>
```

## Shell

```bash
#!/usr/bin/env bash
set -euo pipefail

cargo build --release
echo "Build complete"
blogr build
blogr serve &
```

## TOML (configuration)

```toml
[blog]
title = "My Blog"
author = "Jane Doe"

[build]
output_dir = "dist"
drafts = false
```

## No language specified

A code block without a language identifier falls back to plain escaped text
with no highlighting spans.

```
This block has no language tag.
It is still wrapped in <pre class="highlight"><code> for consistent styling.
```

## Indented code block

The following indented block is also handled — it receives no highlighting but
is properly escaped and wrapped.

    let x = 1;
    let y = 2;
    println!("{}", x + y);
