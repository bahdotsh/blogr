# Blogr

[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub Pages](https://img.shields.io/badge/deploy-GitHub%20Pages-blue.svg)](https://pages.github.com/)

A fast, lightweight static site generator built in Rust for creating and managing blogs. Write in Markdown, preview with a built-in terminal editor, and deploy to GitHub Pages with a single command.

## Features

**Two Site Types**
- **Blog Mode**: Traditional blog with posts, archives, tags, and RSS feeds
- **Personal Mode**: Portfolio/personal website without blog functionality
- Single command initialization for either type
- Theme-specific optimizations for each mode

**Content Creation**
- Write posts in Markdown with YAML frontmatter
- Built-in terminal editor with live preview
- Draft and published post management
- Tag-based organization
- Automatic slug generation

**Site Generation**
- Fast static site builds
- Multiple themes: 7 built-in themes for blogs and personal sites
  - Blog: Minimal Retro, Obsidian, Terminal Candy
  - Personal: Dark Minimal, Musashi, Slate Portfolio, Typewriter (NEW)
- Full-text search with MiniSearch integration
- Syntax highlighting for code blocks
- RSS/Atom feeds (blog mode)
- SEO-friendly output

**Development**
- Live reload development server
- Interactive configuration editor
- Project validation and cleanup tools
- Comprehensive CLI commands

**Deployment**
- One-command GitHub Pages deployment
- Custom domain support with CNAME generation
- Automatic git branch management
- Deployment status checking

**Newsletter System** (optional)
- Email subscription collection via IMAP
- Interactive subscriber approval interface
- Newsletter creation from blog posts or custom content
- SMTP integration for reliable email delivery
- Import/export from popular services (Mailchimp, ConvertKit, etc.)
- REST API for external integrations
- Extensible plugin system

## Installation

**Requirements**
- Rust 1.70+
- Git (for deployment)
- GitHub account (for GitHub Pages deployment)

**Install from source:**
```bash
git clone https://github.com/bahdotsh/blogr.git
cd blogr
cargo install --path blogr-cli
```

**Install from crates.io:**
```bash
cargo install blogr-cli
```

## Quick Start

**1. Create a new blog or personal website**
```bash
# For a traditional blog
blogr init my-blog
cd my-blog

# For a personal website (no blog posts)
blogr init --personal my-portfolio
cd my-portfolio
```

**2. Set up GitHub token** (for deployment)
```bash
export GITHUB_TOKEN=your_github_token
```
Get a token at: https://github.com/settings/tokens (needs `repo` and `workflow` scopes)

**3. Create your first post**
```bash
blogr new "Hello World"
```

**4. Choose a theme (optional)**
```bash
# Use default Minimal Retro theme, or switch to Obsidian
blogr theme set obsidian              # For Obsidian community themes
curl -o static/obsidian.css https://raw.githubusercontent.com/kepano/obsidian-minimal/HEAD/obsidian.css
```

**5. Preview your blog**
```bash
blogr serve
# Opens http://localhost:3000
```

**6. Deploy to GitHub Pages**
```bash
blogr deploy
```

## Commands

**Project Management**
```bash
blogr init [NAME]           # Create new blog
blogr init --personal [NAME] # Create personal website (no blog)
  --github-username USER    # Set GitHub username
  --github-repo REPO        # Set repository name
  --no-github               # Skip GitHub setup

blogr project info          # Show project details
blogr project check         # Validate project
blogr project clean         # Clean build files
```

**Content Management**
```bash
blogr new "Post Title"      # Create new post
  --draft                   # Save as draft
  --tags "rust,web"         # Add tags

blogr list                  # List all posts
  --drafts                  # Show only drafts
  --tag rust                # Filter by tag

blogr edit my-post-slug     # Edit existing post
blogr delete my-post-slug   # Delete post
```

**Development**
```bash
blogr serve                 # Start dev server
  --port 8080               # Custom port
  --open                    # Open browser

blogr build                 # Build static site
  --drafts                  # Include drafts
```

**Deployment**
```bash
blogr deploy                # Deploy to GitHub Pages
  --message "Update"        # Custom commit message
```

**Configuration**
```bash
blogr config edit          # Interactive config editor
blogr config get blog.title # Get config value
blogr config set blog.title "My Blog" # Set config value

# Domain setup
blogr config domain set example.com     # Set custom domain
blogr config domain list                # List domains
```

**Newsletter** (optional)
```bash
# Subscriber management
blogr newsletter fetch-subscribers      # Fetch from email inbox
blogr newsletter approve                 # Launch approval UI
blogr newsletter list                    # List all subscribers
blogr newsletter export --format csv    # Export subscribers

# Newsletter sending
blogr newsletter send-latest             # Send with latest post
blogr newsletter send-custom "Subject" "Content"  # Send custom
blogr newsletter draft-latest            # Preview latest post
blogr newsletter test user@example.com   # Send test email

# Import from services
blogr newsletter import --source mailchimp subscribers.csv
blogr newsletter import --source convertkit subscribers.json

# API server for integrations
blogr newsletter api-server --port 3001 --api-key secret
```

## Terminal Editor

Blogr includes a built-in terminal editor for writing posts:

**Editor Controls**
- `i` - Insert mode (start typing)
- `Esc` - Normal mode  
- `p` - Preview mode
- `Tab` - Switch between editor and preview
- `s` - Save post
- `q` - Quit
- `h` - Show help

**Features**
- Live markdown preview
- Syntax highlighting  
- Split-pane view
- Auto-save

**Configuration Editor**
- `blogr config edit` - Interactive config editor
- `↑/↓` - Navigate settings
- `Enter` - Edit field
- `s` - Save changes

## Project Structure

```
my-blog/
├── blogr.toml              # Configuration
├── posts/                  # Markdown posts
│   ├── welcome.md          # Sample post
│   └── about.md            # About page
├── static/                 # Static files
│   ├── images/
│   ├── css/
│   └── js/
└── .github/workflows/      # GitHub Actions (auto-generated)
    └── deploy.yml
```

## Configuration

Edit `blogr.toml` to configure your site:

```toml
[blog]
title = "My Blog"
author = "Your Name"
description = "My thoughts and ideas"
base_url = "https://yourusername.github.io/blog"

[theme]
name = "minimal-retro"  # or "dark-minimal" for personal sites

[theme.config]
# Dark Minimal theme options (for personal sites)
show_status_bar = true
status_text = "Available for opportunities"
status_color = "#00ff88"
enable_animations = true
show_social_icons = true

[github]
username = "yourusername"
repository = "blog"

[build]
output_dir = "dist"

[site]
site_type = "blog"  # or "personal" for portfolio sites

[search]
enabled = true
fields = ["title", "tags", "content"]
exclude = ["drafts/"]
max_content_chars = 2000
excerpt_words = 30
minify = true
lazy_load = true
remove_stopwords = false

# Optional: Custom field boost weights for search scoring
[search.field_boosts]
title = 5.0
tags = 3.0
content = 1.0

# Newsletter configuration (optional)
[newsletter]
enabled = false
subscribe_email = "subscribe@yourdomain.com"
sender_name = "Your Blog Name"
confirmation_subject = "Welcome to Your Blog Newsletter"

# IMAP settings for fetching subscribers
[newsletter.imap]
server = "imap.gmail.com"
port = 993
username = "subscribe@yourdomain.com"
use_tls = true

# SMTP settings for sending emails
[newsletter.smtp]
server = "smtp.gmail.com"
port = 587
username = "subscribe@yourdomain.com"
use_tls = true

# Plugin configurations
[newsletter.plugins.analytics]
enabled = false
api_key = "your-analytics-key"

[newsletter.plugins.webhook]
enabled = false
url = "https://yoursite.com/webhook"
```

**Custom domains:** Use `blogr config domain set yourdomain.com` to configure.

## Post Format

Posts use Markdown with YAML frontmatter:

```markdown
---
title: "My Blog Post"
date: "2024-01-15"
author: "Your Name"
description: "Brief description"
tags: ["rust", "programming"]
status: "published"
slug: "my-blog-post"
---

# My Blog Post

Your content goes here in **Markdown**.

```rust
fn main() {
    println!("Hello, world!");
}
```
```

## Personal Website Content

For personal mode (`--personal`), use `content.md` with frontmatter to define your site:

```markdown
---
title: "Your Name"
description: "Your tagline or role"
author: "Your Name"
theme: "typewriter"
theme_config:
  show_paper_texture: true
  typing_animation: true
sections:
  about:
    title: "About Me"
    content: |
      <p>Your introduction here...</p>
    tagline: "One keystroke at a time"

  contact:
    title: "Get In Touch"
    text: "Let's connect!"
    email: "you@example.com"
    social:
      github: "https://github.com/yourusername"
      twitter: "https://twitter.com/yourusername"
      linkedin: "https://linkedin.com/in/yourusername"
      blog: "https://yourblog.com"  # NEW in v0.4.0
---
```

**Note**: In personal mode, the `title` and `description` from `content.md` will override the values in `blogr.toml`.

## Search

Blogr includes a powerful client-side full-text search feature powered by MiniSearch. Search is enabled by default and works entirely in the browser without requiring a server.

### Features

- **Full-text search**: Search across post titles, tags, and content
- **Real-time results**: Instant search results as you type
- **Keyboard navigation**: Use `/` to focus search, arrow keys to navigate, Enter to open
- **Smart excerpts**: Highlighted search terms in result snippets
- **Lazy loading**: Search index and library loaded only when needed (configurable)
- **Responsive design**: Works seamlessly on desktop and mobile

### Usage

Once your site is built and deployed, readers can:

1. **Quick search**: Use the search bar in the site header
2. **Keyboard shortcut**: Press `/` to focus the search input
3. **Navigate results**: Use arrow keys to navigate, Enter to open a result
4. **Tag search**: Click on tags in search results to search by that tag
5. **Clear search**: Press Escape to clear and hide results

### Configuration Options

The search feature can be customized in your `blogr.toml`:

```toml
[search]
# Enable or disable search entirely
enabled = true

# Fields to include in search index
fields = ["title", "tags", "content"]

# Paths to exclude from indexing (relative to posts directory)
exclude = ["drafts/", "private/"]

# Maximum characters to include from post content
max_content_chars = 2000

# Number of words to include in search result excerpts
excerpt_words = 30

# Whether to minify the search index JSON (recommended for production)
minify = true

# Whether to lazy-load search assets (improves initial page load)
lazy_load = true

# Whether to remove common English stopwords (experimental)
remove_stopwords = false

# Custom field boost weights for search scoring
[search.field_boosts]
title = 5.0    # Matches in titles are weighted 5x
tags = 3.0     # Matches in tags are weighted 3x  
content = 1.0  # Matches in content have base weight
```

### Performance

- **Index size**: Typically 10-50KB for small to medium blogs
- **Load time**: < 100ms for initial search setup with lazy loading
- **Search speed**: < 10ms for typical queries on 100+ posts
- **Browser support**: Works in all modern browsers (IE11+)

### Customization

Search UI can be customized by modifying theme templates:

- Search input styling via CSS classes `.search-input`, `.search-results`
- Result item layout in theme templates
- Search behavior by modifying the generated `search.js`

### Troubleshooting

**Search not working?**
- Ensure `search.enabled = true` in your config
- Check that `search_index.json` exists in your `dist/` folder after build
- Verify JavaScript is enabled in the browser

**Large index files?**
- Reduce `max_content_chars` to limit content per post
- Enable `minify = true` to compress the JSON
- Consider adding more paths to `exclude` for draft content

## Themes

Blogr comes with multiple built-in themes, each designed for different purposes:

### Available Themes

**Minimal Retro** (default for blogs)
- Clean, artistic design
- Retro color scheme
- Expandable post previews
- Mobile responsive
- Syntax highlighting

**Obsidian** (community themes support)
- Uses authentic Obsidian workspace structure
- Compatible with any Obsidian community theme CSS
- Familiar note-taking interface
- Callouts, backlinks, and embedded content
- Dark/light mode with system detection

**Terminal Candy** (quirky personal sites)
- Terminal-inspired design with pastel colors
- Glitch effects and playful animations
- ASCII art decorations
- Typewriter animations
- Perfect for creative personal websites

**Dark Minimal** (default for personal sites)
- Dark minimalist-maximalist aesthetic
- Cyberpunk/brutalist design
- Neon accent colors (green, magenta, cyan)
- Animated grid background
- Auto-glitching gradient text
- Geometric shapes and clip-paths
- Dramatic shadows and hover effects
- Customizable status bar
- Perfect for portfolios and personal brands

**Musashi** (dynamic personal sites)
- Modern dynamic content loading
- Smooth animations and transitions
- Clean typography
- Responsive design
- Perfect for personal websites and project showcases

**Slate Portfolio** (professional portfolios)
- Modern glassmorphic design
- Frosted glass effects
- Elegant transitions
- Professional layout
- Perfect for freelancers and professionals

**Typewriter** (NEW in v0.4.0 - vintage personal sites)
- Vintage typewriter-inspired aesthetics
- Cream paper background with subtle texture
- Monospace Courier font family
- Typewriter typing animation for title
- Blinking cursor effect
- Vintage date stamp
- Typewriter-style line separators
- Perfect for writers, bloggers, and literary portfolios

### Theme Commands
```bash
blogr theme list              # Show available themes
blogr theme set minimal-retro # Switch to Minimal Retro theme
blogr theme set obsidian      # Switch to Obsidian theme
blogr theme set terminal-candy # Switch to Terminal Candy theme
blogr theme set dark-minimal  # Switch to Dark Minimal theme
blogr theme set musashi       # Switch to Musashi theme
blogr theme set slate-portfolio # Switch to Slate Portfolio theme
blogr theme set typewriter    # Switch to Typewriter theme
blogr theme info typewriter   # Show theme configuration options
```

### Dark Minimal Theme Configuration

The Dark Minimal theme includes extensive customization options:

```toml
[theme]
name = "dark-minimal"

[theme.config]
# Colors
primary_color = "#00ff88"           # Neon green accent
secondary_color = "#ff00ff"         # Magenta accent
accent_color = "#00d4ff"            # Cyan accent
background_color = "#0a0a0a"        # Pure dark background
text_color = "#e0e0e0"              # Soft white text

# Typography
font_family = "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"

# Features
enable_animations = true            # Smooth animations
show_social_icons = true            # Display social media links

# Status Bar (customizable availability indicator)
show_status_bar = true              # Show/hide status bar
status_text = "Available for opportunities"  # Custom text
status_color = "#00ff88"            # Dot color (any hex code)
```

**Status Bar Examples:**
```toml
# Available for work
status_text = "Available for hire"
status_color = "#00ff88"  # Green

# Currently busy
status_text = "Currently working on exciting projects"
status_color = "#00d4ff"  # Cyan

# Not available
status_text = "Not accepting new projects"
status_color = "#ff4444"  # Red

# Custom message
status_text = "Building cool stuff"
status_color = "#ff00ff"  # Magenta

# Hide status bar
show_status_bar = false
```

### Typewriter Theme Configuration

The Typewriter theme offers vintage customization options:

```toml
[theme]
name = "typewriter"

[theme.config]
# Colors
paper_color = "#f4f1e8"           # Vintage cream paper
ink_color = "#2b2b2b"              # Dark charcoal ink
accent_color = "#8b4513"           # Vintage brown accent

# Typography
font_family = "'Courier Prime', 'Courier New', monospace"

# Visual Effects
show_paper_texture = true          # Subtle paper texture overlay
typing_animation = true            # Typewriter typing animation
show_date_stamp = true             # Vintage date stamp
cursor_blink = true                # Blinking cursor effect
```

### Brutja Theme Configuration

Configure the following fields in `blogr.toml`:
```toml
[theme.config]
css = "static/custom.css"
hero_title = "Benjamin Corey"
hero_subtitle = "Software developer in Philadelphia"
github_username = "bcorey"
linkedin_username = "benjamin-corey-b18b5517a"
```

And set the look of the application with these CSS variables:
```css
:root {
    --bg: #f0ffe3;
    --fg: #7f2172;
    /* should be an opacity of the fg */
    --hint: #7f217244;
    --accent-primary: #a7d3d1;
    --on-accent-primary: var(--fg);
    --accent-secondary: #E20018;
    --on-accent-secondary: white;
    --accent-tertiary: #fbffb5;
    --on-accent-tertiary: var(--fg);
}
```

### Available Themes:

- **Minimal Retro** - Clean, artistic design with retro aesthetics (for blogs)
- **Obsidian** - Adopts Obsidian community themes for familiar note-taking styling (for blogs)
- **Terminal Candy** - Quirky terminal-inspired theme with pastel colors (for blogs/personal)
- **Dark Minimal** - Dark minimalist-maximalist with cyberpunk aesthetics (for personal sites)
- **Musashi** - Dynamic modern theme with smooth animations (for personal sites)
- **Slate Portfolio** - Glassmorphic professional portfolio theme (for personal sites)
- **Typewriter** - Vintage typewriter aesthetics with nostalgic charm (for personal sites)
- **Brutja** A minimal brutalist look (for blogs)

**Obsidian Theme Setup**

The Obsidian theme allows you to use any Obsidian community theme CSS with your blog:

1. **Switch to Obsidian theme:**
   ```bash
   blogr theme set obsidian
   ```

2. **Add an Obsidian community theme:**
   ```bash
   # Download a popular theme (example: Minimal by @kepano)
   curl -o static/obsidian.css https://raw.githubusercontent.com/kepano/obsidian-minimal/HEAD/obsidian.css
   
   # Or use any other Obsidian theme CSS file
   # Save it as static/obsidian.css in your blog directory
   ```

3. **Configure theme options (optional):**
   ```bash
   blogr config edit
   ```
   ```toml
   [theme.config]
   obsidian_css = "static/obsidian.css"  # Path to your Obsidian CSS
   color_mode = "auto"                   # auto | dark | light
   ```

4. **Build and deploy:**
   ```bash
   blogr build
   blogr deploy
   ```

**Popular Obsidian Themes:**
- **Minimal by @kepano**: `https://raw.githubusercontent.com/kepano/obsidian-minimal/HEAD/obsidian.css`
- **California Coast**: Available in Obsidian community themes
- **Atom**: Available in Obsidian community themes
- **Shimmering Focus**: Available in Obsidian community themes

**Obsidian Theme Features:**
- ✅ Full Obsidian workspace HTML structure
- ✅ Supports any Obsidian community theme CSS
- ✅ Automatic dark/light mode detection
- ✅ Fallback styles when CSS fails to load
- ✅ Obsidian-style callouts, tags, and embeds
- ✅ Compatible with existing Blogr functionality

**Custom Themes**
Themes are Rust modules in `blogr-themes/src/`. Each theme provides templates, CSS, and configuration options.

## Newsletter

Blogr includes a comprehensive email newsletter system that allows you to collect subscribers, manage them through an approval interface, and send newsletters based on your blog posts or custom content.

### Features

**Subscriber Management**
- Email subscription collection via IMAP
- Interactive approval interface for subscriber requests
- Export subscribers to CSV/JSON formats
- Import from popular services (Mailchimp, ConvertKit, Substack, Beehiiv)
- REST API for external integrations

**Newsletter Composition**
- Automatic newsletters from latest blog posts
- Custom newsletter creation with Markdown content
- Template-based email rendering
- HTML and text versions generated automatically
- Preview newsletters before sending

**Sending & Delivery**
- SMTP integration for reliable email delivery
- Rate limiting to prevent spam issues
- Test email functionality
- Batch sending with progress tracking

**Plugin System**
- Extensible plugin architecture
- Custom templates and workflows
- Third-party service integrations
- Hook-based event system

### Setup

**1. Enable Newsletter in Configuration**

Add to your `blogr.toml`:
```toml
[newsletter]
enabled = true
subscribe_email = "subscribe@yourdomain.com"
sender_name = "Your Blog Name"
confirmation_subject = "Welcome to Your Blog Newsletter"

# IMAP Configuration for fetching subscribers
[newsletter.imap]
server = "imap.gmail.com"
port = 993
username = "subscribe@yourdomain.com"
use_tls = true

# SMTP Configuration for sending emails
[newsletter.smtp]
server = "smtp.gmail.com"
port = 587
username = "subscribe@yourdomain.com"
use_tls = true
```

**2. Set Environment Variables**

```bash
# IMAP password for fetching subscription emails
export NEWSLETTER_IMAP_PASSWORD="your-imap-password"

# SMTP password for sending emails
export NEWSLETTER_SMTP_PASSWORD="your-smtp-password"
```

**3. Create Email Account**

Set up a dedicated email address for newsletter subscriptions:
- Gmail: Create app-specific password for IMAP/SMTP access
- Other providers: Ensure IMAP/SMTP access is enabled

### Commands

**Subscriber Management**
```bash
# Fetch new subscribers from email inbox
blogr newsletter fetch-subscribers
blogr newsletter fetch-subscribers --interactive  # Configure IMAP interactively

# Launch approval UI to manage subscriber requests
blogr newsletter approve

# List all subscribers
blogr newsletter list
blogr newsletter list --status approved    # Filter by status
blogr newsletter list --status pending

# Remove a subscriber
blogr newsletter remove user@example.com
blogr newsletter remove user@example.com --force  # Skip confirmation

# Export subscribers
blogr newsletter export --format csv --output subscribers.csv
blogr newsletter export --format json --status approved
```

**Newsletter Creation & Sending**
```bash
# Send newsletter with latest blog post
blogr newsletter send-latest
blogr newsletter send-latest --interactive  # Interactive confirmation

# Send custom newsletter
blogr newsletter send-custom "Weekly Update" "# This Week\n\nHere's what's new..."
blogr newsletter send-custom "Weekly Update" "content" --interactive

# Preview newsletters without sending
blogr newsletter draft-latest                    # Preview latest post
blogr newsletter draft-custom "Subject" "Content"  # Preview custom content

# Send test email
blogr newsletter test user@example.com
blogr newsletter test user@example.com --interactive
```

**Import & Export**
```bash
# Import from popular services
blogr newsletter import --source mailchimp subscribers.csv
blogr newsletter import --source convertkit subscribers.json
blogr newsletter import --source substack subscribers.csv
blogr newsletter import --source beehiiv subscribers.csv
blogr newsletter import --source generic subscribers.csv

# Preview imports before applying
blogr newsletter import --source mailchimp --preview subscribers.csv
blogr newsletter import --source generic --preview --email-column "email" --name-column "name" subscribers.csv

# Custom column mapping for generic CSV
blogr newsletter import --source generic \
  --email-column "email_address" \
  --name-column "full_name" \
  --status-column "subscription_status" \
  subscribers.csv
```

**Plugin System**
```bash
# List available plugins
blogr newsletter plugin list

# Get plugin information
blogr newsletter plugin info analytics-plugin

# Enable/disable plugins
blogr newsletter plugin enable webhook-plugin
blogr newsletter plugin disable analytics-plugin

# Run custom plugin commands
blogr newsletter plugin run sync-external
blogr newsletter plugin run generate-report pdf
```

**API Server**
```bash
# Start API server for external integrations
blogr newsletter api-server

# Custom configuration
blogr newsletter api-server --port 8080 --host 0.0.0.0
blogr newsletter api-server --api-key your-secret-key
blogr newsletter api-server --port 3001 --no-cors
```

### Configuration Options

**Basic Newsletter Settings**
```toml
[newsletter]
# Enable/disable newsletter functionality
enabled = true

# Email address for newsletter subscriptions
subscribe_email = "subscribe@yourdomain.com"

# Name displayed in newsletter emails
sender_name = "Your Blog Name"

# Subject line for confirmation emails
confirmation_subject = "Welcome to Your Blog Newsletter"
```

**IMAP Configuration**
```toml
[newsletter.imap]
server = "imap.gmail.com"     # IMAP server address
port = 993                    # IMAP port (993 for SSL, 143 for non-SSL)
username = "your-email@domain.com"  # IMAP username
use_tls = true               # Enable TLS/SSL encryption
```

**SMTP Configuration**
```toml
[newsletter.smtp]
server = "smtp.gmail.com"     # SMTP server address
port = 587                    # SMTP port (587 for TLS, 465 for SSL, 25 for non-encrypted)
username = "your-email@domain.com"  # SMTP username
use_tls = true               # Enable TLS/SSL encryption
```

**Plugin Configuration**
```toml
[newsletter.plugins.analytics]
enabled = true
api_key = "your-analytics-api-key"
endpoint = "https://api.analytics.com"

[newsletter.plugins.webhook]
enabled = true
url = "https://yoursite.com/webhook"
events = ["subscriber_approved", "newsletter_sent"]
```

### Email Provider Setup

**Gmail**
1. Enable 2-factor authentication on your Google account
2. Generate an app-specific password:
   - Go to Google Account settings → Security → App passwords
   - Generate password for "Mail" application
3. Use app-specific password for both IMAP and SMTP

**Outlook/Hotmail**
1. Enable 2-factor authentication
2. Generate app password in security settings
3. Use app password for authentication

**Other Providers**
- Ensure IMAP and SMTP access is enabled
- Use provider-specific server settings
- Some providers may require app-specific passwords

### API Integration

The newsletter system includes a REST API for external integrations:

**Start API Server:**
```bash
blogr newsletter api-server --port 3001 --api-key your-secret-key
```

**Available Endpoints:**
- `GET /health` - Health check
- `GET /subscribers` - List subscribers
- `POST /subscribers` - Create subscriber
- `GET /subscribers/:email` - Get specific subscriber
- `PUT /subscribers/:email` - Update subscriber
- `DELETE /subscribers/:email` - Remove subscriber
- `GET /stats` - Get newsletter statistics
- `GET /export` - Export subscribers

**Example Usage:**
```bash
# List approved subscribers
curl -H "Authorization: Bearer your-secret-key" \
     "http://localhost:3001/subscribers?status=approved"

# Add new subscriber
curl -X POST -H "Authorization: Bearer your-secret-key" \
     -H "Content-Type: application/json" \
     -d '{"email":"user@example.com","status":"pending"}' \
     http://localhost:3001/subscribers
```

### Plugin Development

Create custom plugins to extend newsletter functionality:

```rust
use blogr_cli::newsletter::{NewsletterPlugin, PluginMetadata, PluginHook};

pub struct MyPlugin {
    metadata: PluginMetadata,
}

impl NewsletterPlugin for MyPlugin {
    fn handles_hook(&self, hook: &PluginHook) -> bool {
        matches!(hook, PluginHook::PreSend | PluginHook::PostSend)
    }

    fn execute_hook(&self, context: &PluginContext) -> Result<PluginResult> {
        // Custom plugin logic
        Ok(PluginResult::success("Plugin executed"))
    }
}
```

**Available Hooks:**
- `PreFetch` - Before fetching subscribers
- `PostFetch` - After fetching subscribers
- `PreApprove` - Before approving subscribers
- `PostApprove` - After approving subscribers
- `PreCompose` - Before composing newsletter
- `PostCompose` - After composing newsletter
- `PreSend` - Before sending newsletter
- `PostSend` - After sending newsletter

### Troubleshooting

**Common Issues:**

1. **IMAP Connection Failed**
   - Verify server settings and credentials
   - Enable "Less secure app access" or use app-specific passwords
   - Check firewall/network connectivity

2. **SMTP Sending Failed**
   - Verify SMTP server settings
   - Check authentication credentials
   - Ensure port is not blocked by firewall

3. **No Subscribers Found**
   - Run `blogr newsletter fetch-subscribers` first
   - Check that subscription emails are being received
   - Verify IMAP folder contains emails

4. **Newsletter Not Rendering**
   - Check that blog posts exist and are published
   - Verify theme templates are working
   - Test with `blogr newsletter draft-latest`

For detailed API documentation, see [NEWSLETTER_API.md](NEWSLETTER_API.md).
For plugin development guide, see [NEWSLETTER_PLUGINS.md](NEWSLETTER_PLUGINS.md).

## Deployment

**GitHub Pages (recommended)**

1. Set up GitHub token:
   ```bash
   export GITHUB_TOKEN=your_token
   ```
   Get a token at https://github.com/settings/tokens (needs `repo` and `workflow` scopes)

2. Deploy:
   ```bash
   blogr deploy
   ```

Your blog will be available at `https://yourusername.github.io/repository`

**Custom Domains**

1. Configure domain:
   ```bash
   blogr config domain set yourdomain.com
   ```

2. Set up DNS records:
   - **A records** for apex domains: `185.199.108.153`, `185.199.109.153`, `185.199.110.153`, `185.199.111.153`
   - **CNAME record** for subdomains: `yourusername.github.io`

3. Deploy and configure in GitHub repository Settings → Pages → Custom domain

**Manual Deployment**
```bash
blogr build
# Copy contents of `dist/` folder to your web server
```

## Development

**Build from source:**
```bash
git clone https://github.com/bahdotsh/blogr.git
cd blogr
cargo build --release
```

**Run tests:**
```bash
cargo test
```

**Code structure:**
- `blogr-cli/` - Main CLI application
- `blogr-themes/` - Theme system

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

**Areas to help:**
- New themes
- Features and bug fixes
- Documentation
- Testing

## Changelog

### v0.4.0 (Latest)

**New Features:**
- 🎨 **Typewriter Theme**: Vintage typewriter-inspired theme for personal websites with nostalgic aesthetics
- 🔗 **Blog Link Support**: All personal website themes now support a `blog` link in social links
- 📝 **Content.md Override**: Personal mode now uses `title` and `description` from `content.md` instead of `blogr.toml`
- ✨ **Conditional Separators**: Typewriter theme displays separator lines only when sections are present

**Improvements:**
- Better theme organization with clear separation between blog and personal themes
- Enhanced personal website customization options
- Improved documentation for all themes

**Themes:**
- Blog themes: Minimal Retro, Obsidian, Terminal Candy
- Personal themes: Dark Minimal, Musashi, Slate Portfolio, Typewriter

### Previous Versions

See [blogr-themes/README.md](blogr-themes/README.md) for detailed theme changelog.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Links

- [Issues](https://github.com/bahdotsh/blogr/issues)
- [Contributing](CONTRIBUTING.md)

---

Built with Rust 🦀
