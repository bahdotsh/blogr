# Blogr Commands

This document provides a comprehensive reference for all Blogr CLI commands.

## Project Management

### Create a new blog or personal website
```bash
blogr init my-blog                    # Create new blog
blogr init --personal my-portfolio    # Create personal website
blogr init --path /some/dir my-blog   # Create in specific directory
blogr init --no-github my-blog        # Skip GitHub repository creation
blogr init --github-username USER --github-repo REPO  # Set GitHub details
```

### Project information
```bash
blogr project info                    # Show project details
blogr project check                   # Validate project
blogr project clean                   # Clean build files
blogr project stats                   # Show project statistics
```

## Content Management

### Create and manage posts
```bash
blogr new "My Post Title"             # Create new post
blogr new "Draft Post" --draft        # Create draft post
blogr new "Tagged Post" --tags "rust,web"  # Create post with tags
blogr new "My Post" --slug custom-slug    # Custom URL slug
blogr new "My Post" --tui                 # Open in TUI editor
blogr new "My Post" --template post       # Use specific template
blogr new "External" --external-url "https://example.com/article"  # External post
```

### List and edit posts
```bash
blogr list                            # List all posts
blogr list --drafts                   # Show only drafts
blogr list --published                # Show only published posts
blogr list --tag rust                 # Filter by tag
blogr list --sort title               # Sort by title (default: date)
blogr edit my-post-slug               # Edit existing post
blogr edit my-post-slug --tui         # Edit in TUI editor
blogr delete my-post-slug             # Delete post
blogr delete my-post-slug -f          # Delete without confirmation
```

## Development

### Development server
```bash
blogr serve                           # Start dev server (localhost:3000)
blogr serve --port 8080              # Custom port
blogr serve --host 0.0.0.0          # Bind to all interfaces
blogr serve --open                    # Open browser automatically
blogr serve --drafts                  # Include draft posts
```

### Build static site
```bash
blogr build                           # Build static site
blogr build --drafts                  # Include drafts in build
blogr build --future                  # Include future-dated posts
blogr build --output /path/to/dir     # Custom output directory
```

## Deployment

### Deploy to GitHub Pages
```bash
blogr deploy                          # Deploy to GitHub Pages
blogr deploy --message "Update"       # Custom commit message
blogr deploy --branch gh-pages        # Deploy branch (default: gh-pages)
```

## Configuration

### Interactive configuration
```bash
blogr config edit                     # Interactive config editor
blogr config get blog.title           # Get config value
blogr config set blog.title "My Blog" # Set config value
```

### Domain setup
```bash
blogr config domain set example.com               # Set custom domain
blogr config domain set --subdomain blog           # Set subdomain
blogr config domain set example.com --github-pages # Create CNAME for GitHub Pages
blogr config domain set example.com --enforce-https false  # Disable HTTPS enforcement
blogr config domain list                           # List domains
blogr config domain clear                          # Clear all domain configuration
blogr config domain add-alias alias.example.com    # Add domain alias
blogr config domain remove-alias alias.example.com # Remove domain alias
```

## Theme Commands

### Theme management
```bash
# List all available themes
blogr theme list

# Switch to a specific theme
blogr theme set minimal-retro     # Blog theme
blogr theme set obsidian          # Blog theme
blogr theme set terminal-candy    # Blog theme
blogr theme set brutja            # Blog theme
blogr theme set dark-minimal      # Personal theme
blogr theme set musashi          # Personal theme
blogr theme set slate-portfolio  # Personal theme
blogr theme set typewriter       # Personal theme

# Get theme information
blogr theme info typewriter      # Show theme configuration options

# Preview theme in TUI
blogr theme preview typewriter   # Preview theme in terminal
```

## Newsletter Commands

### Subscriber Management
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

### Newsletter Creation & Sending
```bash
# Send newsletter with latest blog post
blogr newsletter send-latest
blogr newsletter send-latest --interactive  # Interactive confirmation
blogr newsletter send-latest --tags "rust"  # Send only to tagged subscribers

# Send custom newsletter
blogr newsletter send-custom "Weekly Update" "# This Week\n\nHere's what's new..."
blogr newsletter send-custom "Weekly Update" "content" --interactive
blogr newsletter send-custom "Weekly Update" "content" --tags "rust"

# Preview newsletters without sending
blogr newsletter draft-latest                    # Preview latest post
blogr newsletter draft-custom "Subject" "Content"  # Preview custom content

# Send test email
blogr newsletter test user@example.com
blogr newsletter test user@example.com --interactive
```

### Subscriber Tags
```bash
# Tag a subscriber
blogr newsletter tag user@example.com --add "rust"
blogr newsletter tag user@example.com --remove "rust"

# List all tags with subscriber counts
blogr newsletter tags
```

### Cleanup
```bash
# Clean up old send data (default: older than 90 days)
blogr newsletter cleanup
blogr newsletter cleanup --days 30
```

### Import & Export
```bash
# Import from popular services
blogr newsletter import --source mailchimp subscribers.csv
blogr newsletter import --source convertkit subscribers.json
blogr newsletter import --source substack subscribers.csv
blogr newsletter import --source beehiiv subscribers.csv
blogr newsletter import --source generic subscribers.csv

# Preview imports before applying
blogr newsletter import --source mailchimp --preview subscribers.csv
blogr newsletter import --source mailchimp --preview --preview-limit 20 subscribers.csv
blogr newsletter import --source generic --preview --email-column "email" --name-column "name" subscribers.csv

# Custom column mapping for generic CSV
blogr newsletter import --source generic \
  --email-column "email_address" \
  --name-column "full_name" \
  --status-column "subscription_status" \
  subscribers.csv
```

### Plugin System
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

### API Server
```bash
# Start API server for external integrations
blogr newsletter api-server

# Custom configuration
blogr newsletter api-server --port 8080 --host 0.0.0.0
blogr newsletter api-server --api-key your-secret-key
blogr newsletter api-server --port 3001 --no-cors
```
