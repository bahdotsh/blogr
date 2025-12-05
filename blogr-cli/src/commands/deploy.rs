use crate::config::{DeploymentType, EnvConfig};
use crate::generator::SiteBuilder;
use crate::project::Project;
use crate::utils::Console;
use anyhow::{anyhow, Context, Result};
use blogr_themes::SiteType;
use git2::{BranchType, Repository, Signature};
use std::fs;
use std::path::Path;
use uuid::Uuid;
use walkdir::WalkDir;

pub async fn handle_deploy(branch: String, message: Option<String>) -> Result<()> {
    Console::info(&format!("Deploying to GitHub Pages (branch: {branch})"));

    // Validate GitHub token early
    Console::step(1, 7, "Validating GitHub token...");
    let github_token = EnvConfig::github_token()
        .ok_or_else(|| anyhow!("GitHub token not found. Set GITHUB_TOKEN environment variable."))?;

    if github_token.is_empty() {
        anyhow::bail!(
            "GitHub token is empty. Please set a valid GITHUB_TOKEN environment variable."
        );
    }

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load configuration BEFORE any git operations to preserve current settings
    let mut config = project.load_config()?;

    // Ensure URL configuration consistency
    config.sync_base_url_with_domains();

    let github_config = config.github.as_ref()
        .ok_or_else(|| anyhow!("GitHub configuration not found. Initialize with GitHub integration or configure manually."))?;

    // For personal mode, read content.md BEFORE stashing to preserve uncommitted changes
    let content_md = if config.site.site_type == SiteType::Personal {
        let content_md_path = project.root.join("content.md");
        if content_md_path.exists() {
            Some(
                fs::read_to_string(&content_md_path)
                    .with_context(|| "Failed to read content.md")?,
            )
        } else {
            None
        }
    } else {
        None
    };

    Console::step(2, 7, "Preparing git repository...");

    // Open the git repository
    let mut repo = Repository::open(&project.root)
        .with_context(|| "Failed to open git repository. Ensure this is a git repository.")?;

    // Check if working directory has uncommitted changes and handle them automatically
    let has_uncommitted_changes = {
        let statuses = repo.statuses(None)?;
        // Check for actual changes that would affect stashing
        statuses.iter().any(|entry| {
            let flags = entry.status();
            // Check for modified, added, deleted, renamed, or typechange files in working tree or index
            flags.contains(git2::Status::WT_MODIFIED)
                || flags.contains(git2::Status::WT_DELETED)
                || flags.contains(git2::Status::WT_TYPECHANGE)
                || flags.contains(git2::Status::WT_RENAMED)
                || flags.contains(git2::Status::WT_NEW)
                || flags.contains(git2::Status::INDEX_MODIFIED)
                || flags.contains(git2::Status::INDEX_NEW)
                || flags.contains(git2::Status::INDEX_DELETED)
                || flags.contains(git2::Status::INDEX_RENAMED)
                || flags.contains(git2::Status::INDEX_TYPECHANGE)
        })
    };

    let mut stash_id = None;
    if has_uncommitted_changes {
        Console::info("Working directory has uncommitted changes. Auto-stashing for deployment...");

        // Auto-stash uncommitted changes (they will be restored after deployment)
        let signature = get_git_signature()?;
        let id = repo.stash_save(&signature, "Auto-stash before deployment", None)?;
        stash_id = Some(id);
        Console::info(&format!("Changes stashed with ID: {}", id));
    }

    Console::step(3, 7, "Building site...");

    // Build the site AFTER handling git state to ensure build directory exists
    // Use a temporary directory outside the project to avoid conflicts
    let temp_output = std::env::temp_dir().join(format!("blogr-deploy-{}", Uuid::new_v4()));
    // Use the pre-loaded config and content.md to avoid issues with git stashing
    let site_builder = SiteBuilder::new_with_config_and_content(
        project.clone(),
        config.clone(),
        content_md,
        Some(temp_output.clone()),
        false,
        false,
    )?;
    site_builder.build()?;

    Console::step(4, 7, &format!("Preparing {} branch...", branch));

    // Create a unique temporary directory for deployment worktree
    let mut temp_deploy_dir;
    let mut attempts = 0;
    loop {
        temp_deploy_dir =
            std::env::temp_dir().join(format!("blogr-deploy-worktree-{}", Uuid::new_v4()));

        // Ensure directory doesn't exist
        if temp_deploy_dir.exists() {
            fs::remove_dir_all(&temp_deploy_dir)?;
        }

        // Don't create the directory - let git2 create it
        if !temp_deploy_dir.exists() {
            break;
        }

        attempts += 1;
        if attempts > 5 {
            anyhow::bail!(
                "Failed to create unique temporary directory after {} attempts",
                attempts
            );
        }
    }

    // Ensure deployment branch exists
    let deploy_branch_exists = repo.find_branch(&branch, BranchType::Local).is_ok();

    if !deploy_branch_exists {
        Console::info(&format!("Creating new deployment branch '{}'...", branch));
        // Create orphan branch for GitHub Pages
        create_orphan_branch(&repo, &branch)?;
    } else {
        Console::info(&format!("Using existing deployment branch '{}'...", branch));
    }

    // Verify the branch was created/exists
    if repo.find_branch(&branch, BranchType::Local).is_err() {
        anyhow::bail!("Failed to create or find deployment branch '{}'", branch);
    }

    // Clean up any existing worktrees for this branch
    if let Ok(worktrees) = repo.worktrees() {
        for worktree_name in worktrees.iter().flatten() {
            if let Ok(worktree) = repo.find_worktree(worktree_name) {
                // Check if this worktree is for our deployment branch by checking its name pattern
                if worktree_name.starts_with(&format!("{}-", branch)) || worktree_name == branch {
                    Console::info(&format!(
                        "Cleaning up existing worktree '{}' for branch '{}'...",
                        worktree_name, branch
                    ));

                    // Get the worktree path before pruning
                    let worktree_path = worktree.path().to_path_buf();

                    if let Err(e) = worktree.prune(None) {
                        Console::warn(&format!(
                            "Warning: Could not prune existing worktree '{}': {}",
                            worktree_name, e
                        ));
                    }

                    // Also manually clean up the directory if it still exists
                    if worktree_path.exists() {
                        if let Err(e) = fs::remove_dir_all(&worktree_path) {
                            Console::warn(&format!(
                                "Warning: Could not remove worktree directory '{}': {}",
                                worktree_path.display(),
                                e
                            ));
                        }
                    }
                }
            }
        }
    }

    // Create a worktree for the deployment branch
    // Use a specific worktree name to avoid conflicts
    let worktree_name = format!(
        "{}-{}",
        branch,
        Uuid::new_v4()
            .to_string()
            .split('-')
            .next()
            .unwrap_or("tmp")
    );

    // Create worktree pointing to the deployment branch
    let branch_ref = repo
        .find_reference(&format!("refs/heads/{}", branch))
        .with_context(|| format!("Could not find reference for branch '{}'", branch))?;
    let mut opts = git2::WorktreeAddOptions::new();
    opts.reference(Some(&branch_ref));
    let worktree_result = repo.worktree(&worktree_name, &temp_deploy_dir, Some(&opts));

    worktree_result.with_context(|| {
        format!(
            "Failed to create worktree '{}' for branch '{}' at '{}'",
            worktree_name,
            branch,
            temp_deploy_dir.display()
        )
    })?;

    let deploy_repo = Repository::open(&temp_deploy_dir)?;

    Console::step(5, 7, "Copying built files...");

    // Clear the deployment worktree and copy built files
    clear_deployment_branch(&temp_deploy_dir)?;
    copy_site_files(&temp_output, &temp_deploy_dir)?;

    // Smart CNAME file creation based on deployment type
    let deployment_type = config.get_deployment_type();

    // Create CNAME file for custom domains
    let cname_created = match deployment_type {
        DeploymentType::CustomDomain => {
            let effective_url = config.get_effective_base_url();
            if let Ok(url) = url::Url::parse(&effective_url) {
                if let Some(host) = url.host_str() {
                    let cname_path = temp_deploy_dir.join("CNAME");
                    fs::write(cname_path, format!("{}\n", host))?;
                    Console::info(&format!("Created CNAME file for custom domain: {}", host));
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }
        DeploymentType::GitHubPagesRoot => {
            Console::info("Deploying to GitHub Pages root domain - no CNAME file needed");
            false
        }
        DeploymentType::GitHubPagesSubpath => {
            Console::info("Deploying to GitHub Pages subpath - no CNAME file needed");
            false
        }
        DeploymentType::Unknown => {
            Console::warn(
                "Could not determine deployment type - checking for explicit GitHub Pages domain",
            );
            false
        }
    };

    // Fallback: if no CNAME was created but we have a github_pages_domain configured, use it
    if !cname_created {
        if let Some(domains) = &config.blog.domains {
            if let Some(github_domain) = &domains.github_pages_domain {
                let cname_path = temp_deploy_dir.join("CNAME");
                fs::write(cname_path, format!("{}\n", github_domain))?;
                Console::info(&format!(
                    "Created CNAME file from github_pages_domain: {}",
                    github_domain
                ));
            }
        }
    }

    Console::step(6, 7, "Committing changes...");

    // Stage all files in the deploy repository
    let mut index = deploy_repo.index()?;
    index.add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)?;
    index.write()?;

    // Create commit in the deploy repository
    let deploy_message = message.unwrap_or_else(|| {
        format!(
            "Deploy site - {}",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        )
    });

    let signature = get_git_signature()?;
    let tree_id = index.write_tree()?;
    let tree = deploy_repo.find_tree(tree_id)?;

    // Check if this is the first commit on this branch
    let parent_commit = deploy_repo
        .head()
        .ok()
        .and_then(|head| head.target())
        .and_then(|oid| deploy_repo.find_commit(oid).ok());

    let commit_id = if let Some(parent) = parent_commit {
        deploy_repo.commit(
            Some("HEAD"),
            &signature,
            &signature,
            &deploy_message,
            &tree,
            &[&parent],
        )?
    } else {
        deploy_repo.commit(
            Some("HEAD"),
            &signature,
            &signature,
            &deploy_message,
            &tree,
            &[],
        )?
    };

    Console::step(7, 7, "Pushing to GitHub...");

    // Push to GitHub from the deploy repository
    push_to_github(&deploy_repo, &branch, github_config)?;

    // Restore stashed changes if any were stashed
    if let Some(_stash_id) = stash_id {
        Console::info("Restoring stashed changes...");
        // Create a new repo handle to avoid borrowing conflicts
        let mut restore_repo = Repository::open(&project.root)?;
        match restore_repo.stash_pop(0, None) {
            Ok(_) => Console::info("Successfully restored stashed changes"),
            Err(e) => Console::warn(&format!(
                "Warning: Could not restore stashed changes: {}",
                e
            )),
        }
    }

    // Clean up worktree and temporary directories
    // Note: The worktree will be automatically cleaned up when we remove temp_deploy_dir

    if temp_output.exists() {
        fs::remove_dir_all(&temp_output)?;
    }
    if temp_deploy_dir.exists() {
        fs::remove_dir_all(&temp_deploy_dir)?;
    }

    println!();
    Console::success("🚀 Site deployed to GitHub Pages!");
    println!("🌐 Your site will be available at:");

    println!("   {}", config.blog.base_url);

    println!("📝 Deployment branch: {}", branch);
    println!("📦 Commit: {}", commit_id);
    println!();

    // Check GitHub Pages deployment status
    Console::info("Checking GitHub Pages deployment status...");
    match check_github_pages_status(&github_config.username, &github_config.repository).await {
        Ok(status) => match status.as_str() {
            "built" => Console::success("GitHub Pages is enabled and working"),
            "building" => Console::info("GitHub Pages is currently building"),
            "errored" => Console::warn("GitHub Pages deployment has errors"),
            _ => Console::info(&format!("GitHub Pages status: {}", status)),
        },
        Err(e) => Console::warn(&format!("Could not check GitHub Pages status: {}", e)),
    }

    println!();
    println!("ℹ️  Note: It may take a few minutes for changes to appear on GitHub Pages");

    Ok(())
}

async fn check_github_pages_status(username: &str, repository: &str) -> Result<String> {
    let github_token = EnvConfig::github_token()
        .ok_or_else(|| anyhow!("GitHub token not found. Set GITHUB_TOKEN environment variable."))?;

    let client = reqwest::Client::new();
    let url = format!(
        "https://api.github.com/repos/{}/{}/pages",
        username, repository
    );

    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", github_token))
        .header("Accept", "application/vnd.github.v3+json")
        .header("User-Agent", "blogr-cli")
        .send()
        .await
        .with_context(|| "Failed to check GitHub Pages status")?;

    if response.status() == 404 {
        return Ok("not_configured".to_string());
    }

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GitHub API error ({status}): {body}");
    }

    let pages_info: serde_json::Value = response
        .json()
        .await
        .with_context(|| "Failed to parse GitHub Pages response")?;

    let status = pages_info
        .get("status")
        .and_then(|s| s.as_str())
        .unwrap_or("unknown")
        .to_string();

    Ok(status)
}

fn create_orphan_branch(repo: &Repository, branch_name: &str) -> Result<()> {
    // Create a new orphan branch by creating an empty commit
    let signature = get_git_signature()?;

    // Create an empty tree
    let tree_id = {
        let tree_builder = repo.treebuilder(None)?;
        tree_builder.write()?
    };
    let tree = repo.find_tree(tree_id)?;

    // Create the initial commit on the new branch
    let commit_id = repo.commit(
        Some(&format!("refs/heads/{}", branch_name)),
        &signature,
        &signature,
        "Initial commit for GitHub Pages",
        &tree,
        &[],
    )?;

    // Checkout the new branch
    let commit = repo.find_commit(commit_id)?;
    repo.checkout_tree(commit.as_object(), None)?;
    repo.set_head(&format!("refs/heads/{}", branch_name))?;

    Ok(())
}

fn clear_deployment_branch(project_root: &Path) -> Result<()> {
    for entry in fs::read_dir(project_root)? {
        let entry = entry?;
        let path = entry.path();

        // Skip .git directory
        if path.file_name().unwrap_or_default() == ".git" {
            continue;
        }

        if path.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }

    Ok(())
}

fn copy_site_files(source: &Path, dest: &Path) -> Result<()> {
    for entry in WalkDir::new(source) {
        let entry = entry?;
        let path = entry.path();

        if path == source {
            continue;
        }

        let relative_path = path.strip_prefix(source)?;
        let dest_path = dest.join(relative_path);

        if path.is_dir() {
            fs::create_dir_all(&dest_path)?;
        } else {
            if let Some(parent) = dest_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(path, dest_path)?;
        }
    }

    Ok(())
}

fn get_git_signature() -> Result<Signature<'static>> {
    let name = EnvConfig::git_author_name().unwrap_or_else(|| "Blogr".to_string());
    let email = EnvConfig::git_author_email().unwrap_or_else(|| "blogr@example.com".to_string());

    Signature::now(&name, &email).with_context(|| "Failed to create git signature")
}

fn push_to_github(
    repo: &Repository,
    branch: &str,
    _github_config: &crate::config::GitHubConfig,
) -> Result<()> {
    // Get the existing remote
    let mut remote = repo
        .find_remote("origin")
        .with_context(|| "No 'origin' remote found. Please add a remote first.")?;

    let remote_url = remote.url().unwrap_or("");

    // Force push the branch to avoid conflicts
    let refspec = format!("+refs/heads/{}:refs/heads/{}", branch, branch);

    let mut callbacks = git2::RemoteCallbacks::new();

    // Handle authentication based on remote URL type
    if remote_url.starts_with("git@") || remote_url.starts_with("ssh://") {
        // SSH authentication - use SSH agent or SSH keys
        Console::info("Using SSH authentication for git push...");
        callbacks.credentials(|_url, username_from_url, allowed_types| {
            if allowed_types.contains(git2::CredentialType::SSH_KEY) {
                // Try SSH agent first
                if let Ok(cred) = git2::Cred::ssh_key_from_agent(username_from_url.unwrap_or("git"))
                {
                    return Ok(cred);
                }

                // Try default SSH key locations
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
                let private_key = format!("{}/.ssh/id_rsa", home);
                let public_key = format!("{}/.ssh/id_rsa.pub", home);

                if std::path::Path::new(&private_key).exists() {
                    return git2::Cred::ssh_key(
                        username_from_url.unwrap_or("git"),
                        Some(std::path::Path::new(&public_key)),
                        std::path::Path::new(&private_key),
                        None,
                    );
                }
            }

            Err(git2::Error::from_str("No SSH credentials available"))
        });
    } else if remote_url.starts_with("https://") {
        // HTTPS authentication - use GitHub token
        let github_token = EnvConfig::github_token()
            .ok_or_else(|| anyhow!("GitHub token not found. Set GITHUB_TOKEN environment variable for HTTPS authentication."))?;

        Console::info("Using HTTPS token authentication for git push...");
        let token_clone = github_token.clone();
        callbacks.credentials(move |_url, _username_from_url, _allowed_types| {
            git2::Cred::userpass_plaintext(&token_clone, "")
        });
    } else {
        anyhow::bail!("Unsupported remote URL format: {}", remote_url);
    }

    let mut push_options = git2::PushOptions::new();
    push_options.remote_callbacks(callbacks);

    remote.push(&[&refspec], Some(&mut push_options))?;

    Ok(())
}
