use crate::generator::SiteBuilder;
use crate::project::Project;
use crate::utils::Console;
use anyhow::{anyhow, Result};
use axum::body::Body;
use axum::http::header;
use axum::response::sse::{Event, Sse};
use axum::response::Response;
use axum::Router;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tower_http::services::ServeDir;

const LIVE_RELOAD_SCRIPT: &str = r#"<script>
(function() {
  var source = new EventSource('/__livereload');
  source.onmessage = function(e) {
    if (e.data === 'reload') {
      window.location.reload();
    }
  };
  source.onerror = function() {
    source.close();
    var attempts = 0;
    var maxAttempts = 10;
    function tryReconnect() {
      if (attempts >= maxAttempts) return;
      attempts++;
      setTimeout(function() {
        var retry = new EventSource('/__livereload');
        retry.onmessage = function(e) {
          if (e.data === 'reload') window.location.reload();
        };
        retry.onerror = function() {
          retry.close();
          tryReconnect();
        };
      }, 1000 * Math.min(attempts, 5));
    }
    tryReconnect();
  };
})();
</script>"#;

pub async fn handle_serve(port: u16, host: String, drafts: bool, open: bool) -> Result<()> {
    Console::info(&format!("Starting development server on {}:{}", host, port));

    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Ensure templates generate root-relative URLs in dev (so assets load locally)
    std::env::set_var("BLOGR_DEV", "1");

    Console::info("Building site...");

    let config = project.load_config()?;
    let output_dir = config.build.resolve_output_dir(&project.root);

    let auto_reload = config.dev.auto_reload;

    let site_builder = SiteBuilder::new(project.clone(), Some(output_dir.clone()), drafts, false)?;
    site_builder.build()?;

    let (reload_tx, _) = broadcast::channel::<()>(16);

    if auto_reload {
        start_file_watcher(
            project.clone(),
            output_dir.clone(),
            drafts,
            reload_tx.clone(),
        )?;
    }

    let app = if auto_reload {
        let reload_tx_sse = reload_tx.clone();
        Router::new()
            .route(
                "/__livereload",
                axum::routing::get(move || sse_handler(reload_tx_sse.subscribe())),
            )
            .fallback_service(ServeDir::new(output_dir.clone()))
            .layer(axum::middleware::map_response(inject_live_reload))
    } else {
        Router::new().fallback_service(ServeDir::new(output_dir.clone()))
    };

    Console::success(&format!(
        "Development server running at http://{}:{}",
        host, port
    ));
    println!("  Site built and ready");
    println!("  Server serving from: {}", output_dir.display());
    if drafts {
        println!("  Including draft posts");
    }
    if auto_reload {
        println!("  Live reload enabled");
    }
    println!("Press Ctrl+C to stop");

    if open {
        let url = format!("http://{}:{}", host, port);
        Console::info(&format!("Opening browser to {}", url));
        if let Err(e) = ::open::that(&url) {
            Console::warn(&format!("Failed to open browser: {}", e));
        }
    }

    let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn sse_handler(
    rx: broadcast::Receiver<()>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(()) => Some(Ok(Event::default().data("reload"))),
        Err(_) => None,
    });
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("ping"),
    )
}

async fn inject_live_reload(response: Response) -> Response {
    let is_html = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.contains("text/html"));

    if !is_html {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            Console::warn("Response body too large for live reload injection");
            return Response::from_parts(parts, Body::empty());
        }
    };
    // Remove Content-Length since we're changing the body size
    parts.headers.remove(header::CONTENT_LENGTH);

    let html = String::from_utf8_lossy(&bytes);
    let modified = if let Some(pos) = html.rfind("</body>") {
        let mut result = String::with_capacity(html.len() + LIVE_RELOAD_SCRIPT.len());
        result.push_str(&html[..pos]);
        result.push_str(LIVE_RELOAD_SCRIPT);
        result.push_str(&html[pos..]);
        result
    } else {
        // No </body>, append script at the end
        let mut result = html.into_owned();
        result.push_str(LIVE_RELOAD_SCRIPT);
        result
    };

    Response::from_parts(parts, Body::from(modified))
}

fn start_file_watcher(
    project: Project,
    output_dir: PathBuf,
    drafts: bool,
    reload_tx: broadcast::Sender<()>,
) -> Result<()> {
    use notify_debouncer_mini::{new_debouncer, DebouncedEventKind};
    use std::time::Duration;

    let (notify_tx, notify_rx) = std::sync::mpsc::channel();

    let mut debouncer = new_debouncer(Duration::from_millis(300), notify_tx)?;
    let watcher = debouncer.watcher();

    let watch_dirs = ["posts", "static", "themes"];
    for dir_name in &watch_dirs {
        let dir = project.root.join(dir_name);
        if dir.exists() {
            watcher.watch(&dir, notify::RecursiveMode::Recursive)?;
        }
    }

    let watch_files = ["blogr.toml", "content.md"];
    for file_name in &watch_files {
        let file = project.root.join(file_name);
        if file.exists() {
            watcher.watch(&file, notify::RecursiveMode::NonRecursive)?;
        }
    }

    Console::info("Watching for file changes...");

    let rebuilding = Arc::new(AtomicBool::new(false));
    // Canonicalize output_dir to prevent symlink mismatches in path filtering
    let canonical_output_dir = output_dir.canonicalize().unwrap_or(output_dir);

    // Bridge sync notify events to async rebuild loop
    let (async_tx, mut async_rx) = tokio::sync::mpsc::channel::<()>(1);
    std::thread::spawn(move || {
        // Keep debouncer alive in this thread
        let _debouncer = debouncer;
        loop {
            match notify_rx.recv() {
                Ok(Ok(events)) => {
                    // Filter out events in the output directory
                    let relevant = events.iter().any(|e| {
                        e.kind == DebouncedEventKind::Any
                            && !e
                                .path
                                .canonicalize()
                                .unwrap_or_else(|_| e.path.clone())
                                .starts_with(&canonical_output_dir)
                    });
                    if relevant {
                        let _ = async_tx.try_send(());
                    }
                }
                Ok(Err(errs)) => {
                    Console::warn(&format!("File watch error: {}", errs));
                }
                Err(_) => break, // Channel closed
            }
        }
    });

    let dirty = Arc::new(AtomicBool::new(false));
    let dirty_clone = dirty.clone();
    tokio::spawn(async move {
        while async_rx.recv().await.is_some() {
            // Mark dirty so back-to-back events are coalesced
            dirty_clone.store(true, Ordering::SeqCst);

            if rebuilding
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue; // Already rebuilding; dirty flag ensures a follow-up
            }

            loop {
                dirty_clone.store(false, Ordering::SeqCst);

                let project = project.clone();
                let reload_tx = reload_tx.clone();

                let build_ok = tokio::task::spawn_blocking(move || {
                    Console::info("File changed, rebuilding...");
                    match project.load_config() {
                        Ok(config) => {
                            let out = config.build.resolve_output_dir(&project.root);
                            match SiteBuilder::new(project, Some(out), drafts, false) {
                                Ok(builder) => match builder.build() {
                                    Ok(()) => {
                                        Console::success("Rebuild complete");
                                        let _ = reload_tx.send(());
                                        true
                                    }
                                    Err(e) => {
                                        Console::error(&format!("Rebuild failed: {}", e));
                                        false
                                    }
                                },
                                Err(e) => {
                                    Console::error(&format!("Rebuild failed: {}", e));
                                    false
                                }
                            }
                        }
                        Err(e) => {
                            Console::error(&format!("Failed to load config: {}", e));
                            false
                        }
                    }
                })
                .await
                .unwrap_or(false);

                // If new changes arrived during the rebuild, rebuild again
                if !dirty_clone.load(Ordering::SeqCst) || !build_ok {
                    break;
                }
                Console::info("Changes detected during rebuild, rebuilding again...");
            }

            rebuilding.store(false, Ordering::SeqCst);
        }
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{header, Response, StatusCode};

    async fn make_html_response(body: &str) -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn response_body_string(response: Response<Body>) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), 10 * 1024 * 1024)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn test_inject_live_reload_before_body_close() {
        let html = "<html><body><p>Hello</p></body></html>";
        let response = make_html_response(html).await;
        let result = inject_live_reload(response).await;
        let body = response_body_string(result).await;

        assert!(body.contains(LIVE_RELOAD_SCRIPT));
        assert!(body.contains(&format!("{}</body>", LIVE_RELOAD_SCRIPT)));
    }

    #[tokio::test]
    async fn test_inject_live_reload_no_body_tag() {
        let html = "<html><p>No body tag</p></html>";
        let response = make_html_response(html).await;
        let result = inject_live_reload(response).await;
        let body = response_body_string(result).await;

        assert!(body.contains(LIVE_RELOAD_SCRIPT));
        assert!(body.ends_with(LIVE_RELOAD_SCRIPT));
    }

    #[tokio::test]
    async fn test_inject_live_reload_skips_non_html() {
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{\"key\": \"value\"}"))
            .unwrap();
        let result = inject_live_reload(response).await;
        let body = response_body_string(result).await;

        assert!(!body.contains(LIVE_RELOAD_SCRIPT));
        assert_eq!(body, "{\"key\": \"value\"}");
    }

    #[tokio::test]
    async fn test_inject_live_reload_removes_content_length() {
        let html = "<html><body></body></html>";
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html")
            .header(header::CONTENT_LENGTH, html.len().to_string())
            .body(Body::from(html))
            .unwrap();
        let result = inject_live_reload(response).await;

        assert!(result.headers().get(header::CONTENT_LENGTH).is_none());
    }
}
