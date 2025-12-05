use anyhow::Result;
use std::fs;
use std::path::Path;
use walkdir::WalkDir;

/// Copy a directory recursively
pub fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;

    for entry in WalkDir::new(src) {
        let entry = entry?;
        let path = entry.path();

        if path == src {
            continue;
        }

        let relative_path = path.strip_prefix(src)?;
        let dst_path = dst.join(relative_path);

        if entry.file_type().is_dir() {
            fs::create_dir_all(&dst_path)?;
        } else {
            if let Some(parent) = dst_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(path, &dst_path)?;
        }
    }

    Ok(())
}

/// Optimize CSS by removing comments and extra whitespace
#[allow(dead_code)]
pub fn optimize_css(css: &str) -> String {
    css.lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty() && !line.starts_with("/*"))
        .collect::<Vec<_>>()
        .join(" ")
        .replace("  ", " ")
        .replace("; ", ";")
        .replace(" {", "{")
        .replace("{ ", "{")
        .replace(" }", "}")
        .replace("} ", "}")
}

/// Optimize JavaScript by removing comments and extra whitespace
#[allow(dead_code)]
pub fn optimize_js(js: &str) -> String {
    js.lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty() && !line.starts_with("//") && !line.starts_with("/*"))
        .collect::<Vec<_>>()
        .join(" ")
        .replace("  ", " ")
}
