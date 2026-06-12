//! Stamps build provenance into the binary so `--version` identifies the exact
//! build. Nightly releases are all tagged `nightly` and share the crate version,
//! so without this every build would report an identical version string and an
//! upgrade could not be verified.
//!
//! Emits `OMNIAGENT_BUILD_SHA` and `OMNIAGENT_BUILD_DATE` as compile-time env
//! vars (always set; `"unknown"` when git is unavailable, e.g. local non-git
//! checkouts).

use std::path::Path;
use std::process::Command;

fn main() {
    let sha = git(&["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let date = git(&["log", "-1", "--format=%cd", "--date=short"])
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=OMNIAGENT_BUILD_SHA={sha}");
    println!("cargo:rustc-env=OMNIAGENT_BUILD_DATE={date}");

    // Re-stamp when the checked-out commit moves. Only register paths that exist
    // so a non-git checkout doesn't trigger perpetual rebuilds.
    for path in ["../../.git/HEAD", "../../.git/index"] {
        if Path::new(path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}

/// Runs `git <args>` and returns trimmed stdout, or `None` if git is missing or
/// the command fails (e.g. not a git checkout).
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if value.is_empty() { None } else { Some(value) }
}
