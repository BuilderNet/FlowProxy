use std::process::Command;

fn main() {
    // Capture the git commit hash at build time
    let git_hash = match Command::new("git").args(["rev-parse", "--short", "HEAD"]).output() {
        Ok(output) => String::from_utf8(output.stdout)
            .expect("Invalid UTF-8 sequence from git")
            .trim()
            .to_string(),
        Err(e) => {
            eprintln!("Failed to get git hash, building with unknown hash: {e:?}");
            "unknown".to_string()
        }
    };

    // Set the GIT_HASH environment variable for use in the binary
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    // Re-run this build script if the git HEAD changes
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");
}
