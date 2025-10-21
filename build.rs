use std::process::Command;

fn main() {
    // Capture the git commit hash at build time
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .expect("Failed to execute git command");

    let git_hash = String::from_utf8(output.stdout)
        .expect("Invalid UTF-8 sequence from git")
        .trim()
        .to_string();

    // Set the GIT_HASH environment variable for use in the binary
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    // Re-run this build script if the git HEAD changes
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs/heads");
}
