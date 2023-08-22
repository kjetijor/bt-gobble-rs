use std::process::Command;
fn main() {
    // note: add error checking yourself.
    let git_rev_dirty_out = Command::new("git").args(&["describe", "--always", "--dirty", "--tags"]).output().unwrap();
    let git_rev = String::from_utf8(git_rev_dirty_out.stdout).unwrap();
    println!("cargo:rustc-env=GIT_REV={}", git_rev);
}
