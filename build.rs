use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Capture build information
    capture_build_info();

    // Only patch for WASM targets
    let target = env::var("TARGET").unwrap();
    if !target.contains("wasm32") {
        return;
    }

    println!("cargo:rerun-if-changed=build.rs");

    // Use cargo metadata to find the exact location of zstd-sys
    if let Ok(output) = Command::new("cargo")
        .args(&["metadata", "--format-version", "1"])
        .output()
    {
        if let Ok(metadata) = String::from_utf8(output.stdout) {
            // Parse to find zstd-sys package
            for line in metadata.lines() {
                if line.contains("\"zstd-sys\"") && line.contains("\"manifest_path\"") {
                    // Extract path and patch
                    if let Some(start) = line.find("\":\"") {
                        if let Some(end) = line[start + 3..].find("\"") {
                            let manifest_path = &line[start + 3..start + 3 + end];
                            if let Some(parent) = PathBuf::from(manifest_path).parent() {
                                let shim_path = parent.join("wasm-shim/stdlib.h");
                                if shim_path.exists() {
                                    patch_stdlib_h(&shim_path);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Fallback: search for zstd-sys in common locations
    if let Ok(cargo_home) = env::var("CARGO_HOME") {
        let registry = PathBuf::from(cargo_home).join("registry/src");
        if let Ok(entries) = fs::read_dir(&registry) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Ok(crates) = fs::read_dir(&path) {
                    for crate_entry in crates.flatten() {
                        let crate_path = crate_entry.path();
                        if let Some(crate_name) = crate_path.file_name() {
                            if crate_name.to_string_lossy().starts_with("zstd-sys-") {
                                let shim_path = crate_path.join("wasm-shim/stdlib.h");
                                if shim_path.exists() {
                                    patch_stdlib_h(&shim_path);
                                    return;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn patch_stdlib_h(path: &PathBuf) {
    if let Ok(content) = fs::read_to_string(path) {
        // Check if already patched
        if content.contains("qsort_r") {
            return;
        }

        // Add qsort_r shim
        let patched = content.replace(
            "#endif  // _STDLIB_H",
            r#"
/* qsort_r shim for WASM - ignores context parameter */
#define qsort_r(base, nitems, size, compar, arg) \
  rust_zstd_wasm_shim_qsort(base, nitems, size, (int (*)(const void*, const void*))compar)

#endif  // _STDLIB_H"#,
        );

        if let Err(e) = fs::write(path, patched) {
            eprintln!("Warning: Failed to patch zstd-sys wasm-shim: {}", e);
        }
    }
}

fn capture_build_info() {
    // Capture git hash
    if let Ok(output) = Command::new("git")
        .args(&["rev-parse", "--short", "HEAD"])
        .output()
    {
        if output.status.success() {
            if let Ok(git_hash) = String::from_utf8(output.stdout) {
                println!("cargo:rustc-env=GIT_HASH={}", git_hash.trim());
            }
        }
    }

    // Capture build date
    if let Ok(output) = Command::new("date").args(&["+%Y-%m-%d"]).output() {
        if output.status.success() {
            if let Ok(build_date) = String::from_utf8(output.stdout) {
                println!("cargo:rustc-env=BUILD_DATE={}", build_date.trim());
            }
        }
    }

    // Capture rustc version
    if let Ok(output) = Command::new("rustc").args(&["--version"]).output() {
        if output.status.success() {
            if let Ok(rustc_version) = String::from_utf8(output.stdout) {
                // Extract just version number
                if let Some(version) = rustc_version.split_whitespace().nth(1) {
                    println!("cargo:rustc-env=RUSTC_VERSION={}", version);
                }
            }
        }
    }

    println!("cargo:rerun-if-changed=.git/HEAD");
}
