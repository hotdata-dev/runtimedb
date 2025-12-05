use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();

    // Map target platform to driver directory
    let platform_dir = match (target_os.as_str(), target_arch.as_str()) {
        ("macos", "aarch64") => "macos-arm64",
        ("macos", "x86_64") => "macos-x64",
        ("linux", "x86_64") => "linux-x64",
        ("windows", "x86_64") => "windows-x64",
        _ => {
            println!("cargo:warning=Unsupported platform: {}-{}", target_os, target_arch);
            return;
        }
    };

    // Source and destination paths
    let source_dir = PathBuf::from("src/datafetch/drivers").join(platform_dir);
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let dest_dir = out_dir.join("drivers");

    // Emit rerun-if-changed for the entire drivers directory
    println!("cargo:rerun-if-changed=src/datafetch/drivers");

    // Check if source directory exists
    if !source_dir.exists() {
        println!("cargo:warning=Driver directory does not exist: {}", source_dir.display());
        return;
    }

    // Create destination directory
    if let Err(e) = fs::create_dir_all(&dest_dir) {
        println!("cargo:warning=Failed to create destination directory: {}", e);
        return;
    }

    // Copy driver files (skip .gitkeep and README.md)
    match fs::read_dir(&source_dir) {
        Ok(entries) => {
            let mut copied_count = 0;
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(filename) = path.file_name() {
                    let filename_str = filename.to_string_lossy();

                    // Skip .gitkeep and README files
                    if filename_str == ".gitkeep" || filename_str.starts_with("README") {
                        continue;
                    }

                    let dest_path = dest_dir.join(filename);
                    if let Err(e) = fs::copy(&path, &dest_path) {
                        println!("cargo:warning=Failed to copy driver {}: {}", filename_str, e);
                    } else {
                        println!("cargo:warning=Copied driver: {}", filename_str);
                        copied_count += 1;
                    }
                }
            }

            if copied_count == 0 {
                println!("cargo:warning=No driver files found in {}", source_dir.display());
            }
        }
        Err(e) => {
            println!("cargo:warning=Failed to read driver directory: {}", e);
        }
    }

    // Export OUT_DIR for runtime use
    println!("cargo:rustc-env=RIVETDB_DRIVER_DIR={}", dest_dir.display());
}