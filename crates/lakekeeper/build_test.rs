fn main() {
    // Print environment variables available to build script
    println!("cargo:warning=PWD: {:?}", std::env::var("PWD"));
    println!("cargo:warning=CARGO_MANIFEST_DIR: {:?}", std::env::var("CARGO_MANIFEST_DIR"));
    println!("cargo:warning=OUT_DIR: {:?}", std::env::var("OUT_DIR"));
    
    // Check if .tmp_git_root exists
    let tmp_git_root = std::path::Path::new(".tmp_git_root");
    println!("cargo:warning=.tmp_git_root exists: {}", tmp_git_root.exists());
    
    let sqlx_dir = std::path::Path::new(".tmp_git_root/.sqlx");
    println!("cargo:warning=.tmp_git_root/.sqlx exists: {}", sqlx_dir.exists());
    
    // Try to get absolute path
    if let Ok(abs_path) = std::fs::canonicalize(".tmp_git_root/.sqlx") {
        println!("cargo:warning=Absolute path: {:?}", abs_path);
    }
    
    println!("cargo:rerun-if-changed=migrations");
}
