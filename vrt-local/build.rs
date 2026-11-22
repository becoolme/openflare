use std::io::Write;

/// Stage the `of-bundler` helper for `include_bytes!` in
/// `src/bundle.rs`. Release packaging sets `OF_EMBED_BUNDLER` to a
/// freshly built helper binary; dev builds embed nothing and resolve
/// the helper from the repo target dir instead.
fn main() {
    println!("cargo:rerun-if-env-changed=OF_EMBED_BUNDLER");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let dst = out.join("of-bundler.bin");
    match std::env::var("OF_EMBED_BUNDLER") {
        Ok(src) if !src.is_empty() => {
            let bytes = std::fs::read(&src).unwrap_or_else(|e| {
                panic!("OF_EMBED_BUNDLER {src}: {e}")
            });
            std::fs::write(&dst, bytes).unwrap();
        }
        _ => {
            std::fs::File::create(&dst)
                .unwrap()
                .write_all(&[])
                .unwrap();
        }
    }
}
