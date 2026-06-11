//! One-shot codegen: CRI v1 proto -> committed Rust in lightr-cri-proto.
//! Run from the workspace root: `cargo run -p protogen`.
//! CI's verify-codegen job reruns this and diffs src/generated (drift = red).

use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();
    let proto_dir = root.join("crates/lightr-cri-proto/proto");
    let out_dir = root.join("crates/lightr-cri-proto/src/generated");
    std::fs::create_dir_all(&out_dir).expect("create out dir");

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .emit_rerun_if_changed(false)
        .out_dir(&out_dir)
        .compile_protos(&[proto_dir.join("api.proto")], &[proto_dir])
        .expect("tonic codegen");

    println!("generated into {}", out_dir.display());
}
