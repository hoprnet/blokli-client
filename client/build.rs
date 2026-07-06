use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let schema_path = manifest_dir.join("target-api-schema.graphql");

    println!("cargo:rerun-if-changed={}", schema_path.display());
    cynic_codegen::register_schema("blokli")
        .from_sdl_file(schema_path)?
        .as_default()
        .map(|_| ())
        .map_err(Into::into)
}
