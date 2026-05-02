//! plures-object CLI — S3-compatible object storage server.

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use plures_chunkstore::FsChunkStore;
use plures_manifest_db::FsManifestStore;
use plures_object_http::make_router;
use plures_object_store::ObjectService;

#[derive(Parser)]
#[command(name = "plures-object", version, about = "S3-compatible object storage with content-addressed chunks")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the HTTP object storage server
    Serve {
        /// Address to bind
        #[arg(long, default_value = "127.0.0.1:8300")]
        bind: String,

        /// Data directory for chunks and manifests
        #[arg(long, default_value = "./plures-object-data")]
        data_dir: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve { bind, data_dir } => {
            let chunks_dir = data_dir.join("chunks");
            let manifests_dir = data_dir.join("manifests");
            std::fs::create_dir_all(&chunks_dir)?;
            std::fs::create_dir_all(&manifests_dir)?;

            let chunks = Arc::new(FsChunkStore::new(&chunks_dir));
            let manifests = Arc::new(FsManifestStore::new(&manifests_dir));
            let service = Arc::new(ObjectService::new(chunks, manifests));

            let router = make_router(service);
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            tracing::info!("plures-object serving on {}", bind);
            axum::serve(listener, router).await?;
        }
    }

    Ok(())
}
