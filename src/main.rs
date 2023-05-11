extern crate deltafor;
extern crate redpanda_adl;
extern crate redpanda_records;

mod batch_crc;
mod bucket_reader;
mod error;
mod fundamental;
mod ntp_mask;
mod remote_types;

use log::{error, info, warn};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

use crate::bucket_reader::{AnomalyStatus, BucketReader};
use crate::fundamental::NTPR;
use crate::ntp_mask::NTPMask;
use crate::remote_types::PartitionManifest;
use clap::{Parser, Subcommand};
use serde_json::json;

/// Parser for use with `clap` argument parsing
pub fn ntpr_mask_parser(input: &str) -> Result<NTPMask, String> {
    NTPMask::from_str(input).map_err(|e| e.to_string())
}

#[derive(clap::ValueEnum, Clone)]
enum Backend {
    AWS,
    GCP,
    Azure,
}

impl Display for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Backend::AWS => f.write_str("aws"),
            Backend::GCP => f.write_str("gcp"),
            Backend::Azure => f.write_str("azure"),
        }
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, default_value_t = Backend::AWS)]
    backend: Backend,

    #[arg(short, long, value_parser = ntpr_mask_parser, default_value_t = NTPMask::match_all())]
    filter: NTPMask,
}

#[derive(Subcommand)]
enum Commands {
    Scan {
        #[arg(short, long)]
        source: String,
    },
    DecodePartitionManifest {
        #[arg(short, long)]
        path: String,
    },
}

/**
 * Read-only scan of data, report anomalies, optionally also decode all record batches.
 */
async fn scan(cli: &Cli, source: &str) {
    let client: Arc<dyn object_store::ObjectStore> = match cli.backend {
        Backend::AWS => {
            let mut client_builder = object_store::aws::AmazonS3Builder::from_env();
            client_builder = client_builder.with_bucket_name(source);
            Arc::new(client_builder.build().unwrap())
        }
        Backend::GCP => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(source)
                .build()
                .unwrap(),
        ),
        Backend::Azure => {
            let client = object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_container_name(source)
                .build()
                .unwrap();
            Arc::new(client)
        }
    };
    let mut reader = BucketReader::new(client).await;
    match reader.scan().await {
        Err(e) => {
            error!("Error scanning bucket: {:?}", e);
            return;
        }
        Ok(_) => {}
    }

    let mut failed = false;
    match reader.anomalies.status() {
        AnomalyStatus::Clean => {
            info!("Scan of bucket {}:\n{}", source, reader.anomalies.report());
        }
        _ => {
            // Report on any unclean bucket contents.
            warn!(
                "Anomalies detected in bucket {}:\n{}",
                source,
                reader.anomalies.report()
            );

            failed = true;
        }
    }

    println!("{}", json!(reader.anomalies));

    if failed {
        error!("Issues detected in bucket");
        std::process::exit(-1);
    }
}

async fn decode_partition_manifest(path: &str) {
    let mut f = tokio::fs::File::open(path).await.unwrap();
    let mut buf: Vec<u8> = vec![];
    f.read_to_end(&mut buf).await.unwrap();

    let manifest = PartitionManifest::from_bytes(bytes::Bytes::from(buf)).unwrap();
    serde_json::to_writer_pretty(&::std::io::stdout(), &manifest).unwrap();
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::Scan { source }) => {
            scan(&cli, source).await;
        }
        Some(Commands::DecodePartitionManifest { path }) => {
            decode_partition_manifest(path).await;
        }

        None => {}
    }
}
