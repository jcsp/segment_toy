extern crate redpanda_adl;
extern crate redpanda_records;
extern crate deltafor;

mod batch_crc;
mod batch_reader;
mod batch_writer;
mod bucket_reader;
mod fundamental;
mod ntp_mask;
mod segment_writer;
mod varint;

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use futures::StreamExt;
use log::{error, info, warn};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

use crate::batch_reader::DumpError;
use crate::bucket_reader::{AnomalyStatus, BucketReader, PartitionManifest};
use crate::fundamental::NTPR;
use crate::ntp_mask::NTPMask;
use batch_reader::BatchStream;
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
            AWS => f.write_str("aws"),
            GCP => f.write_str("gcp"),
            Azure => f.write_str("azure")
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
        #[arg(short, long)]
        detail: bool,
    },
    DecodePartitionManifest {
        #[arg(short, long)]
        path: String,
    },
}

// TODO: reinstate scan_detail based on object_store streams
// async fn scan_detail(bucket_reader: BucketReader) {
//     for (ntpr, _) in &bucket_reader.partitions {
//         let mut data_stream = bucket_reader.stream(ntpr);
//         while let Some(byte_stream) = data_stream.next().await {
//             let mut batch_stream = BatchStream::new(byte_stream.into_async_read());
//             while let Ok(bb) = batch_stream.read_batch_buffer().await {
//                 info!("[{}] Batch {}", ntpr, bb.header);
//                 for record in bb.iter() {
//                     info!(
//                         "[{}] Record o={} s={}",
//                         ntpr,
//                         bb.header.base_offset + record.offset_delta as u64,
//                         record.len
//                     );
//                 }
//             }
//         }
//     }
// }

/**
 * Read-only scan of data, report anomalies, optionally also decode all record batches.
 */
async fn scan(cli: &Cli, source: &str, detail: bool) {
    let client: Arc<dyn object_store::ObjectStore> = match (cli.backend) {
        Backend::AWS => {
            let mut client_builder = object_store::aws::AmazonS3Builder::from_env();
            client_builder = client_builder.with_bucket_name(source);
            Arc::new(client_builder.build().unwrap())
        }
        Backend::GCP => {
            Arc::new(object_store::gcp::GoogleCloudStorageBuilder::from_env().with_bucket_name(
                source
            ).build().unwrap())
        }
        Backend::Azure => {
            let client = object_store::azure::MicrosoftAzureBuilder::from_env().with_container_name(
                source
            ).build().unwrap();
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

    // Detail mode: exhaustive read of all segment contents, down the the record level.
    if detail {
        // TODO: wire up the batch/record read to consider any EOFs etc as errors
        // when reading from S3, and set failed=true here

        // TODO: reinstate scan_detail based on object_store streams
        //scan_detail(reader).await
    }

    if failed {
        error!("Issues detected in bucket");
        std::process::exit(-1);
    }
}

async fn decode_partition_manifest(cli: &Cli, path: &str) {
    let mut f = tokio::fs::File::open(path).await.unwrap();
    let mut buf: Vec<u8> = vec![];
    f.read_to_end(&mut buf).await.unwrap();

    let manifest = PartitionManifest::from_bytes(bytes::Bytes::from(buf)).unwrap();
    serde_json::to_writer_pretty(&::std::io::stderr(), &manifest).unwrap();
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::Scan { source, detail }) => {
            scan(&cli, source, *detail).await;
        }
        Some(Commands::DecodePartitionManifest { path }) => {
            decode_partition_manifest(&cli, path).await;
        }

        None => {}
    }
}

/**
 * Stream design:
 * - The class that decodes batches needs to be cued on when a segment starts, because
 *   it will stop reading once it sees dead bytes at the end of an open segment.
 * - Downloads can fail partway through!  Whatever does the downloading needs to know
 *   how far it got writing into the batch decoder, so that it can pick up approxiastely
 *   where it left off.
 * - Same goes for uploads: if we fail partway through uploading a segment, we need
 *   to be able to rewind the reader/decoder (or restart from a known offset).
 */

async fn _dump_one(filename: &str) -> Result<(), DumpError> {
    info!("Reading {}", filename);

    let file = File::open(filename).await?;
    let _file_size = file.metadata().await?.len();
    let reader = BufReader::new(file);
    let mut stream = BatchStream::new(reader);

    loop {
        match stream.read_batch_buffer().await {
            Ok(bb) => {
                info!(
                    "Read batch {} bytes, header {:?}",
                    bb.bytes.len(),
                    bb.header
                );
            }
            Err(e) => {
                info!("Stream complete: {:?}", e);
                break;
            }
        }
    }

    Ok(())
}
