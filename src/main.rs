extern crate deltafor;
extern crate redpanda_adl;
extern crate redpanda_records;

mod batch_crc;
mod batch_reader;
mod bucket_reader;
mod error;
mod fundamental;
mod ntp_mask;
mod remote_types;
mod varint;

use log::{debug, error, info, trace, warn};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::bucket_reader::{AnomalyStatus, BucketReader};
use crate::fundamental::NTPR;
use crate::ntp_mask::NTPFilter;
use crate::remote_types::PartitionManifest;
use batch_reader::BatchStream;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use object_store::ObjectStore;
// TODO use the one in futures?
use crate::error::BucketReaderError;
use pin_utils::pin_mut;
use tokio_util::io::StreamReader;

/// Parser for use with `clap` argument parsing
pub fn ntp_filter_parser(input: &str) -> Result<NTPFilter, String> {
    NTPFilter::from_str(input).map_err(|e| e.to_string())
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

    #[arg(short, long, value_parser = ntp_filter_parser, default_value_t = NTPFilter::match_all())]
    filter: NTPFilter,
}

#[derive(Subcommand)]
enum Commands {
    ScanMetadata {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        meta_file: Option<String>,
    },
    ScanData {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        meta_file: Option<String>,
    },
    DecodePartitionManifest {
        #[arg(short, long)]
        path: String,
    },
    Extract {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        sink: String,
        #[arg(short, long)]
        meta_file: Option<String>,
    },
}

/// Construct an object store client based on the CLI flags
fn build_client(
    cli: &Cli,
    bucket: &str,
) -> Result<Arc<dyn object_store::ObjectStore>, object_store::Error> {
    let c: Arc<dyn object_store::ObjectStore> = match cli.backend {
        Backend::AWS => {
            let mut client_builder = object_store::aws::AmazonS3Builder::from_env();
            client_builder = client_builder.with_bucket_name(bucket);
            Arc::new(client_builder.build()?)
        }
        Backend::GCP => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        ),
        Backend::Azure => {
            let client = object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_container_name(bucket)
                .build()
                .unwrap();
            Arc::new(client)
        }
    };

    Ok(c)
}

async fn make_bucket_reader(
    cli: &Cli,
    source: &str,
    meta_file: Option<&str>,
) -> Result<BucketReader, BucketReaderError> {
    let client = build_client(cli, source)?;
    if let Some(path) = meta_file {
        info!("Loading metadata for {} from {}", source, path);
        Ok(BucketReader::from_file(path, client).await?)
    } else {
        info!("Scanning bucket {}...", source);
        let mut reader = BucketReader::new(client).await;
        reader.scan(&cli.filter).await?;
        Ok(reader)
    }
}

/**
 * Walk the data in NTPs matching filter, compare with metadata
 * report anomalies.
 */
async fn scan_data(
    cli: &Cli,
    source: &str,
    meta_file: Option<&str>,
) -> Result<(), BucketReaderError> {
    let bucket_reader = make_bucket_reader(cli, source, meta_file).await?;

    // TODO: wire up the batch/record read to consider any EOFs etc as errors
    // when reading from S3, and set failed=true here

    for (ntpr, _) in &bucket_reader.partitions {
        if !cli.filter.match_ntpr(ntpr) {
            continue;
        }

        let data_stream = bucket_reader.stream(ntpr);
        pin_mut!(data_stream);
        while let Some(segment_stream) = data_stream.next().await {
            let segment_stream_data = segment_stream?;
            let byte_stream = StreamReader::new(segment_stream_data);

            let mut batch_stream = BatchStream::new(byte_stream);
            while let Ok(bb) = batch_stream.read_batch_buffer().await {
                trace!("[{}] Batch {}", ntpr, bb.header);
                for record in bb.iter() {
                    trace!(
                        "[{}] Record o={} s={}",
                        ntpr,
                        bb.header.base_offset + record.offset_delta as u64,
                        record.len
                    );
                }
            }
        }
    }

    Ok(())
}

/**
 * Brute-force listing of bucket, read-only scan of metadata,
 * report anomalies.
 */
async fn scan_metadata(
    cli: &Cli,
    source: &str,
    meta_file: Option<&str>,
) -> Result<(), BucketReaderError> {
    let mut reader = make_bucket_reader(cli, source, None).await?;
    reader.scan(&cli.filter).await?;

    if let Some(out_file) = meta_file {
        reader.to_file(out_file).await.unwrap();
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

    println!(
        "{}",
        serde_json::to_string_pretty(&reader.anomalies).unwrap()
    );

    if failed {
        error!("Issues detected in bucket");
        std::process::exit(-1);
    }

    Ok(())
}

async fn extract(
    cli: &Cli,
    source: &str,
    sink: &str,
    meta_file: Option<&str>,
) -> Result<(), BucketReaderError> {
    let bucket_reader = make_bucket_reader(cli, source, meta_file).await?;

    // TODO: generalized URI-ish things so that callers can use object stores as sinks
    let sink_client = object_store::local::LocalFileSystem::new_with_prefix(sink)?;

    for (ntpr, _objects) in bucket_reader.partitions.iter() {
        if !cli.filter.match_ntpr(ntpr) {
            // If metadata was loaded from a file, it might not be filtered
            // in a way that lines up with cli.filter: re-filter so that one
            // can have a monolithic metadata file but extract individual partitions
            // on demand
            continue;
        } else {
            info!("extract match: {}", ntpr);
        }

        let manifest_paths: Vec<object_store::path::Path> = vec!["bin", "json"]
            .iter()
            .map(|e| PartitionManifest::manifest_key(&ntpr, e))
            .map(|s| object_store::path::Path::from(s))
            .collect();

        for path in &manifest_paths {
            debug!("Trying to download manifest {}", path);
            match bucket_reader.client.get(path).await {
                Ok(get_result) => {
                    let bytes = get_result.bytes().await?;
                    sink_client.put(path, bytes).await?;
                    info!("Downloaded manifest {}", path);
                }
                Err(e) => {
                    match e {
                        object_store::Error::NotFound { path: _, source: _ } => {
                            // Normal that one or other of the manifest paths is missing
                            debug!("Didn't fetch {}: {}", path, e);
                        }
                        _ => {
                            warn!("Unexpected error fetching {}: {}", path, e);
                            return Err(e.into());
                        }
                    }
                }
            };
        }
    }

    for (ntpr, objects) in bucket_reader.partitions.iter() {
        if !cli.filter.match_ntpr(ntpr) {
            continue;
        } else {
            info!("extract match: {}", ntpr);
        }

        for key in objects.all_keys() {
            info!("Copying {}", key);
            // TODO; make bucket reader return an object_store::Error?
            let mut stream = bucket_reader.stream_one(&key).await.unwrap();

            let (_, mut sink_stream) = sink_client
                .put_multipart(&object_store::path::Path::from(key.as_str()))
                .await?;

            while let Some(chunk) = stream.next().await {
                sink_stream.write(chunk.unwrap().as_ref()).await?;
            }
            sink_stream.shutdown().await?;
        }
    }
    Ok(())
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
        Some(Commands::ScanData { source, meta_file }) => {
            let r = scan_data(&cli, source, meta_file.as_ref().map(|s| s.as_str())).await;
            if let Err(e) = r {
                error!("Error: {:?}", e);
                std::process::exit(-1);
            }
        }
        Some(Commands::ScanMetadata { source, meta_file }) => {
            let r = scan_metadata(&cli, source, meta_file.as_ref().map(|s| s.as_str())).await;
            if let Err(e) = r {
                error!("Error: {:?}", e);
                std::process::exit(-1);
            }
        }
        Some(Commands::Extract {
            source,
            sink,
            meta_file,
        }) => {
            let r = extract(&cli, source, sink, meta_file.as_ref().map(|s| s.as_str())).await;

            if let Err(e) = r {
                error!("Error: {:?}", e);
                std::process::exit(-1);
            }
        }
        Some(Commands::DecodePartitionManifest { path }) => {
            decode_partition_manifest(path).await;
        }

        None => {}
    }
}
