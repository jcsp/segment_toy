extern crate redpanda_adl;
extern crate redpanda_records;

mod batch_crc;
mod batch_reader;
mod batch_writer;
mod bucket_reader;
mod fundamental;
mod ntp_mask;
mod segment_writer;
mod varint;

use std::sync::Arc;
use futures::StreamExt;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::io::{AsyncWriteExt, BufReader};

use crate::batch_reader::DumpError;
use crate::batch_writer::reserialize_batch;
use crate::bucket_reader::{AnomalyStatus, BucketReader};
use crate::fundamental::NTPR;
use crate::ntp_mask::NTPMask;
use crate::segment_writer::SegmentWriter;
use batch_reader::BatchStream;
use clap::{Parser, Subcommand};
use serde_json::json;
use redpanda_records::RecordOwned;

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

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long)]
    backend: Backend,

    #[arg(short, long, value_parser = ntpr_mask_parser, default_value_t = NTPMask::match_all())]
    filter: NTPMask,
}

#[derive(Subcommand)]
enum Commands {
    Rewrite {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        force: bool,
    },
    Scan {
        #[arg(short, long)]
        source: String,
        #[arg(short, long)]
        detail: bool,
    },
    Scrub {
        #[arg(short, long)]
        source: String,
    },
    Compact {
        #[arg(short, long)]
        source: String,
    },
}

/**
 * Safe scrubbing work: remove stray segment files not referenced by
 * any manifest
 */
async fn scrub(cli: &Cli, source: &str) {
    let mut reader = BucketReader::new(&cli.uri, source).await;
    reader.scan().await.unwrap();

    if !reader.anomalies.segments_outside_manifest.is_empty() {
        if let AnomalyStatus::Corrupt = reader.anomalies.status() {
            // If there are corrupt manifests, then we cannot reliably say which objects
            // are referred to by a manifest: maybe some segments _are_ correct data, but
            // something went wrong reading the manifest, and we must not risk deleting
            // segments in this case.
            warn!(
                "Skipping erase of {} keys outside manifest, because bucket is corrupt",
                reader.anomalies.segments_outside_manifest.len()
            );
        } else {
            info!("Erasing keys outside manifests...");
            for key in reader.anomalies.segments_outside_manifest {
                match reader
                    .client
                    .delete_object()
                    .bucket(source)
                    .key(&key)
                    .send()
                    .await
                {
                    Ok(_) => {
                        info!("Deleted {}", key);
                    }
                    Err(e) => {
                        warn!("Failed to delete {}: {}", key, e);
                    }
                }
            }
        }
    }

    info!("Scrub complete.")
}

/**
 * Rewrite data like-for-like, re-segmenting as we go.
 */
async fn rewrite(cli: &Cli, source: &str, force: bool) {
    let mut reader = BucketReader::new(&cli.uri, source).await;
    reader.scan().await.unwrap();

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
        }
    }

    // The scan process opportunistically detects any inconsistencies.  If we found any,
    // then pause ouf of abundance of caution.  We permit Dirty state because it is not
    // harmful, just indicates a few stray objects.
    if let AnomalyStatus::Corrupt = reader.anomalies.status() {
        if !force {
            warn!("Anomalies detected, not proceeding with operation.");
            return;
        }
    }

    // Okay, we know about all the segments and all the manifests, yey.
    for (ntpr, _) in &reader.partitions {
        let dir = std::path::PathBuf::from(format!(
            "/tmp/dump/{}/{}/{}_{}",
            ntpr.ntp.namespace, ntpr.ntp.topic, ntpr.ntp.partition_id, ntpr.revision_id
        ));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let mut writer = SegmentWriter::new(1024 * 1024, dir);

        info!("Reading partition {:?}", ntpr);
        let mut data_stream = reader.stream(ntpr);
        while let Some(byte_stream) = data_stream.next().await {
            let mut batch_stream = BatchStream::new(byte_stream.into_async_read());
            while let Ok(bb) = batch_stream.read_batch_buffer().await {
                info!(
                    "Read batch {} bytes, header {:?}",
                    bb.bytes.len(),
                    bb.header
                );
                writer.write_batch(&bb).await.unwrap();
            }
        }

        writer.flush_close().await.unwrap();
    }
}

async fn compact(cli: &Cli, source: &str) {
    let mut reader = BucketReader::new(&cli.uri, source).await;
    reader.scan().await.unwrap();

    // TODO: generalize the "stop if corrupt" check and apply it here, as we do in rewrite

    for (ntpr, _) in &reader.partitions {
        // TODO: inspect topic manifest to see if it is meant to be compacted.
        if !cli.filter.compare(ntpr) {
            debug!("Skipping {}, doesn't match NTPR mask", ntpr);
            continue;
        }

        let dir = std::path::PathBuf::from(format!(
            "/tmp/dump/{}/{}/{}_{}",
            ntpr.ntp.namespace, ntpr.ntp.topic, ntpr.ntp.partition_id, ntpr.revision_id
        ));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let mut writer = SegmentWriter::new(1024 * 1024, dir);

        info!("Reading partition {:?}", ntpr);
        let mut data_stream = reader.stream(ntpr);
        while let Some(byte_stream) = data_stream.next().await {
            info!("Compacting segment...");
            let mut dropped_count: u64 = 0;
            let mut retained_count: u64 = 0;

            // This is just basic self-compaction of each segment.
            let mut seen_keys: HashMap<Option<Vec<u8>>, u64> = HashMap::new();
            let tmp_file_path = "/tmp/compact.bin";
            let mut tmp_file = File::create(tmp_file_path).await.unwrap();
            let mut batch_stream = BatchStream::new(byte_stream.into_async_read());
            while let Ok(bb) = batch_stream.read_batch_buffer().await {
                for r in bb.iter() {
                    // TODO: check the rules on compactino of records with no key
                    // TODO: only compact data records and/or do special rules for
                    // other record types, like dropping configuration batches and
                    // dropping aborted transactions.
                    seen_keys.insert(
                        r.key.map(|v| Vec::from(v)),
                        bb.header.base_offset + r.offset_delta as u64,
                    );
                }
                tmp_file.write_all(&bb.bytes).await.unwrap();
            }
            tmp_file.flush().await.unwrap();
            drop(tmp_file);
            drop(batch_stream);

            let mut reread_batch_stream =
                BatchStream::new(File::open(tmp_file_path).await.unwrap());
            while let Ok(bb) = reread_batch_stream.read_batch_buffer().await {
                let mut retain_records: Vec<RecordOwned> = vec![];
                for r in bb.iter() {
                    let offset = bb.header.base_offset + r.offset_delta as u64;
                    // Unwrap because we exhaustively populated seen_keys above
                    // TODO: efficiency: constructing a Vec for every key we check
                    if offset >= *seen_keys.get(&r.key.map(|v| Vec::from(v))).unwrap() {
                        retain_records.push(r.to_owned());
                        retained_count += 1;
                    } else {
                        dropped_count += 1;
                    }
                }
                // TODO: dont' reserialize if we didn't change anything
                let _new_batch = reserialize_batch(&bb.header, &retain_records);

                // TODO: hook into a streamwriter
            }

            info!(
                "Compacted segment, dropped {}, retained {}",
                dropped_count, retained_count
            );
        }
        writer.flush_close().await.unwrap();
    }
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
    reader.scan().await.unwrap();

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

#[tokio::main]
async fn main() {
    env_logger::init();

    let cli = Cli::parse();
    match &cli.command {
        Some(Commands::Rewrite { source, force }) => {
            rewrite(&cli, source, *force).await;
        }
        Some(Commands::Scan { source, detail }) => {
            scan(&cli, source, *detail).await;
        }
        Some(Commands::Scrub { source }) => {
            scrub(&cli, source).await;
        }
        Some(Commands::Compact { source }) => {
            compact(&cli, source).await;
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
