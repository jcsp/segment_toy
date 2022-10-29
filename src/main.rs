extern crate redpanda_adl;

mod batch_reader;
mod bucket_reader;
mod segment_writer;
mod varint;

use futures::StreamExt;
use log::info;
use std::env;
use tokio::fs::File;
use tokio::io::BufReader;

use crate::batch_reader::DumpError;
use crate::bucket_reader::BucketReader;
use crate::segment_writer::SegmentWriter;
use batch_reader::BatchStream;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    //let filename = &args[1];
    //dump_one(&filename).await.unwrap();

    let bucket = &args[1];
    let mut reader = BucketReader::new(bucket).await;
    reader.scan().await.unwrap();

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
