use bincode::enc::write::SliceWriter;
use crc32c::crc32c;
use log::{debug, error, info};
use redpanda_adl::from_bytes;
use redpanda_records::{
    RecordBatchHeader, RecordBatchHeaderCrcFirst, RecordBatchHeaderCrcSecond, RecordBatchType,
};
use serde::ser::Serialize;
use std::io;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::varint::VarIntDecoder;

#[derive(Debug)]
pub enum DumpError {
    Message(String),
    DecodeError(redpanda_adl::Error),
    IOError(io::Error),
    EOF,
}

impl From<redpanda_adl::Error> for DumpError {
    fn from(e: redpanda_adl::Error) -> Self {
        Self::DecodeError(e)
    }
}

impl From<std::io::Error> for DumpError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(e)
    }
}

pub struct BatchStream<T: AsyncReadExt> {
    inner: T,
    bytes_read: u64,
    cur_batch_raw: Vec<u8>,
}

pub struct BatchBuffer {
    pub header: RecordBatchHeader,
    pub bytes: Vec<u8>,
}

fn hexdump(bytes: &[u8]) -> String {
    let mut output = String::new();
    for b in bytes {
        output.push_str(&format!("{:02x}", b));
    }
    output
}

impl<T: AsyncReadExt + Unpin> BatchStream<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            bytes_read: 0,
            cur_batch_raw: vec![],
        }
    }

    pub fn get_bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Call read_exact on the inner stream, copying the read bytes into
    /// the `cur_batch_raw` accumulator buffer.
    async fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let r = self.inner.read_exact(buf).await?;
        self.cur_batch_raw.extend_from_slice(&buf[0..r]);
        Ok(r)
    }

    async fn read_vari64(&mut self) -> io::Result<(i64, u8)> {
        let mut decoder = VarIntDecoder::new();
        let mut read_bytes = 0u8;
        loop {
            let b = self.read_u8().await?;
            read_bytes += 1;
            if decoder.feed(b) {
                break;
            }
        }

        self.bytes_read += read_bytes as u64;
        Ok((decoder.result(), read_bytes))
    }

    async fn read_u8(&mut self) -> io::Result<u8> {
        let mut b: [u8; 1] = [0u8; 1];
        let read_sz = self.read_exact(&mut b).await?;
        self.bytes_read += read_sz as u64;
        Ok(b[0])
    }

    async fn read_vec(&mut self, size: usize) -> io::Result<Vec<u8>> {
        let mut b: [u8; 4096] = [0u8; 4096];
        let mut result = Vec::<u8>::new();

        if size == 0 {
            return Ok(result);
        }

        loop {
            let bytes_remaining = size - result.len();
            let read_sz = self.read_exact(&mut b[0..bytes_remaining]).await?;
            self.bytes_read += read_sz as u64;
            result.extend_from_slice(&b[0..bytes_remaining]);
            if result.len() >= size {
                return Ok(result);
            }
        }
    }

    async fn read_batch_header(&mut self) -> Result<(RecordBatchHeader, u32), DumpError> {
        let mut header_buf: [u8; std::mem::size_of::<RecordBatchHeader>()] =
            [0u8; std::mem::size_of::<RecordBatchHeader>()];
        let read_sz = self.read_exact(&mut header_buf).await?;
        self.bytes_read += read_sz as u64;
        if read_sz < header_buf.len() {
            info!("EOF");
            Err(DumpError::EOF)
        } else {
            let result: RecordBatchHeader = from_bytes(&header_buf, bincode::config::standard())?;

            // Some other code talks about needing to re-encode this stuff big-endian
            // to calculate header CRCs, but the
            let header_crc_bytes = &header_buf[4..];
            let header_crcc = crc32c::crc32c(header_crc_bytes);
            info!(
                "read_batch_header header_crc_bytes({}) {} {}",
                header_crcc,
                header_crc_bytes.len(),
                hexdump(&header_crc_bytes)
            );
            Ok((result, header_crcc))
        }
    }

    pub async fn read_batch_buffer(&mut self) -> Result<BatchBuffer, DumpError> {
        // Read one batch header (fixed size)
        let (batch_header, header_hash) = self.read_batch_header().await?;
        debug!(
            "Batch {:?} {} {}",
            batch_header,
            header_hash,
            std::mem::size_of::<RecordBatchHeader>()
        );

        // Fast-reject path avoids calculating CRC if the header is clearly invalid
        if batch_header.record_batch_type == 0
            || batch_header.record_batch_type >= RecordBatchType::Max as i8
        {
            debug!("Blank batch hit (falloc region)");
            return Err(DumpError::EOF);
        }

        if batch_header.header_crc != header_hash {
            info!("Bad CRC on batch header, torn write at end of local-storage segment?");
            return Err(DumpError::EOF);
        }

        // Calculate CRC, to determine whether this is a real batch, or some trailing
        // junk at the end of a falloc'd disk segment.
        const header_crc_bytes_len: usize = 17 + 40;
        let mut header_crc_bytes: [u8; header_crc_bytes_len] = [0; header_crc_bytes_len];
        let mut header_crc_fields = RecordBatchHeaderCrcFirst {
            size_bytes: batch_header.size_bytes,
            base_offset: batch_header.base_offset,
            record_batch_type: batch_header.record_batch_type,
            crc: batch_header.crc,
        };
        // let writer = SliceWriter::new(&mut header_crc_bytes);
        // let mut encoder = bincode::enc::EncoderImpl::new(
        //     writer,
        //     bincode::config::standard().with_little_endian(),
        // );
        // RecordBatchHeaderCrcFirst::serialize(&header_crc_fields, encoder).unwrap();
        bincode::encode_into_slice(
            &header_crc_fields,
            &mut header_crc_bytes,
            bincode::config::standard()
                .with_little_endian()
                .with_fixed_int_encoding(),
        )
        .unwrap();

        let vecver1 = bincode::encode_to_vec(
            &header_crc_fields,
            bincode::config::standard()
                .with_little_endian()
                .with_fixed_int_encoding(),
        )
        .unwrap();

        let mut header_crc_fields_two = RecordBatchHeaderCrcSecond {
            record_batch_attributes: batch_header.record_batch_attributes,
            last_offset_delta: batch_header.last_offset_delta,
            first_timestamp: batch_header.first_timestamp,
            max_timestamp: batch_header.max_timestamp,
            producer_id: batch_header.producer_id,
            producer_epoch: batch_header.producer_epoch,
            base_sequence: batch_header.base_sequence,
            record_count: batch_header.record_count,
        };

        let vecver2 = bincode::encode_to_vec(
            &header_crc_fields_two,
            bincode::config::standard()
                .with_little_endian()
                .with_fixed_int_encoding(),
        )
        .unwrap();

        info!("len1: {}, len2: {}", vecver1.len(), vecver2.len());

        bincode::encode_into_slice(
            header_crc_fields_two,
            &mut header_crc_bytes[17..],
            bincode::config::standard()
                .with_little_endian()
                .with_fixed_int_encoding(),
        )
        .unwrap();

        info!(
            "header_crc_bytes: {}, {}",
            header_crc_bytes.len(),
            hexdump(&header_crc_bytes)
        );

        let mut sum: u32 = 0;
        for b in header_crc_bytes {
            sum += b as u32;
        }
        debug!("Header sum: {}", sum);

        let header_crc = crc32c::crc32c(&header_crc_bytes);
        debug!("Header CRC: {}", header_crc);

        const header2_crc_bytes_len: usize = 40;
        let mut header2_crc_bytes: [u8; header2_crc_bytes_len] = [0; header2_crc_bytes_len];
        bincode::encode_into_slice(
            header_crc_fields_two,
            &mut header2_crc_bytes,
            bincode::config::standard()
                .with_big_endian()
                .with_fixed_int_encoding(),
        )
        .unwrap();
        let mut body_crc32c = crc32c::crc32c(&header2_crc_bytes);
        debug!("header2 crc: {}", body_crc32c);

        // CRC calculation is based on header bytes as they would be encoded by Kafka, not as
        // they are encoded by redpanda.  We must rebuild the header.

        // TODO: CRC check is not mandatory when reading from S3 objects, as these are
        // written atomically and do not have junk at end.
        // TODO: we must also check for bytes remaining, in case of an incompletely
        // written batch in a disk log.

        // TODO: just skip, and move the decoding to somewhere else where we really
        // care about reading the records.
        // For n records within the batch (variable size), consume.
        for _record_i in 0..batch_header.record_count {
            let len = self.read_vari64().await?;
            debug!("Len {:?}", len);
            let attrs: u8 = self.read_u8().await?;
            debug!("Attrs {:x}", attrs);
            let ts_delta = self.read_vari64().await?.0 as u32;
            let offset_delta = self.read_vari64().await?.0 as u32;
            debug!("Deltas: {} {}", ts_delta, offset_delta);

            let key_len = self.read_vari64().await?.0;
            debug!("key_len: {}", key_len);
            if key_len > 0 {
                // key_len may be -1, means skip
                let _key = self.read_vec(key_len as usize).await?;
            }
            let val_len = self.read_vari64().await?.0;
            if val_len > 0 {
                // val_len may be -1, means skip
                let _val = self.read_vec(val_len as usize).await?;
            }
            debug!("Key, val: {} {}", key_len, val_len);

            let n_headers = self.read_vari64().await?.0 as usize;
            for _header_i in 0..n_headers {
                let key_len = self.read_vari64().await?.0 as usize;
                let _key = self.read_vec(key_len).await?;
                let val_len = self.read_vari64().await?.0 as usize;
                let _val = self.read_vec(key_len).await?;
                debug!("Header Key, val: {} {}", key_len, val_len);
            }
        }

        body_crc32c = crc32c::crc32c_append(
            body_crc32c,
            &self.cur_batch_raw[std::mem::size_of::<RecordBatchHeader>()..],
        );
        debug!(
            "Body crc: {:08x} (vs {:08x}) (records {})",
            body_crc32c,
            batch_header.crc as u32,
            self.cur_batch_raw.len() - std::mem::size_of::<RecordBatchHeader>()
        );

        debug!(
            "Just records crc32c: {:08x}",
            crc32c::crc32c(&self.cur_batch_raw[std::mem::size_of::<RecordBatchHeader>()..])
        );

        if (body_crc32c != batch_header.crc) {
            info!(
                "Batch CRC mismatch ({:08x} != {:08x})",
                body_crc32c, batch_header.crc as u32
            );
            // TODO: A stronger check: this _can_ happen on a torn write at the end
            // of o local disk segment, but if we aren't anywhere near the end of the seg,
            // or if we're reading an object storage segment, this is a real corruption.
            return Err(DumpError::EOF);
        }

        if batch_header.header_crc == 3836992428 {
            let mut f = File::create("/tmp/dump.bin").await.unwrap();
            f.write(&self.cur_batch_raw[std::mem::size_of::<RecordBatchHeader>()..])
                .await
                .unwrap();
        }

        let batch_bytes = std::mem::take(&mut self.cur_batch_raw);
        if batch_header.header_crc == 3836992428 {
            info!(
                "Reading {} bytes ({})",
                batch_header.size_bytes as usize,
                batch_bytes.len()
            );
        }

        Ok(BatchBuffer {
            header: batch_header,
            bytes: batch_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::info;
    use std::env;
    use tokio::fs::File;
    use tokio::io::{AsyncReadExt, BufReader};

    #[test_log::test(tokio::test)]
    pub async fn decode_simple() {
        let cargo_path = env::var("CARGO_MANIFEST_DIR").unwrap();

        // A simple segment file with 5 batches, 1 record in each one,
        // written by Redpanda 22.3
        let expect_batches = 5;
        let filename = cargo_path + "/resources/test/test_segment.bin";

        let file = File::open(filename).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let reader = BufReader::new(file);
        let mut stream = BatchStream::new(reader);

        let mut count: usize = 0;
        loop {
            match stream.read_batch_buffer().await {
                Ok(bb) => {
                    info!(
                        "Read batch {} bytes, header {:?}",
                        bb.bytes.len(),
                        bb.header
                    );
                    count += 1;
                    assert!(count <= expect_batches);
                }
                Err(e) => {
                    info!("Stream complete: {:?}", e);
                    break;
                }
            }
        }
        assert_eq!(count, expect_batches);
    }

    #[test_log::test(tokio::test)]
    pub async fn decode_simple_two() {
        let cargo_path = env::var("CARGO_MANIFEST_DIR").unwrap();

        // A simple segment file with 5 batches, 1 record in each one,
        // written by Redpanda 22.3
        let expect_batches = 1;
        let filename = cargo_path + "/resources/test/3676-7429-77-1-v1.log.1";

        let file = File::open(filename).await.unwrap();
        let file_size = file.metadata().await.unwrap().len();
        let reader = BufReader::new(file);
        let mut stream = BatchStream::new(reader);

        let mut count: usize = 0;
        loop {
            match stream.read_batch_buffer().await {
                Ok(bb) => {
                    info!(
                        "Read batch {} bytes, header {:?}",
                        bb.bytes.len(),
                        bb.header
                    );
                    count += 1;
                    assert!(count <= expect_batches);
                }
                Err(e) => {
                    info!("Stream complete: {:?}", e);
                    break;
                }
            }
        }

        assert_eq!(count, expect_batches);
    }
}
