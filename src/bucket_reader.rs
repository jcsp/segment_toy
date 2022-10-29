use crate::bucket_reader::BucketReaderError::{GetError, ListError, ParseError};
use aws_config;
use aws_endpoint::{AwsEndpoint, BoxError, CredentialScope, ResolveAwsEndpoint};
use aws_sdk_s3::error::{GetObjectError, ListObjectsV2Error};
use aws_sdk_s3::types::SdkError;
use aws_sdk_s3::{Client, Endpoint, Region};
use futures::stream::Stream;
use futures::{stream, StreamExt};
use http::Uri;
use lazy_static::lazy_static;
use log::{debug, warn};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;

pub struct SegmentObject {
    key: String,
    base_offset: u64,
}

pub struct PartitionObjects {
    segment_objects: Vec<SegmentObject>,
}

impl PartitionObjects {
    fn new() -> Self {
        Self {
            segment_objects: vec![],
        }
    }
}

#[derive(Eq, PartialEq, Hash, Debug)]
pub struct NTP {
    pub namespace: String,
    pub topic: String,
    pub partition_id: u32,
}

#[derive(Eq, PartialEq, Hash, Debug)]
pub struct NTPR {
    pub ntp: NTP,
    pub revision_id: u64,
}

/// Find all the partitions and their segments within a bucket
pub struct BucketReader {
    bucket_name: String,
    pub partitions: HashMap<NTPR, PartitionObjects>,
    pub partition_manifests: HashMap<NTPR, PartitionManifest>,
    client: Client,
}

#[derive(Debug)]
struct StaticResolver {
    uri: Uri,
}

struct SegmentStream {
    // TODO: rework to take a ref to the original vector
    segment_objects: Vec<SegmentObject>,
}

impl ResolveAwsEndpoint for StaticResolver {
    fn resolve_endpoint(&self, region: &Region) -> Result<AwsEndpoint, BoxError> {
        Ok(AwsEndpoint::new(
            Endpoint::immutable(self.uri.clone()),
            CredentialScope::builder().build(),
        ))
    }
}

#[derive(Debug)]
pub enum BucketReaderError {
    ListError(SdkError<ListObjectsV2Error>),
    GetError(SdkError<GetObjectError>),
    NetError(aws_smithy_http::byte_stream::Error),
    ParseError(serde_json::Error),
}

impl From<SdkError<ListObjectsV2Error>> for BucketReaderError {
    fn from(e: SdkError<ListObjectsV2Error>) -> Self {
        ListError(e)
    }
}

impl From<SdkError<GetObjectError>> for BucketReaderError {
    fn from(e: SdkError<GetObjectError>) -> Self {
        GetError(e)
    }
}

impl From<aws_smithy_http::byte_stream::Error> for BucketReaderError {
    fn from(e: aws_smithy_http::byte_stream::Error) -> Self {
        BucketReaderError::NetError(e)
    }
}

impl From<serde_json::Error> for BucketReaderError {
    fn from(e: serde_json::Error) -> Self {
        ParseError(e)
    }
}

impl BucketReader {
    pub async fn new(bucket_name: &str) -> Self {
        let resolver = StaticResolver {
            uri: Uri::from_str("http://192.168.1.100:9000").unwrap(),
        };
        let env_config = aws_config::load_from_env().await;
        let config = aws_sdk_s3::config::Builder::from(&env_config)
            .endpoint_resolver(resolver)
            .build();
        let client = Client::from_conf(config);

        Self {
            bucket_name: String::from(bucket_name),
            partitions: HashMap::new(),
            partition_manifests: HashMap::new(),
            client,
        }
    }

    pub async fn scan(&mut self) -> Result<(), BucketReaderError> {
        // TODO: for this to work at unlimited scale, we need:
        //  - ability to only address some hash subset of the partitions
        //    on each run (because we may not have enough memory to hold
        //    every partition's manifest)
        //  - load the manifests first, and only bother storing extra vectors
        //    of segments if those segments aren't in the manifest
        //  - or use a disk-spilling database for all this state.
        let mut list_stream = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .into_paginator()
            .send();
        while let Some(result_r) = list_stream.next().await {
            let result = result_r?;
            if let Some(meta) = result.contents {
                for o in meta {
                    let key: &String = &o.key.unwrap();
                    if key.as_str().ends_with("manifest.json") {
                        debug!("Parsing partition manifest key {}", key);
                        self.ingest_manifest(key).await?;
                    } else {
                        debug!("Parsing segment key {}", key);
                        self.ingest_segment(key);
                    }
                }
            }
        }

        // TODO: need to reconcile segments with manifests: if segments overlap
        // with each other, then whichever one is in the manifest wins.

        for (ntpr, partition_objects) in &mut self.partitions {
            partition_objects
                .segment_objects
                .sort_by_key(|so| so.base_offset);
        }
        Ok(())
    }

    /// Yield a byte stream for each segment
    pub fn stream(
        &self,
        ntpr: &NTPR,
    ) -> Pin<Box<dyn Stream<Item = aws_smithy_http::byte_stream::ByteStream> + '_>> {
        // TODO error handling for parittion DNE
        let partition_objects = self.partitions.get(ntpr).unwrap();
        Box::pin(
            stream::iter(0..partition_objects.segment_objects.len())
                .then(|i| self.stream_one(&partition_objects.segment_objects[i])),
        )
    }

    pub async fn stream_one(&self, po: &SegmentObject) -> aws_smithy_http::byte_stream::ByteStream {
        // TOOD Handle request failure
        debug!("stream_one: {}", po.key);
        self.client
            .get_object()
            .bucket(&self.bucket_name)
            .key(po.key.to_string())
            .send()
            .await
            .unwrap()
            .body
    }

    async fn ingest_manifest(&mut self, key: &str) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref partition_manifest_key: Regex =
                Regex::new("[a-f0-9]+/([^]]+)/([^]]+)/(\\d+)_(\\d+)/manifest.json").unwrap();
        }
        if let Some(grps) = partition_manifest_key.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<u64>().unwrap();
            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic: topic,
                    partition_id: partition_id,
                },
                revision_id: partition_revision,
            };
            // TODO: I don't really want to surface the error here+now if it's
            // transient: retry wrapper?  Maybe aws-sdk-s3 already has one?
            let get_r = self
                .client
                .get_object()
                .bucket(&self.bucket_name)
                .key(key)
                .send()
                .await?;
            let bytes = get_r.body.collect().await?.into_bytes();

            // Note: assuming memory is sufficient for manifests
            let manifest: PartitionManifest = serde_json::from_slice(&bytes)?;
        } else {
            warn!("Malformed manifest key {}", key)
            // TODO: surface as an error?
        }
        Ok(())
    }

    fn ingest_segment(&mut self, key: &str) {
        lazy_static! {
            static ref segment_key: Regex = Regex::new(
                "[a-f0-9]+/([^]]+)/([^]]+)/(\\d+)_(\\d+)/(\\d+)-(\\d+)-(\\d+)-(\\d+)-v1.log.(\\d+)"
            )
            .unwrap();
        }
        if let Some(grps) = segment_key.captures(key) {
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<u64>().unwrap();
            let start_offset = grps.get(5).unwrap().as_str().parse::<u64>().unwrap();
            let _committed_offset = grps.get(6).unwrap().as_str();
            let _size_bytes = grps.get(7).unwrap().as_str();
            let _original_term = grps.get(8).unwrap().as_str();
            let _upload_term = grps.get(9).unwrap().as_str();
            debug!(
                "Segment {}/{}/{} {} (key {}",
                ns, topic, partition_id, start_offset, key
            );

            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic: topic,
                    partition_id: partition_id,
                },
                revision_id: partition_revision,
            };
            let obj = SegmentObject {
                key: key.to_string(),
                base_offset: start_offset,
            };
            let values = self
                .partitions
                .entry(ntpr)
                .or_insert_with(|| PartitionObjects::new());
            values.segment_objects.push(obj);
        } else {
            debug!("Ignoring non-segment-like key {}", key)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionManifestSegment {
    base_offset: u64,
    committed_offset: u64,
    delta_offset: u64,
    delta_offset_end: u64,
    base_timestamp: u64,
    max_timestamp: u64,
    is_compacted: bool,
    size_bytes: i64,
    archiver_term: u64,
    segment_term: u64,
    sname_format: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionManifest {
    version: u32,
    namespace: String,
    topic: String,
    partition: u32,
    revision: u32,
    last_offset: u64,
    insync_offset: u64,
    start_offset: u64,
    last_uploaded_compacted_offset: u64,
    segments: HashMap<String, PartitionManifestSegment>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    #[test_log::test(tokio::test)]
    async fn test_manifest_decode() {
        let cargo_path = env::var("CARGO_MANIFEST_DIR").unwrap();
        let filename = cargo_path + "/resources/test/manifest.json";
        let mut file = File::open(filename).await.unwrap();

        let mut contents = String::new();
        file.read_to_string(&mut contents).await.unwrap();

        let manifest: PartitionManifest = serde_json::from_str(&contents).unwrap();
        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.segments.len(), 3);
        assert_eq!(manifest.namespace, "kafka");
        assert_eq!(manifest.topic, "tiered");
        assert_eq!(manifest.partition, 4);
    }
}
