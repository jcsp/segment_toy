use crate::fundamental::{NTP, NTPR, NTR};
use futures::stream::{BoxStream, Stream};
use futures::{stream, StreamExt};
use lazy_static::lazy_static;
use log::{debug, warn};
use regex::Regex;
use serde::{Serialize};
use serde_json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use object_store::{ObjectStore};
use crate::remote_types::{PartitionManifest, TopicManifest, ArchivePartitionManifest};
use crate::error::BucketReaderError;

pub struct SegmentObject {
    key: String,
    base_offset: u64,
    original_term: u64,
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

#[derive(Serialize)]
pub struct Anomalies {
    /// Segment objects not mentioned in their manifest
    pub segments_outside_manifest: Vec<String>,

    /// Archive manifests not referenced by a head manifest
    pub archive_manifests_outside_manifest: Vec<String>,

    /// PartitionManifest that could not be loaded
    pub malformed_manifests: Vec<String>,

    /// TopicManifest that could not be loaded
    pub malformed_topic_manifests: Vec<String>,

    /// NTPR that had segment objects, but no partition manifest
    pub ntpr_no_manifest: HashSet<NTPR>,

    /// NTR that had segment objects and/or partition manifests, but no topic manifest
    pub ntr_no_topic_manifest: HashSet<NTR>,

    /// Keys that do not look like any object we expect
    pub unknown_keys: Vec<String>,

    /// Segments referenced by a manifest, which do not exist in the bucket
    pub missing_segments: Vec<String>,
}

impl Anomalies {
    pub fn status(&self) -> AnomalyStatus {
        if !self.malformed_manifests.is_empty() || !self.malformed_topic_manifests.is_empty() || !self.missing_segments.is_empty() {
            AnomalyStatus::Corrupt
        } else if !self.segments_outside_manifest.is_empty()
            || !self.ntpr_no_manifest.is_empty()
            || !self.ntr_no_topic_manifest.is_empty()
            || !self.unknown_keys.is_empty()
        {
            AnomalyStatus::Dirty
        } else {
            AnomalyStatus::Clean
        }
    }

    fn report_line<
        I: Iterator<Item=J> + ExactSizeIterator,
        J: std::fmt::Display,
        T: IntoIterator<IntoIter=I, Item=J>,
    >(
        desc: &str,
        coll: T,
    ) -> String {
        let mut result = String::new();
        result.push_str(&format!("{}: ", desc));
        let mut first = true;
        for i in coll {
            if first {
                result.push_str("\n");
                first = false;
            }
            result.push_str(&format!("  {}\n", i));
        }
        if first {
            // No items.
            result.push_str("OK\n");
        }
        result
    }

    pub fn report(&self) -> String {
        let mut result = String::new();
        result.push_str(&Self::report_line(
            "Segments outside manifest",
            &self.segments_outside_manifest,
        ));
        result.push_str(&Self::report_line(
            "Archive manifests outside manifest",
            &self.archive_manifests_outside_manifest,
        ));
        result.push_str(&Self::report_line(
            "Malformed partition manifests",
            &self.malformed_manifests,
        ));
        result.push_str(&Self::report_line(
            "Malformed topic manifests",
            &self.malformed_topic_manifests,
        ));
        result.push_str(&Self::report_line(
            "Partitions with segments but no manifest",
            &self.ntpr_no_manifest,
        ));
        result.push_str(&Self::report_line(
            "Topics with segments but no topic manifest",
            &self.ntr_no_topic_manifest,
        ));
        result.push_str(&Self::report_line(
            "Segments referenced in manifest but not found",
            &self.missing_segments,
        ));
        result.push_str(&Self::report_line("Unexpected keys", &self.unknown_keys));
        result
    }
}

pub enum AnomalyStatus {
    // Everything lines up: every segment is in a manifest, every partition+topic has a manifest
    Clean,
    // An expected situation requiring cleanup, such as segments outside the manifest
    Dirty,
    // Something has happened that should never happen (e.g. unreadable manifest), or that prevents us knowing
    // quite how to handle the data (e.g. no topic manifest)
    Corrupt,
}

impl Anomalies {
    fn new() -> Anomalies {
        Self {
            segments_outside_manifest: vec![],
            archive_manifests_outside_manifest: vec![],
            malformed_manifests: vec![],
            malformed_topic_manifests: vec![],
            ntpr_no_manifest: HashSet::new(),
            ntr_no_topic_manifest: HashSet::new(),
            unknown_keys: vec![],
            missing_segments: vec![],
        }
    }
}

pub struct PartitionMetadata {
    // This field is not logically optional for a well-formed partition's metadata, but is
    // physically optional here because we may discover archive manifests prior to discovering
    // the head manifest.
    head_manifest: Option<PartitionManifest>,
    archive_manifests: Vec<ArchivePartitionManifest>,

}

impl PartitionMetadata {
    pub fn contains_segment(&self, seg: &SegmentObject) -> bool {
        let shortname = format!("{}-{}-v1.log", seg.base_offset, seg.original_term);

        if let Some(hm) = &self.head_manifest {
            if hm.contains_segment_shortname(&shortname) {
                return true;
            }
        }

        for am in &self.archive_manifests {
            if am.manifest.contains_segment_shortname(&shortname) {
                return true;
            }
        }

        false
    }
}

/// Find all the partitions and their segments within a bucket
pub struct BucketReader {
    pub partitions: HashMap<NTPR, PartitionObjects>,
    pub partition_manifests: HashMap<NTPR, PartitionMetadata>,
    pub topic_manifests: HashMap<NTR, TopicManifest>,
    pub anomalies: Anomalies,
    pub client: Arc<dyn ObjectStore>,
}

impl BucketReader {
    pub async fn new(client: Arc<dyn ObjectStore>) -> Self {
        Self {
            partitions: HashMap::new(),
            partition_manifests: HashMap::new(),
            topic_manifests: HashMap::new(),
            anomalies: Anomalies::new(),
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

        // Must clone because otherwise we hold immutable reference to `self` while
        // iterating through list results
        let client = self.client.clone();

        let mut list_stream = client.list(None).await?;

        while let Some(result_r) = list_stream.next().await {
            let o = result_r?;
            let key = o.location.to_string();
            if key.ends_with("/manifest.json") || key.ends_with("/manifest.bin") {
                debug!("Parsing partition manifest key {}", key);
                self.ingest_manifest(&key).await?;
            } else if key.ends_with("/topic_manifest.json") {
                debug!("Parsing topic manifest key {}", key);
                self.ingest_topic_manifest(&key).await?;
            } else if key.contains("manifest.json_") || key.contains("manifest.bin_") {
                debug!("Parsing partition archive manifest key {}", key);
                self.ingest_archive_manifest(&key).await?;
            } else if key.ends_with(".index") {
                // TODO: do something with index files: currently ignore them as they are
                // somewhat disposable.
                debug!("Ignoring index key {}", key);
            } else {
                debug!("Parsing segment key {}", key);
                self.ingest_segment(&key);
            }
        }

        debug!(
            "Loaded {} partition manifests",
            self.partition_manifests.len()
        );
        debug!("Loaded {} topic manifests", self.topic_manifests.len());

        for (ntpr, partition_objects) in &mut self.partitions {
            if ntpr.ntp.partition_id == 0 {
                let t_manifest_o = self.topic_manifests.get(&ntpr.to_ntr());
                if let None = t_manifest_o {
                    self.anomalies.ntr_no_topic_manifest.insert(ntpr.to_ntr());
                }
            }

            let p_metadata_o = self.partition_manifests.get(ntpr);
            match p_metadata_o {
                None => {
                    // The manifest may be missing because we couldn't load it, in which
                    // case that is already tracked in malformed_manifests
                    let manifest_key = PartitionManifest::manifest_key(ntpr);
                    if self.anomalies.malformed_manifests.contains(&manifest_key) {
                        debug!("Not reporting {} as missing because it's already reported as malformed", ntpr);
                    } else {
                        self.anomalies.ntpr_no_manifest.insert(ntpr.clone());
                    }
                }
                Some(p_metadata) => {
                    for o in &partition_objects.segment_objects {
                        if !p_metadata.contains_segment(&o) {
                            self.anomalies.segments_outside_manifest.push(o.key.clone());
                        }
                    }
                }
            }

            // TODO: also mutate the lists of objects, to simplify
            //       subsequent processing:
            // - Drop segments that are outside the manifest, unless they are
            //   at an offset higher than the tip of the manifest.
            // - Drop segments that overlap: retain the one that is mentioned
            //   in the manifest, or whichever appears to come from a newer term.
        }

        for (ntpr, partition_metadata) in &self.partition_manifests {
            let mut known_objects: HashSet<String> = HashSet::new();
            if let Some(segment_objects) = self.partitions.get(&ntpr) {
                for o in &segment_objects.segment_objects {
                    known_objects.insert(o.key.clone());
                }
            }

            // We will validate the manifest.  If there is no head manifest, that is an anomaly.
            let partition_manifest = match &partition_metadata.head_manifest {
                Some(pm) => pm,
                None => {
                    // No head manifest: this is a partition for which we found archive
                    // manifests but no head manifest.
                    for am in &partition_metadata.archive_manifests {
                        self.anomalies.archive_manifests_outside_manifest.push(am.key(ntpr))
                    }
                    continue;
                }
            };

            // For all segments in the manifest, check they were found in the bucket
            debug!(
    "Checking {} ({} segments)",
    partition_manifest.ntp(),
    partition_manifest.segments.as_ref().map_or(0, | s | s.len())
    );
            if let Some(manifest_segments) = &partition_manifest.segments {
                for (segment_short_name, segment) in manifest_segments {
                    if let Some(so) = partition_manifest.start_offset {
                        if segment.committed_offset < so {
                            debug!(
    "Not checking {} {}, it is below start offset",
    partition_manifest.ntp(),
    segment_short_name
    );
                            continue;
                        }
                    }

                    debug!(
    "Checking {} {}",
    partition_manifest.ntp(),
    segment_short_name
    );
                    if let Some(expect_key) = partition_manifest.segment_key(segment) {
                        debug!("Calculated segment {}", expect_key);
                        if !known_objects.contains(&expect_key) {
                            self.anomalies.missing_segments.push(expect_key);
                        }
                    }
                }
            }
        }

        for (ntpr, _) in &mut self.partition_manifests {
            let t_manifest_o = self.topic_manifests.get(&ntpr.to_ntr());
            if let None = t_manifest_o {
                self.anomalies.ntr_no_topic_manifest.insert(ntpr.to_ntr());
            }
        }

        for (_ntpr, partition_objects) in &mut self.partitions {
            partition_objects
                .segment_objects
                .sort_by_key(|so| so.base_offset);
        }
        Ok(())
    }

// /// Yield a byte stream for each segment
// pub fn stream(
//     &self,
//     ntpr: &NTPR,
// ) -> Pin<Box<dyn Stream<Item=aws_smithy_http::byte_stream::ByteStream> + '_>> {
//     // TODO error handling for parittion DNE
//     let partition_objects = self.partitions.get(ntpr).unwrap();
//     Box::pin(
//         stream::iter(0..partition_objects.segment_objects.len())
//             .then(|i| self.stream_one(&partition_objects.segment_objects[i])),
//     )
// }
//
// // TODO: return type should include name of the segment we're streaming, so that
// // caller can include it in logs.
// pub async fn stream_one(&self, po: &SegmentObject) -> Result<BoxStream<'static,
//     object_store::Result<bytes::Bytes>>, BucketReaderError> {
//     // TOOD Handle request failure
//     debug!("stream_one: {}", po.key);
//     self.client.get(po.key.into()).await?.into_stream()
// }

    fn decode_partition_manifest(key: &str, buf: bytes::Bytes) -> Result<PartitionManifest, BucketReaderError> {
        if key.ends_with(".json") || key.contains(".json_") {
            Ok(serde_json::from_slice(&buf)?)
        } else if key.ends_with(".bin") || key.contains(".bin_") {
            Ok(PartitionManifest::from_bytes(buf)?)
        } else {
            Err(BucketReaderError::SyntaxError("Malformed key".to_string()))
        }
    }

    async fn ingest_manifest(&mut self, key: &str) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref PARTITION_MANIFEST_KEY: Regex =
                Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/(\\d+)_(\\d+)/manifest.(json|bin)").unwrap();
        }
        if let Some(grps) = PARTITION_MANIFEST_KEY.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<u64>().unwrap();
            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic,
                    partition_id,
                },
                revision_id: partition_revision,
            };
            // TODO: I don't really want to surface the error here+now if it's
            // transient: retry wrapper?
            let path = object_store::path::Path::from(key);
            let bytes = self.client.get(&path).await?.bytes().await?;

            let manifest = match Self::decode_partition_manifest(key, bytes) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Error parsing partition manifest {}: {:?}", key, e);
                    self.anomalies.malformed_manifests.push(key.to_string());
                    return Ok(());
                }
            };

            // Note: assuming memory is sufficient for manifests
            match self.partition_manifests.get_mut(&ntpr) {
                Some(meta) => {
                    meta.head_manifest = Some(manifest);
                }
                None => {
                    self.partition_manifests.insert(ntpr, PartitionMetadata {
                        head_manifest: Some(manifest),
                        archive_manifests: vec![],
                    });
                }
            }
        } else {
            warn!("Malformed partition manifest key {}", key);
            self.anomalies.malformed_manifests.push(key.to_string());
        }
        Ok(())
    }

    async fn ingest_archive_manifest(&mut self, key: &str) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref PARTITION_MANIFEST_KEY: Regex =
                Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/(\\d+)_(\\d+)/manifest.(?:json|bin)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_(\\d+)_(\\d+)").unwrap();
        }
        if let Some(grps) = PARTITION_MANIFEST_KEY.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<u64>().unwrap();

            let base_offset = grps.get(5).unwrap().as_str().parse::<u64>().unwrap();
            let committed_offset = grps.get(6).unwrap().as_str().parse::<u64>().unwrap();
            let base_kafka_offset = grps.get(7).unwrap().as_str().parse::<u64>().unwrap();
            let next_kafka_offset = grps.get(8).unwrap().as_str().parse::<u64>().unwrap();
            let base_ts = grps.get(9).unwrap().as_str().parse::<u64>().unwrap();
            let last_ts = grps.get(10).unwrap().as_str().parse::<u64>().unwrap();

            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic,
                    partition_id,
                },
                revision_id: partition_revision,
            };
            // TODO: I don't really want to surface the error here+now if it's
            // transient: retry wrapper?
            let path = object_store::path::Path::from(key);
            let bytes = self.client.get(&path).await?.bytes().await?;

            // Note: assuming memory is sufficient for manifests
            debug!("Storing archive manifest for {} from key {}", ntpr, key);

            let manifest: PartitionManifest = match Self::decode_partition_manifest(key, bytes) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Error parsing partition archive manifest {}: {:?}", key, e);
                    self.anomalies.malformed_manifests.push(key.to_string());
                    // This is OK because we cleanly logged anomaly.
                    return Ok(());
                }
            };

            let archive_manifest = ArchivePartitionManifest {
                manifest,
                base_offset,
                committed_offset,
                base_kafka_offset,
                next_kafka_offset,
                base_ts,
                last_ts,
            };

            // Note: assuming memory is sufficient for manifests
            match self.partition_manifests.get_mut(&ntpr) {
                Some(meta) => {
                    meta.archive_manifests.push(archive_manifest);
                }
                None => {
                    self.partition_manifests.insert(ntpr, PartitionMetadata {
                        head_manifest: None,
                        archive_manifests: vec![archive_manifest],
                    });
                }
            }
        } else {
            warn!("Malformed partition manifest key {}", key);
            self.anomalies.malformed_manifests.push(key.to_string());
        }
        Ok(())
    }

    async fn ingest_topic_manifest(&mut self, key: &str) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref PARTITION_MANIFEST_KEY: Regex =
                Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/topic_manifest.json").unwrap();
        }
        if let Some(grps) = PARTITION_MANIFEST_KEY.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();

            let path = object_store::path::Path::from(key);
            let bytes = self.client.get(&path).await?.bytes().await?;
            if let Ok(manifest) = serde_json::from_slice::<TopicManifest>(&bytes) {
                let ntr = NTR {
                    namespace: ns,
                    topic,
                    revision_id: manifest.revision_id as u64,
                };
                if let Some(_) = self.topic_manifests.insert(ntr, manifest) {
                    warn!("Two topic manifests for same NTR seen ({})", key);
                }
            } else {
                warn!("Error parsing JSON topic manifest {}", key);
                self.anomalies
                    .malformed_topic_manifests
                    .push(key.to_string());
            }
        } else {
            warn!("Malformed topic manifest key {}", key);
            self.anomalies
                .malformed_topic_manifests
                .push(key.to_string());
        }
        Ok(())
    }

    fn ingest_segment(&mut self, key: &str) {
        lazy_static! {
            static ref SEGMENT_KEY: Regex = Regex::new(
                "[a-f0-9]+/([^]]+)/([^]]+)/(\\d+)_(\\d+)/(\\d+)-(\\d+)-(\\d+)-(\\d+)-v1.log.(\\d+)"
            )
            .unwrap();
        }
        if let Some(grps) = SEGMENT_KEY.captures(key) {
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<u64>().unwrap();
            let start_offset = grps.get(5).unwrap().as_str().parse::<u64>().unwrap();
            let _committed_offset = grps.get(6).unwrap().as_str();
            let _size_bytes = grps.get(7).unwrap().as_str();
            let original_term = grps.get(8).unwrap().as_str().parse::<u64>().unwrap();
            let _upload_term = grps.get(9).unwrap().as_str();
            debug!(
                "ingest_segment {}/{}/{} {} (key {}",
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
                original_term,
            };
            let values = self
                .partitions
                .entry(ntpr)
                .or_insert_with(|| PartitionObjects::new());
            values.segment_objects.push(obj);
        } else {
            debug!("Ignoring non-segment-like key {}", key);
            self.anomalies.unknown_keys.push(key.to_string());
        }
    }
}

