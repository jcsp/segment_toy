use crate::error::BucketReaderError;
use crate::fundamental::{RaftTerm, RawOffset, NTP, NTPR, NTR};
use crate::ntp_mask::NTPFilter;
use crate::remote_types::{ArchivePartitionManifest, PartitionManifest, TopicManifest};
use async_stream::stream;
use futures::stream::{BoxStream, Stream};
use futures::{pin_mut, StreamExt};
use lazy_static::lazy_static;
use log::{debug, info, warn};
use object_store::{GetResult, ObjectMeta, ObjectStore};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::StreamMap;

/// A segment object
#[derive(Clone, Serialize, Deserialize)]
pub struct SegmentObject {
    pub key: String,
    pub base_offset: RawOffset,
    pub upload_term: RaftTerm,
    pub original_term: RaftTerm,
    pub size_bytes: u64,
}

/// The raw objects for a NTP, discovered by scanning the bucket: this is distinct
/// from the partition manifest, but on a health system the two should be very similar.
#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionObjects {
    pub segment_objects: BTreeMap<RawOffset, SegmentObject>,

    // Segments not included in segment_objects because they conflict with another
    // segment, e.g. two uploads with the same base offset but different terms.
    pub dropped_objects: Vec<String>,

    // These dropped objects _might_ be preferable to some objects with the same
    // base_offset that are present in `segment_objects`: when reconstructing metadata,
    // it may be necessary to fully read segments to make a decision about which to
    // use.  If this vector is empty, then `segment_objects` may be treated as a robust
    // source of truth for the list of segments to include in a reconstructed partition
    // manifest.
    pub dropped_objects_ambiguous: Vec<String>,
}

impl PartitionObjects {
    fn new() -> Self {
        Self {
            segment_objects: BTreeMap::new(),
            dropped_objects: Vec::new(),
            dropped_objects_ambiguous: Vec::new(),
        }
    }

    fn push(&mut self, obj: SegmentObject) {
        let existing = self.segment_objects.get(&obj.base_offset);
        let mut ambiguous = false;
        if let Some(existing) = existing {
            if existing.upload_term > obj.upload_term {
                self.dropped_objects.push(obj.key);
                return;
            } else if existing.upload_term == obj.upload_term {
                // Ambiguous case: two objects at same base offset uploaded in the same term can be:
                // - An I/O error, then uploading a larger object later because there's more data
                // - Compaction re-upload, where a smaller object replaces a larger one
                // - Adjacent segment compaction, where a larger object replaces a smaller one.
                //
                // It is safer to prefer larger objects, as this avoids any possibility of
                // data loss if we get it wrong, and a reader can intentionally stop reading
                // when it gets to an offset that should be provided by the following segment.
                //
                // The output from reading a partition based on this class's reconstruction
                // may therefore see duplicate batches, and should account for that.
                if existing.size_bytes > obj.size_bytes {
                    self.dropped_objects_ambiguous.push(obj.key);
                    return;
                } else {
                    ambiguous = true;
                }
            }

            // Fall through and permit the input object to replace the existing
            // object at the same base offset.
        }

        let replaced = self.segment_objects.insert(obj.base_offset, obj);
        if let Some(replaced) = replaced {
            if ambiguous {
                self.dropped_objects_ambiguous.push(replaced.key)
            } else {
                self.dropped_objects.push(replaced.key)
            }
        }
    }

    /// All the objects we know about, including those that may be redundant/orphan.
    pub fn all_keys(&self) -> impl Iterator<Item = String> + '_ {
        let i1 = self.dropped_objects.iter().map(|o| o.clone());
        let i2 = self
            .segment_objects
            .values()
            .into_iter()
            .map(|o| o.key.clone());
        let i3 = self.dropped_objects_ambiguous.iter().map(|o| o.clone());
        i3.chain(i2.chain(i1))
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
        if !self.malformed_manifests.is_empty()
            || !self.malformed_topic_manifests.is_empty()
            || !self.missing_segments.is_empty()
        {
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
        I: Iterator<Item = J> + ExactSizeIterator,
        J: std::fmt::Display,
        T: IntoIterator<IntoIter = I, Item = J>,
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

#[derive(Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    // This field is not logically optional for a well-formed partition's metadata, but is
    // physically optional here because we may discover archive manifests prior to discovering
    // the head manifest.
    pub head_manifest: Option<PartitionManifest>,
    pub archive_manifests: Vec<ArchivePartitionManifest>,
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

async fn list_parallel<'a>(
    client: &'a dyn ObjectStore,
    parallelism: usize,
) -> Result<impl Stream<Item = object_store::Result<ObjectMeta>> + 'a, object_store::Error> {
    assert!(parallelism == 1 || parallelism == 16 || parallelism == 256);

    Ok(stream! {
        let mut stream_map = StreamMap::new();

        for i in 0..parallelism {
            let prefix = if parallelism == 1 {
                "".to_string()
            } else if parallelism == 16 {
                format!("{:1x}", i)
            } else if parallelism == 256 {
                format!("{:02x}", i)
            } else {
                panic!();
            };

            let stream_key = prefix.clone();
            let prefix_path = object_store::path::Path::from(prefix);
            match client.list(Some(&prefix_path)).await {
                Ok(s) => {
                    debug!("Streaming keys for prefix '{}'", prefix_path);
                    stream_map.insert(stream_key, s);
                },
                Err(e) => {
                    warn!("Error streaming keys for prefix '{}'", prefix_path);
                    yield Err(e);
                }
            };
        }

        while let Some(item) = stream_map.next().await {
            debug!("Yielding item...");
            yield item.1;
        }
    })
}

/// Find all the partitions and their segments within a bucket
pub struct BucketReader {
    pub partitions: HashMap<NTPR, PartitionObjects>,
    pub partition_manifests: HashMap<NTPR, PartitionMetadata>,
    pub topic_manifests: HashMap<NTR, TopicManifest>,
    pub anomalies: Anomalies,
    pub client: Arc<dyn ObjectStore>,
}

#[derive(Serialize, Deserialize)]
struct SavedBucketReader {
    pub partitions: HashMap<NTPR, PartitionObjects>,
    pub partition_manifests: HashMap<NTPR, PartitionMetadata>,
    pub topic_manifests: HashMap<NTR, TopicManifest>,
}

type SegmentStream = BoxStream<'static, object_store::Result<bytes::Bytes>>;
type SegmentStreamResult = Result<SegmentStream, BucketReaderError>;

impl BucketReader {
    pub async fn from_file(
        path: &str,
        client: Arc<dyn ObjectStore>,
    ) -> Result<Self, tokio::io::Error> {
        let mut file = tokio::fs::File::open(path).await.unwrap();
        let mut buf: String = String::new();
        file.read_to_string(&mut buf).await?;
        let saved_state = serde_json::from_str::<SavedBucketReader>(&buf).unwrap();
        Ok(Self {
            partitions: saved_state.partitions,
            partition_manifests: saved_state.partition_manifests,
            topic_manifests: saved_state.topic_manifests,
            anomalies: Anomalies::new(),
            client,
        })
    }

    pub async fn to_file(&self, path: &str) -> Result<(), tokio::io::Error> {
        let saved_state = SavedBucketReader {
            partitions: self.partitions.clone(),
            partition_manifests: self.partition_manifests.clone(),
            topic_manifests: self.topic_manifests.clone(),
        };

        let buf = serde_json::to_vec(&saved_state).unwrap();

        let mut file = tokio::fs::File::create(path).await.unwrap();
        file.write_all(&buf).await?;
        info!("Wrote {} bytes to {}", buf.len(), path);
        Ok(())
    }

    pub async fn new(client: Arc<dyn ObjectStore>) -> Self {
        Self {
            partitions: HashMap::new(),
            partition_manifests: HashMap::new(),
            topic_manifests: HashMap::new(),
            anomalies: Anomalies::new(),
            client,
        }
    }

    pub async fn scan(&mut self, filter: &NTPFilter) -> Result<(), BucketReaderError> {
        // TODO: for this to work at unlimited scale, we need:
        //  - load the manifests first, and only bother storing extra vectors
        //    of segments if those segments aren't in the manifest
        //  - or use a disk-spilling database for all this state.

        // Must clone because otherwise we hold immutable reference to `self` while
        // iterating through list results
        let client = self.client.clone();

        // TODO: we may estimate the total number of objects in the bucket by
        // doing a listing with delimiter at the base of the bucket.  (1000 / (The highest
        // hash prefix we see)) * 4E9 -> approximate object count

        // =======
        // Phase 1: List all objects in the bucket
        // =======

        let list_stream = list_parallel(client.as_ref(), 16).await?;
        pin_utils::pin_mut!(list_stream);

        #[derive(Clone)]
        enum FetchKey {
            PartitionManifest(String),
            ArchiveManifest(String),
            TopicManifest(String),
        }

        impl FetchKey {
            fn as_str(&self) -> &str {
                match self {
                    FetchKey::PartitionManifest(s) => s,
                    FetchKey::TopicManifest(s) => s,
                    FetchKey::ArchiveManifest(s) => s,
                }
            }
        }

        let mut manifest_keys: Vec<FetchKey> = vec![];

        fn maybe_stash_partition_key(keys: &mut Vec<FetchKey>, k: FetchKey, filter: &NTPFilter) {
            lazy_static! {
                static ref META_NTP_PREFIX: Regex =
                    Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/(\\d+)_(\\d+)/.+").unwrap();
            }
            if let Some(grps) = META_NTP_PREFIX.captures(k.as_str()) {
                let ns = grps.get(1).unwrap().as_str();
                let topic = grps.get(2).unwrap().as_str();
                // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
                let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
                let partition_revision = grps.get(4).unwrap().as_str().parse::<i64>().unwrap();

                if filter.match_parts(ns, topic, Some(partition_id), Some(partition_revision)) {
                    debug!("Stashing partition manifest key {}", k.as_str());
                    keys.push(k);
                } else {
                    debug!("Dropping filtered-out manifest key {}", k.as_str());
                }
            } else {
                // Drop it.
                warn!("Dropping malformed manifest key {}", k.as_str());
            }
        }

        fn maybe_stash_topic_key(keys: &mut Vec<FetchKey>, k: FetchKey, filter: &NTPFilter) {
            lazy_static! {
                static ref META_NTP_PREFIX: Regex =
                    Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/.+").unwrap();
            }
            if let Some(grps) = META_NTP_PREFIX.captures(k.as_str()) {
                let ns = grps.get(1).unwrap().as_str();
                let topic = grps.get(2).unwrap().as_str();

                if filter.match_parts(ns, topic, None, None) {
                    debug!("Stashing topic manifest key {}", k.as_str());
                    keys.push(k);
                } else {
                    debug!("Dropping filtered-out topic key {}", k.as_str());
                }
            } else {
                // Drop it.
                warn!("Dropping malformed topic key {}", k.as_str());
            }
        }

        let mut object_k = 0;
        while let Some(o_r) = list_stream.next().await {
            let o = o_r?;

            let key = o.location.to_string();
            if key.ends_with("/manifest.json") || key.ends_with("/manifest.bin") {
                maybe_stash_partition_key(
                    &mut manifest_keys,
                    FetchKey::PartitionManifest(key),
                    filter,
                );
            } else if key.ends_with("/topic_manifest.json") {
                maybe_stash_topic_key(&mut manifest_keys, FetchKey::TopicManifest(key), filter);
            } else if key.contains("manifest.json.") || key.contains("manifest.bin.") {
                maybe_stash_partition_key(
                    &mut manifest_keys,
                    FetchKey::ArchiveManifest(key),
                    filter,
                );
            } else if key.ends_with(".index") {
                // TODO: do something with index files: currently ignore them as they are
                // somewhat disposable.  Should track .tx and .index files within
                // PartitionObjects: we need .tx files to implement read, and we need
                // both when doing a dump of an ntp for debug.
                debug!("Ignoring index key {}", key);
            } else {
                debug!("Parsing segment key {}", key);
                self.ingest_segment(&key, &filter, o.size as u64);
            }

            object_k += 1;
            if object_k % 10000 == 0 {
                info!("Scan progress: {} objects", object_k);
            }
        }

        // =======
        // Phase 2: Fetch all the manifests
        // =======

        fn getter_stream(
            client: Arc<dyn ObjectStore>,
            keys: Vec<FetchKey>,
        ) -> impl Stream<
            Item = impl std::future::Future<
                Output = (FetchKey, Result<bytes::Bytes, object_store::Error>),
            >,
        > {
            stream! {
                for key in keys {
                    let client_clone = client.clone();
                    let raw_key = match &key {
                        FetchKey::PartitionManifest(s) => s.clone(),
                        FetchKey::TopicManifest(s) => s.clone(),
                        FetchKey::ArchiveManifest(s) =>s.clone(),
                    };
                    yield async move {(key.clone(), client_clone
                                    .get(&object_store::path::Path::from(raw_key))
                                    .await.unwrap() // TODO
                                    .bytes()
                                    .await)}
                }
            }
        }

        let buffered = getter_stream(self.client.clone(), manifest_keys).buffer_unordered(16);
        pin_mut!(buffered);
        while let Some(result) = buffered.next().await {
            let (key, body_r) = result;
            let body = body_r?;
            match key {
                FetchKey::PartitionManifest(key) => {
                    debug!(
                        "Parsing {} bytes from partition manifest key {}",
                        body.len(),
                        key
                    );
                    self.ingest_partition_manifest(&key, body).await?;
                }
                FetchKey::TopicManifest(key) => {
                    debug!(
                        "Parsing {} bytes from topic manifest key {}",
                        body.len(),
                        key
                    );
                    self.ingest_topic_manifest(&key, body).await?;
                }
                FetchKey::ArchiveManifest(key) => {
                    debug!(
                        "Parsing {} bytes from archive partition manifest key {}",
                        body.len(),
                        key
                    );
                    self.ingest_archive_manifest(&key, body).await?;
                }
            }
        }

        debug!(
            "Loaded {} partition manifests",
            self.partition_manifests.len()
        );
        debug!("Loaded {} topic manifests", self.topic_manifests.len());

        // =======
        // Phase 3: Analyze for correctness
        // =======

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
                    let manifest_key_bin = PartitionManifest::manifest_key(ntpr, "bin");
                    let manifest_key_json = PartitionManifest::manifest_key(ntpr, "json");
                    if self
                        .anomalies
                        .malformed_manifests
                        .contains(&manifest_key_bin)
                        || self
                            .anomalies
                            .malformed_manifests
                            .contains(&manifest_key_json)
                    {
                        debug!("Not reporting {} as missing because it's already reported as malformed", ntpr);
                    } else {
                        self.anomalies.ntpr_no_manifest.insert(ntpr.clone());
                    }
                }
                Some(p_metadata) => {
                    for o in partition_objects.segment_objects.values() {
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
            // We will validate the manifest.  If there is no head manifest, that is an anomaly.
            let partition_manifest = match &partition_metadata.head_manifest {
                Some(pm) => pm,
                None => {
                    // No head manifest: this is a partition for which we found archive
                    // manifests but no head manifest.
                    for am in &partition_metadata.archive_manifests {
                        self.anomalies
                            .archive_manifests_outside_manifest
                            .push(am.key(ntpr))
                    }
                    continue;
                }
            };

            let raw_objects = self.partitions.get(&ntpr);

            // For all segments in the manifest, check they were found in the bucket
            debug!(
                "Checking {} ({} segments)",
                partition_manifest.ntp(),
                partition_manifest.segments.as_ref().map_or(0, |s| s.len())
            );
            if let Some(manifest_segments) = &partition_manifest.segments {
                for (segment_short_name, segment) in manifest_segments {
                    if let Some(so) = partition_manifest.start_offset {
                        if segment.committed_offset < so as u64 {
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
                        if let Some(raw_objects) = raw_objects {
                            let found = raw_objects
                                .segment_objects
                                .get(&(segment.base_offset as RawOffset));
                            match found {
                                Some(so) => {
                                    if expect_key != so.key {
                                        self.anomalies.missing_segments.push(expect_key);
                                    } else {
                                        // Matched up manifest segment with object in bucket
                                    }
                                }
                                None => {
                                    self.anomalies.missing_segments.push(expect_key);
                                }
                            }
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

        Ok(())
    }

    /// Yield a byte stream for each segment
    pub fn stream(
        &self,
        ntpr: &NTPR,
        //) -> Pin<Box<dyn Stream<Item = Result<BoxStream<'static, object_store::Result<bytes::Bytes>>, BucketReaderError> + '_>>
    ) -> impl Stream<Item = SegmentStreamResult> + '_ {
        // TODO error handling for parittion DNE

        // TODO go via metadata: if we have no manifest, we should synthesize one and validate
        // rather than just stepping throuhg objects naively.
        let partition_objects = self.partitions.get(ntpr).unwrap();
        // Box::pin(
        //     futures::stream::iter(0..partition_objects.segment_objects.len())
        //         .then(|i| self.stream_one(&partition_objects.segment_objects[i])),
        // )
        // Box::pin(futures::stream::iter(
        //     partition_objects
        //         .segment_objects
        //         .values()
        //         .map(|so| self.stream_one(&so.key)),
        // ))
        stream! {
            for so in partition_objects.segment_objects.values() {
                yield self.stream_one(&so.key).await;
            }
        }
    }

    // TODO: return type should include name of the segment we're streaming, so that
    // caller can include it in logs.
    pub async fn stream_one(&self, key: &String) -> Result<SegmentStream, BucketReaderError> {
        // TOOD Handle request failure
        debug!("stream_one: {}", key);
        let key: &str = &key;
        let path = object_store::path::Path::from(key);
        let get_result = self.client.get(&path).await?;
        match get_result {
            // This code is currently only for use with object storage
            GetResult::File(_, _) => unreachable!(),
            GetResult::Stream(s) => Ok(s),
        }
    }

    fn decode_partition_manifest(
        key: &str,
        buf: bytes::Bytes,
    ) -> Result<PartitionManifest, BucketReaderError> {
        if key.ends_with(".json") || key.contains(".json.") {
            Ok(serde_json::from_slice(&buf)?)
        } else if key.ends_with(".bin") || key.contains(".bin.") {
            Ok(PartitionManifest::from_bytes(buf)?)
        } else {
            Err(BucketReaderError::SyntaxError("Malformed key".to_string()))
        }
    }

    async fn ingest_partition_manifest(
        &mut self,
        key: &str,
        body: bytes::Bytes,
    ) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref PARTITION_MANIFEST_KEY: Regex =
                Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/(\\d+)_(\\d+)/manifest.(json|bin)")
                    .unwrap();
        }
        if let Some(grps) = PARTITION_MANIFEST_KEY.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<i64>().unwrap();
            let ntpr = NTPR {
                ntp: NTP {
                    namespace: ns,
                    topic,
                    partition_id,
                },
                revision_id: partition_revision,
            };

            let manifest = match Self::decode_partition_manifest(key, body) {
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
                    self.partition_manifests.insert(
                        ntpr,
                        PartitionMetadata {
                            head_manifest: Some(manifest),
                            archive_manifests: vec![],
                        },
                    );
                }
            }
        } else {
            warn!("Malformed partition manifest key {}", key);
            self.anomalies.malformed_manifests.push(key.to_string());
        }
        Ok(())
    }

    async fn ingest_archive_manifest(
        &mut self,
        key: &str,
        body: bytes::Bytes,
    ) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref PARTITION_MANIFEST_KEY: Regex =
                Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/(\\d+)_(\\d+)/manifest.(?:json|bin)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)").unwrap();
        }
        if let Some(grps) = PARTITION_MANIFEST_KEY.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<i64>().unwrap();

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

            // Note: assuming memory is sufficient for manifests
            debug!("Storing archive manifest for {} from key {}", ntpr, key);

            let manifest: PartitionManifest = match Self::decode_partition_manifest(key, body) {
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
                    self.partition_manifests.insert(
                        ntpr,
                        PartitionMetadata {
                            head_manifest: None,
                            archive_manifests: vec![archive_manifest],
                        },
                    );
                }
            }
        } else {
            warn!("Malformed partition archive manifest key {}", key);
            self.anomalies.malformed_manifests.push(key.to_string());
        }
        Ok(())
    }

    async fn ingest_topic_manifest(
        &mut self,
        key: &str,
        body: bytes::Bytes,
    ) -> Result<(), BucketReaderError> {
        lazy_static! {
            static ref PARTITION_MANIFEST_KEY: Regex =
                Regex::new("[a-f0-9]+/meta/([^]]+)/([^]]+)/topic_manifest.json").unwrap();
        }
        if let Some(grps) = PARTITION_MANIFEST_KEY.captures(key) {
            // Group::get calls are safe to unwrap() because regex always has those groups if it matched
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();

            if let Ok(manifest) = serde_json::from_slice::<TopicManifest>(&body) {
                let ntr = NTR {
                    namespace: ns,
                    topic,
                    revision_id: manifest.revision_id as i64,
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

    fn ingest_segment(&mut self, key: &str, filter: &NTPFilter, object_size: u64) {
        lazy_static! {
            // e.g. 8606-92-v1.log.92
            // TODO: combine into one regex
            static ref SEGMENT_V1_KEY: Regex = Regex::new(
                "[a-f0-9]+/([^]]+)/([^]]+)/(\\d+)_(\\d+)/(\\d+)-(\\d+)-v1.log.(\\d+)"
            )
            .unwrap();
        }

        lazy_static! {
            static ref SEGMENT_KEY: Regex = Regex::new(
                "[a-f0-9]+/([^]]+)/([^]]+)/(\\d+)_(\\d+)/(\\d+)-(\\d+)-(\\d+)-(\\d+)-v1.log.(\\d+)"
            )
            .unwrap();
        }
        let (ntpr, segment) = if let Some(grps) = SEGMENT_KEY.captures(key) {
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<i64>().unwrap();
            let start_offset = grps.get(5).unwrap().as_str().parse::<RawOffset>().unwrap();
            let _committed_offset = grps.get(6).unwrap().as_str();
            let size_bytes = grps.get(7).unwrap().as_str().parse::<u64>().unwrap();
            let original_term = grps.get(8).unwrap().as_str().parse::<RaftTerm>().unwrap();
            let upload_term = grps.get(9).unwrap().as_str().parse::<RaftTerm>().unwrap();
            debug!(
                "ingest_segment v2+ {}/{}/{} {} (key {}",
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

            if !filter.match_ntp(&ntpr.ntp) {
                return;
            }

            (
                ntpr,
                SegmentObject {
                    key: key.to_string(),
                    base_offset: start_offset,
                    upload_term,
                    original_term,
                    size_bytes,
                },
            )
        } else if let Some(grps) = SEGMENT_V1_KEY.captures(key) {
            let ns = grps.get(1).unwrap().as_str().to_string();
            let topic = grps.get(2).unwrap().as_str().to_string();
            // (TODO: these aren't really-truly safe to unwrap because the string might have had too many digits)
            let partition_id = grps.get(3).unwrap().as_str().parse::<u32>().unwrap();
            let partition_revision = grps.get(4).unwrap().as_str().parse::<i64>().unwrap();
            let start_offset = grps.get(5).unwrap().as_str().parse::<RawOffset>().unwrap();
            let original_term = grps.get(6).unwrap().as_str().parse::<RaftTerm>().unwrap();
            let upload_term = grps.get(7).unwrap().as_str().parse::<RaftTerm>().unwrap();
            debug!(
                "ingest_segment v1 {}/{}/{} {} (key {}",
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

            if !filter.match_ntp(&ntpr.ntp) {
                return;
            }

            (
                ntpr,
                SegmentObject {
                    key: key.to_string(),
                    base_offset: start_offset,
                    upload_term,
                    original_term,
                    // V1 segment name omits size, use the size from the object store listing
                    size_bytes: object_size,
                },
            )
        } else {
            debug!("Ignoring non-segment-like key {}", key);
            self.anomalies.unknown_keys.push(key.to_string());
            return;
        };

        let values = self
            .partitions
            .entry(ntpr)
            .or_insert_with(|| PartitionObjects::new());
        values.push(segment);
    }
}
