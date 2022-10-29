use serde::{Deserialize, Serialize};
use std::env::Vars;

use bincode::Encode;

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[repr(C, packed)]
pub struct RecordBatchHeader {
    pub header_crc: u32,
    pub size_bytes: i32,
    pub base_offset: u64,
    pub record_batch_type: i8,
    pub crc: u32,

    pub record_batch_attributes: u16,
    pub last_offset_delta: i32,
    pub first_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
}

/// For batch header_crc calculation: little-endian encode this
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Encode)]
pub struct RecordBatchHeaderCrcFirst {
    pub size_bytes: i32,
    pub base_offset: u64,
    pub record_batch_type: i8,
    pub crc: u32,
}

/// For batch crc calculation: big-endian encode this to get the initial bytes
/// for batch CRC (then append record body bytes)
#[derive(Serialize, Deserialize, Debug, Copy, Clone, Encode)]
pub struct RecordBatchHeaderCrcSecond {
    pub record_batch_attributes: u16,
    pub last_offset_delta: i32,
    pub first_timestamp: u64,
    pub max_timestamp: u64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
}

#[repr(i8)]
#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum RecordBatchType {
    RaftData = 0x1,
    RaftConfig = 0x2,
    Controller = 3,
    KVStore = 4,
    Checkpoint = 5,
    TopicManagementCmd = 6,
    GhostBatch = 7,
    IdAllocator = 8,
    TxPrepare = 9,
    TxFence = 10,
    TmUpdate = 11,
    UserManagementCmd = 12,
    AclManagementCmd = 13,
    GroupPrepareTx = 14,
    GroupCommitTx = 15,
    GroupAbortTx = 16,
    NodeManagementCmd = 17,
    DataPolicyManagementCmd = 18,
    ArchivalMetadata = 19,
    ClusterConfigCmd = 20,
    FeatureUpdate = 21,
    ClusterBootstrapCmd = 22,
    Max = 23,
}

#[derive(Deserialize, Debug)]
pub struct RecordHeader {
    key: Vec<u8>,
    val: Vec<u8>,
}

/// Record does not implement Deserialize, its encoding is too quirky to make it worthwhile
/// to try and cram into a consistent serde encoding style.
pub struct Record {
    len: u32,
    attrs: i8,
    ts_delta: u32,
    offset_delta: u32,
    key: Vec<u8>,
    value: Vec<u8>,
}
