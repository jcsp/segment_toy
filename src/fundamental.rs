use std::fmt;

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct NTP {
    pub namespace: String,
    pub topic: String,
    pub partition_id: u32,
}

impl fmt::Display for NTP {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "{}/{}/{}",
            self.namespace, self.topic, self.partition_id
        ))
    }
}

/// A Topic, uniquely identified by its revision ID
#[derive(Eq, PartialEq, Hash, Debug)]
pub struct NTR {
    pub namespace: String,
    pub topic: String,
    pub revision_id: u64,
}

impl fmt::Display for NTR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "{}/{}_{}",
            self.namespace, self.topic, self.revision_id
        ))
    }
}

/// A Partition, uniquely identified by its revision ID
#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct NTPR {
    pub ntp: NTP,
    pub revision_id: u64,
}

impl NTPR {
    pub fn to_ntr(&self) -> NTR {
        return NTR {
            namespace: self.ntp.namespace.clone(),
            topic: self.ntp.topic.clone(),
            revision_id: self.revision_id,
        };
    }
}

impl fmt::Display for NTPR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}_{}", self.ntp, self.revision_id))
    }
}
