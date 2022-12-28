use crate::NTPR;
use lazy_static::lazy_static;
use regex::Regex;

#[derive(Clone)]
pub struct NTPMask {
    namespace: Option<Regex>,
    topic: Option<Regex>,
    partition: Option<Regex>,
    revision_id: Option<Regex>,
}

impl NTPMask {
    fn init_opt_regex(input: &str) -> Result<Option<Regex>, regex::Error> {
        if input == "*" || input == ".*" {
            Ok(None)
        } else {
            Ok(Some(Regex::new(input)?))
        }
    }

    pub fn match_all() -> Self {
        Self {
            namespace: None,
            topic: None,
            partition: None,
            revision_id: None,
        }
    }

    pub fn from_str(input: &str) -> Result<Self, regex::Error> {
        lazy_static! {
            static ref NTP_MASK_EXPR: Regex =
                Regex::new("([^]]+)/([^]]+)/([^_]+)_([^_]+)").unwrap();
        }

        if let Some(grps) = NTP_MASK_EXPR.captures(input) {
            Ok(NTPMask {
                namespace: Self::init_opt_regex(grps.get(1).unwrap().as_str())?,
                topic: Self::init_opt_regex(grps.get(2).unwrap().as_str())?,
                partition: Self::init_opt_regex(grps.get(3).unwrap().as_str())?,
                revision_id: Self::init_opt_regex(grps.get(4).unwrap().as_str())?,
            })
        } else {
            Err(regex::Error::Syntax(
                "Malformed NTP query string".to_string(),
            ))
        }
    }

    pub fn compare(&self, ntpr: &NTPR) -> bool {
        if let Some(ns_r) = &self.namespace {
            if !ns_r.is_match(&ntpr.ntp.namespace) {
                return false;
            }
        }

        if let Some(ns_t) = &self.topic {
            if !ns_t.is_match(&ntpr.ntp.topic) {
                return false;
            }
        }

        if let Some(ns_p) = &self.partition {
            if !ns_p.is_match(&format!("{}", ntpr.ntp.partition_id)) {
                return false;
            }
        }

        if let Some(ns_r) = &self.revision_id {
            if !ns_r.is_match(&format!("{}", ntpr.revision_id)) {
                return false;
            }
        }

        true
    }
}

impl std::fmt::Display for NTPMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "{}/{}/{}_{}",
            self.namespace.as_ref().map(|r| r.as_str()).unwrap_or("*"),
            self.topic.as_ref().map(|r| r.as_str()).unwrap_or("*"),
            self.partition.as_ref().map(|r| r.as_str()).unwrap_or("*"),
            self.revision_id.as_ref().map(|r| r.as_str()).unwrap_or("*"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fundamental::NTP;

    #[test_log::test]
    fn test_filter() {
        let example1 = NTPR {
            ntp: NTP {
                namespace: "foo".to_string(),
                topic: "bar".to_string(),
                partition_id: 66,
            },
            revision_id: 123,
        };
        assert_eq!(true, NTPMask::match_all().compare(&example1));
        assert_eq!(
            true,
            NTPMask::from_str("*/*/*_*").unwrap().compare(&example1)
        );
        assert_eq!(
            true,
            NTPMask::from_str("foo/bar/66_123")
                .unwrap()
                .compare(&example1)
        );

        assert_eq!(
            false,
            NTPMask::from_str("boof/bar/66_123")
                .unwrap()
                .compare(&example1)
        );
    }
}
