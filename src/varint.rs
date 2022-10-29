use log::trace;

pub struct VarIntDecoder {
    shift: u8,
    result: i64,
}

impl VarIntDecoder {
    pub fn new() -> Self {
        Self {
            shift: 0,
            result: 0,
        }
    }

    /// Return true when caller should stop feeding us bytes
    pub fn feed(&mut self, b: u8) -> bool {
        let stop = if b & 128 > 0 {
            self.result |= ((b & 0x7fu8) as i64) << self.shift as i64;
            false
        } else {
            self.result |= (b as i64) << (self.shift as i64);
            true
        };
        trace!("varint byte {:x} result {}", b, self.result);
        self.shift += 7;

        if (self.shift as usize) >= 8 * 7 + 1 {
            // We've taken the max bytes that can make up
            // the type we wanted: stop feeding
            true
        } else {
            stop
        }
    }

    pub fn result(&self) -> i64 {
        let r = (self.result >> 1) ^ (!(self.result & 1) + 1);
        trace!("final result {}", r);
        r
    }
}

#[cfg(test)]
mod tests {
    use crate::varint::VarIntDecoder;

    fn assert_decode<const N: usize>(bytes: [u8; N], expect: i64) {
        let mut d = VarIntDecoder::new();
        for b in bytes {
            d.feed(b);
        }
        assert_eq!(d.result(), expect);
    }

    #[test_log::test]
    pub fn decode_simple() {
        assert_decode([0x01u8], -1);
        assert_decode([0x32u8], 25);
        assert_decode([0xcc, 0x14], 1318);
        assert_decode([0xee, 0xd], 887);
    }
}
