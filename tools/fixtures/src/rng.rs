//! A deterministic byte stream: SHA-256 in counter mode over the chain seed.
//!
//! Written here rather than taken from a crate so that a given seed produces the
//! same chain whatever crate versions the workspace resolves.
use crate::hash::sha256;

/// A deterministic pseudo-random byte stream.
#[derive(Clone, Debug)]
pub struct Rng {
    key: [u8; 32],
    counter: u64,
    buffer: [u8; 32],
    used: usize,
}

impl Rng {
    /// The stream for `seed` under the domain `domain`.
    pub fn new(domain: &str, seed: u64) -> Self {
        Self {
            key: sha256(&[
                b"tachyon-fixtures/rng/v1/",
                domain.as_bytes(),
                &seed.to_le_bytes(),
            ]),
            counter: 0,
            buffer: [0; 32],
            used: 32,
        }
    }

    /// Fills `out` with the next bytes of the stream.
    pub fn fill(&mut self, out: &mut [u8]) {
        for byte in out {
            if self.used == 32 {
                self.buffer = sha256(&[&self.key, &self.counter.to_le_bytes()]);
                self.counter += 1;
                self.used = 0;
            }
            *byte = self.buffer[self.used];
            self.used += 1;
        }
    }

    /// The next `N` bytes.
    pub fn array<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0; N];
        self.fill(&mut out);
        out
    }

    /// The next `len` bytes.
    pub fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = vec![0; len];
        self.fill(&mut out);
        out
    }

    /// The next 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        u64::from_le_bytes(self.array())
    }

    /// A value in `low..=high` (`low` if the range is empty). The modulo bias is
    /// irrelevant for fixtures.
    pub fn range(&mut self, low: u64, high: u64) -> u64 {
        if high <= low {
            return low;
        }
        let span = high - low;
        match span.checked_add(1) {
            Some(width) => low + self.next_u64() % width,
            None => self.next_u64(),
        }
    }

    /// True with probability `percent`/100.
    pub fn percent(&mut self, percent: u8) -> bool {
        self.range(0, 99) < u64::from(percent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_are_deterministic_and_seeded() {
        let mut a = Rng::new("x", 1);
        let mut b = Rng::new("x", 1);
        let mut c = Rng::new("x", 2);
        let (sa, sb, sc) = (a.bytes(100), b.bytes(100), c.bytes(100));
        assert_eq!(sa, sb);
        assert_ne!(sa, sc);
        for _ in 0..1000 {
            let value = a.range(3, 7);
            assert!((3..=7).contains(&value));
        }
        assert_eq!(a.range(5, 5), 5);
        assert_eq!(a.range(9, 5), 9);
    }
}
