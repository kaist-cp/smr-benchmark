use core::mem;
use murmur3::murmur3_x64_128;

/// Size of a bloom filter in bytes.  128 bytes = 2 cachelines.
const BLOOM_FILTER_SIZE: usize = 128;
const_assert!(BLOOM_FILTER_SIZE % mem::size_of::<usize>() == 0);

/// Bloom filter for `usize`.
#[derive(Default, Debug)]
pub struct BloomFilter {
    inner: [usize; BLOOM_FILTER_SIZE / mem::size_of::<usize>()],
}

impl BloomFilter {
    pub fn new() -> Self {
        Self {
            inner: [0; BLOOM_FILTER_SIZE / mem::size_of::<usize>()],
        }
    }

    pub fn query(&self, data: usize) -> bool {
        let mut buf = [0u8; 16];
        murmur3_x64_128(data, 0, &mut buf);
        buf.chunks_exact(2).all(|l| unsafe {
            let b0 = *l.get_unchecked(0) as usize;
            let b1 = *l.get_unchecked(1) as usize;
            let v = (b0.wrapping_shl(8).wrapping_add(b1)) & (1usize.wrapping_shl(10) - 1);
            let v0 = v / (8 * mem::size_of::<usize>());
            let v1 = v % (8 * mem::size_of::<usize>());
            *self.inner.get_unchecked(v0) & (1usize.wrapping_shl(v1 as _)) != 0
        })
    }

    pub fn insert(&mut self, data: usize) {
        let mut buf = [0u8; 16];
        murmur3_x64_128(data, 0, &mut buf);
        for l in buf.chunks_exact(2) {
            unsafe {
                let b0 = *l.get_unchecked(0) as usize;
                let b1 = *l.get_unchecked(1) as usize;
                let v = (b0.wrapping_shl(8).wrapping_add(b1)) & (1usize.wrapping_shl(10) - 1);
                let v0 = v / (8 * mem::size_of::<usize>());
                let v1 = v % (8 * mem::size_of::<usize>());
                *self.inner.get_unchecked_mut(v0) |= 1usize.wrapping_shl(v1 as _);
            }
        }
    }

    pub fn union(&mut self, other: &Self) {
        for (s, o) in self.inner.iter_mut().zip(other.inner.iter()) {
            *s |= *o;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let mut filter = BloomFilter::new();

        filter.insert(1);
        filter.insert(2);
        filter.insert(3);

        assert!(filter.query(1));
        assert!(filter.query(2));
        assert!(filter.query(3));

        assert!(!filter.query(4));
        assert!(!filter.query(5));
        assert!(!filter.query(6));
    }

    #[test]
    fn union() {
        let mut lhs = BloomFilter::new();
        let mut rhs = BloomFilter::new();

        lhs.insert(1);
        lhs.insert(2);
        lhs.insert(3);

        rhs.insert(3);
        rhs.insert(4);
        rhs.insert(5);

        lhs.union(&rhs);

        assert!(lhs.query(1));
        assert!(lhs.query(2));
        assert!(lhs.query(3));
        assert!(lhs.query(4));
        assert!(lhs.query(5));

        assert!(!lhs.query(6));
        assert!(!lhs.query(7));
        assert!(!lhs.query(8));
        assert!(!lhs.query(9));
        assert!(!lhs.query(10));
    }
}
