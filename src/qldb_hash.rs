use bytes::{Bytes, BytesMut};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;

const HASH_SIZE: usize = 32;

/// A [`QldbHash`] is either a 256 bit number or a special empty hash.
#[derive(PartialEq, Clone, Debug)]
pub struct QldbHash {
    bytes: Bytes,
}

impl QldbHash {
    pub fn from_bytes(bytes: Bytes) -> Option<QldbHash> {
        match bytes.len() {
            0 | HASH_SIZE => Some(QldbHash { bytes }),
            _ => None,
        }
    }

    pub fn dot(&self, other: &QldbHash) -> QldbHash {
        dot(self, other)
    }

    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl PartialOrd for QldbHash {
    /// Goes through the byte arrays from the last byte to the first. The
    /// first bytes that aren't equal determine ordering.
    ///
    /// Note: The byte arrays are always either 0 or `HASH_SIZE`, see
    /// the constructor.
    fn partial_cmp(&self, other: &QldbHash) -> Option<Ordering> {
        if self.is_empty() {
            if other.is_empty() {
                return Some(Ordering::Equal);
            } else {
                return Some(Ordering::Less);
            }
        }

        if other.is_empty() {
            return Some(Ordering::Greater);
        }

        for (x, y) in self.bytes.iter().rev().zip(other.bytes.iter().rev()) {
            // Note that Java compares bytes as signed integers. We
            // store the bytes as unsigned bytes everywhere and simply
            // re-interpret the bits at this point to determine
            // ordering.
            match (*x as i8).cmp(&(*y as i8)) {
                Ordering::Equal => {}
                cmp => return Some(cmp),
            }
        }

        return Some(Ordering::Equal);
    }
}

impl Default for QldbHash {
    fn default() -> QldbHash {
        QldbHash::from_bytes(Bytes::default()).unwrap()
    }
}

pub fn dot(x: &QldbHash, y: &QldbHash) -> QldbHash {
    if x.is_empty() {
        return y.clone();
    }
    if y.is_empty() {
        return x.clone();
    }

    let mut bytes = BytesMut::with_capacity(x.bytes.len() + y.bytes.len());

    if x < y {
        bytes.extend(&x.bytes);
        bytes.extend(&y.bytes);
    } else {
        bytes.extend(&y.bytes);
        bytes.extend(&x.bytes);
    }

    // NOTE: The produced bytes will always be of len() = HASH_SIZE,
    // because that's what Sha256 produces.
    let digest = sha256(&bytes.freeze());
    QldbHash::from_bytes(digest).unwrap()
}

fn sha256(bytes: &Bytes) -> Bytes {
    let digest = Sha256::digest(&bytes);
    BytesMut::from(digest.as_slice()).freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use hex_literal::hex;

    #[test]
    fn dot_examples() {
        // QldbHash wants 0 or 256 len byte arrays. That's tedious to type. This macro takes a literal string and uses sha256 to produce a 256-len value. In this way we can test
        // properties without verbose examples.
        macro_rules! s {
            ($value:expr) => {
                &QldbHash::from_bytes(sha256(&BytesMut::from($value.as_bytes()).freeze())).unwrap()
            };
        }

        // QldbHash commutes
        assert_eq!(dot(s!("1"), s!("2")), dot(s!("2"), s!("1")));

        // Empty hashes
        assert_eq!(
            dot(&QldbHash::default(), &QldbHash::default()),
            QldbHash::default()
        );
        assert_eq!(dot(s!("1"), &QldbHash::default()), *s!("1"));
        assert_eq!(dot(&QldbHash::default(), s!("1")), *s!("1"));

        // An actual example, values checked against the Java implementation
        assert_eq!(
            s!("1").bytes[..],
            hex!("6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b")[..]
        );
        assert_eq!(
            s!("2").bytes[..],
            hex!("d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35")[..]
        );
        assert_eq!(
            dot(s!("1"), s!("2")).bytes[..],
            hex!("940ed9abddfb5ef28004408546bc5043cda391232b6afe07267f9f8ed2b500c9")[..]
        );
    }
}
