use bytes::{BufMut, Bytes, BytesMut};
use ion_c_sys::*;
use ion_c_sys::{reader::IonCReaderHandle, result::IonCResult};
use sha2::{Digest, Sha256};
use std::{ffi::CStr, str::Utf8Error, string::String};
use writer::IonCWriterHandle;

/// Given a reader, return the pretty Ion text representation.
///
/// FIXME: This implementation currently uses a fixed size buffer
/// which is larger than the maximum QLDB document size, and thus
/// should never overflow. As Ion support in Rust gets better, we will
/// likely be able to remove this method entirely or at least use
/// "streams" to have a variably sized output buffer.
pub fn to_string_pretty(reader: IonCReaderHandle) -> IonCResult<String> {
    let mut buf = vec![0; 4000000];
    {
        let mut opts = ION_WRITER_OPTIONS {
            pretty_print: 1,
            ..Default::default()
        };
        let writer = IonCWriterHandle::new_buf(buf.as_mut(), &mut opts)?;
        ionc!(ion_c_sys::ion_writer_write_all_values(*writer, *reader))?;
    }

    Ok(buf_to_str(buf)?)
}

/// Takes a buffer that should contain a UTF8 string and converts it
/// into a Rust string. Panics if the buffer is not null terminated,
/// as this would represent utter chaos.
fn buf_to_str(buf: Vec<u8>) -> Result<String, Utf8Error> {
    // This should never be possible - writer is given a 4mb buffer
    // and should be writing a null terminated string. So if the last
    // byte is not NULL, something has gone horrifically wrong.
    if *buf.last().unwrap() != b'\0' {
        unreachable!()
    }

    unsafe {
        CStr::from_ptr(buf.as_ptr() as *const _)
            .to_str()
            .map(|s| s.trim_end().to_owned())
    }
}

const B: u8 = 0x0B;
const E: u8 = 0x0E;
const ESC: u8 = 0x0C;

/// Returns the Ion hash (using sha256 as the hasher) of the given
/// String.
///
/// At the time of writing, there was no ion-hash implementation. This
/// function aims only to support hashing of Strings with sha256 as
/// that lets us compute the QldbHash (required for the commit digest)
/// of user input in the shell.
///
/// https://amzn.github.io/ion-hash/docs/spec.html
pub fn ion_hash(string: &str) -> Bytes {
    const TQ: u8 = 0x80;
    let representation = string.as_bytes();
    let mut serialized_bytes = vec![B, TQ];
    serialized_bytes.extend_from_slice(&ion_hash_escape(&representation)[..]);
    serialized_bytes.push(E);
    let digest = Sha256::digest(&serialized_bytes);
    BytesMut::from(digest.as_slice()).freeze()
}

/// Replaces each marker byte M with ESC || M.
fn ion_hash_escape(representation: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(representation.len());
    for byte in representation {
        if let B | E | ESC = *byte {
            out.put_u8(0x0C);
        }
        out.put_u8(*byte);
    }

    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn nul_term() {
        let buf = vec![65, 0, 0];
        let it = buf_to_str(buf);
        assert_eq!("A".to_owned(), it.unwrap());
    }

    #[test]
    fn ion_hash_string() {
        let expected = hex!("82c4010bfc9cace7f645c0a951243b9b122cb5ba21b60b3f71ea79c513c39342");
        let computed = ion_hash("hello world");
        assert_eq!(expected, &computed[..]);
    }
}
