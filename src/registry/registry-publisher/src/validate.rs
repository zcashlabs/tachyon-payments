//! The address checks of spec 9.1 and 9.3: `pk` canonical in the Pallas base
//! field, `ek` passing the ML-KEM-768 modulus check of FIPS 203 section 7.2,
//! and a nonzero `key16` (spec 6.2).
//!
//! These are the last three steps of the registration validation order; the
//! write API runs them after the body length, the challenge and the proof of
//! work (spec 9.3).
use tachyon_pir_protocol::{Address, KEM_EK_BYTES, PK_BYTES};

/// The Pallas base field modulus
/// `p = 0x40000000000000000000000000000000224698fc094cf91b992d30ed00000001`,
/// little-endian (spec 9.1).
pub const PALLAS_BASE_MODULUS_LE: [u8; PK_BYTES] = {
    let be: [u8; 32] = [
        0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x22, 0x46, 0x98, 0xfc, 0x09, 0x4c, 0xf9, 0x1b, 0x99, 0x2d, 0x30, 0xed, 0x00, 0x00,
        0x00, 0x01,
    ];
    let mut le = [0u8; 32];
    let mut i = 0;
    while i < 32 {
        le[i] = be[31 - i];
        i += 1;
    }
    le
};

/// The ML-KEM modulus `q`.
pub const KYBER_Q: u16 = 3329;
/// `ByteEncode12(t̂)`: 768 coefficients of 12 bits.
pub const EK_POLY_BYTES: usize = 1152;
/// Coefficients in `t̂` (k = 3 polynomials of 256).
pub const EK_COEFFICIENTS: usize = 768;

const _: () = assert!(EK_POLY_BYTES + 32 == KEM_EK_BYTES);
const _: () = assert!(EK_COEFFICIENTS * 12 == EK_POLY_BYTES * 8);

/// Which address check failed; its `Display` names the check, and is the
/// message of the `400` body (it never echoes client bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AddressRefusal {
    #[error("pk is not a canonical Pallas base field element")]
    NonCanonicalPk,
    #[error("ek fails the ML-KEM modulus check")]
    EkModulus {
        /// The first coefficient (0-based, in encoding order) that is at
        /// least `q`.
        coefficient: usize,
    },
    #[error("key16 is all zero")]
    ZeroKey,
}

/// `pk`, read as a 32-byte little-endian integer, is less than `p`.
pub fn pk_canonical(pk: &[u8; PK_BYTES]) -> bool {
    for i in (0..PK_BYTES).rev() {
        match pk[i].cmp(&PALLAS_BASE_MODULUS_LE[i]) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => {}
        }
    }
    // Equal to p.
    false
}

/// The 768 coefficients of `ByteDecode12(ek[0..1152])`: every 3 bytes hold
/// two coefficients, bits read little-endian (FIPS 203 algorithm 6 with
/// `d = 12`, before any reduction).
pub fn ek_coefficients(ek: &[u8; KEM_EK_BYTES]) -> impl Iterator<Item = u16> + '_ {
    ek[..EK_POLY_BYTES].chunks_exact(3).flat_map(|b| {
        let (b0, b1, b2) = (u16::from(b[0]), u16::from(b[1]), u16::from(b[2]));
        [b0 | ((b1 & 0x0f) << 8), (b1 >> 4) | (b2 << 4)]
    })
}

/// The FIPS 203 section 7.2 modulus check:
/// `ByteEncode12(ByteDecode12(ek[0..1152])) == ek[0..1152]`, that is every
/// coefficient is below `q`. `Err(i)` names the first coefficient that is not.
pub fn ek_modulus_check(ek: &[u8; KEM_EK_BYTES]) -> Result<(), usize> {
    match ek_coefficients(ek).position(|c| c >= KYBER_Q) {
        Some(index) => Err(index),
        None => Ok(()),
    }
}

/// The address checks of the validation order, in order: `pk` canonical,
/// `ek` modulus check, `key16` not all zero. `key16` is the digest prefix the
/// caller already computed.
pub fn check_address(address: &Address, key16: &[u8; 16]) -> Result<(), AddressRefusal> {
    if !pk_canonical(&address.pk) {
        return Err(AddressRefusal::NonCanonicalPk);
    }
    if let Err(coefficient) = ek_modulus_check(&address.ek) {
        return Err(AddressRefusal::EkModulus { coefficient });
    }
    if key16.iter().all(|b| *b == 0) {
        return Err(AddressRefusal::ZeroKey);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modulus_constant_matches_the_spec_hex() {
        let mut be = PALLAS_BASE_MODULUS_LE;
        be.reverse();
        assert_eq!(
            hex::encode(be),
            "40000000000000000000000000000000224698fc094cf91b992d30ed00000001"
        );
    }

    #[test]
    fn pk_boundaries() {
        let p = PALLAS_BASE_MODULUS_LE;
        assert!(!pk_canonical(&p));
        let mut below = p;
        below[0] -= 1; // p - 1: the low byte of p is 0x01.
        assert!(pk_canonical(&below));
        let mut above = p;
        above[0] += 1;
        assert!(!pk_canonical(&above));
        assert!(pk_canonical(&[0; 32]));
        assert!(!pk_canonical(&[0xff; 32]));
        // 2^254 - 1 and 2^254 are below p (p = 2^254 + 0x2246...0001);
        // 2^255 is not.
        let mut top = [0xff; 32];
        top[31] = 0x3f;
        assert!(pk_canonical(&top));
        let mut two_254 = [0; 32];
        two_254[31] = 0x40;
        assert!(pk_canonical(&two_254));
        let mut two_255 = [0; 32];
        two_255[31] = 0x80;
        assert!(!pk_canonical(&two_255));
    }

    #[test]
    fn ek_decoding_is_little_endian_twelve_bits() {
        let mut ek = [0u8; KEM_EK_BYTES];
        // Coefficients 0x123 and 0xabc pack as 23 c1 ab.
        ek[0] = 0x23;
        ek[1] = 0xc1;
        ek[2] = 0xab;
        let coefficients: Vec<u16> = ek_coefficients(&ek).take(3).collect();
        assert_eq!(coefficients, [0x123, 0xabc, 0]);
        assert_eq!(ek_coefficients(&ek).count(), EK_COEFFICIENTS);
        // 0xabc = 2748 < 3329.
        assert_eq!(ek_modulus_check(&ek), Ok(()));
        // The 32-byte seed is never checked.
        ek[EK_POLY_BYTES..].fill(0xff);
        assert_eq!(ek_modulus_check(&ek), Ok(()));
        // q - 1 passes, q fails, in either half of a 3-byte group.
        let mut ek = [0u8; KEM_EK_BYTES];
        let put = |ek: &mut [u8; KEM_EK_BYTES], index: usize, value: u16| {
            let group = index / 2 * 3;
            if index.is_multiple_of(2) {
                ek[group] = value as u8;
                ek[group + 1] = (ek[group + 1] & 0xf0) | (value >> 8) as u8;
            } else {
                ek[group + 1] = (ek[group + 1] & 0x0f) | ((value & 0x0f) << 4) as u8;
                ek[group + 2] = (value >> 4) as u8;
            }
        };
        put(&mut ek, 766, KYBER_Q - 1);
        put(&mut ek, 767, KYBER_Q - 1);
        assert_eq!(ek_modulus_check(&ek), Ok(()));
        put(&mut ek, 767, KYBER_Q);
        assert_eq!(ek_modulus_check(&ek), Err(767));
        put(&mut ek, 4, 0xfff);
        assert_eq!(ek_modulus_check(&ek), Err(4));
    }

    #[test]
    fn checks_run_in_the_spec_order() {
        let good = Address {
            pk: [1; 32],
            ek: [0; KEM_EK_BYTES],
        };
        assert_eq!(check_address(&good, &[1; 16]), Ok(()));
        // A zero key16 is refused; no address with an all-zero digest prefix
        // can be ground, so the check is exercised through this function.
        assert_eq!(check_address(&good, &[0; 16]), Err(AddressRefusal::ZeroKey));
        let mut bad_ek = good.clone();
        bad_ek.ek[1] = 0x0f;
        bad_ek.ek[0] = 0xff; // coefficient 0 = 0xfff.
        assert_eq!(
            check_address(&bad_ek, &[0; 16]),
            Err(AddressRefusal::EkModulus { coefficient: 0 })
        );
        let mut bad_pk = bad_ek.clone();
        bad_pk.pk = [0xff; 32];
        assert_eq!(
            check_address(&bad_pk, &[0; 16]),
            Err(AddressRefusal::NonCanonicalPk)
        );
        assert_eq!(
            AddressRefusal::EkModulus { coefficient: 9 }.to_string(),
            "ek fails the ML-KEM modulus check"
        );
    }
}
