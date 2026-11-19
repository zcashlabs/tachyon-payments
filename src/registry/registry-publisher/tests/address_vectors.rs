//! Known answers for the spec 9.1 address checks, computed independently by
//! `tests/vectors/address_checks.py` (integer comparison with `p`; FIPS 203
//! `ByteDecode12` bit by bit and the section 7.2 re-encoding check) and
//! checked in as `address_checks.json`.
use registry_publisher::validate::{AddressRefusal, check_address, ek_modulus_check, pk_canonical};
use serde_json::Value;
use tachyon_pir_protocol::Address;

fn vectors() -> Value {
    serde_json::from_str(include_str!("vectors/address_checks.json")).unwrap()
}

fn bytes<const N: usize>(value: &Value) -> [u8; N] {
    hex::decode(value.as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap()
}

#[test]
fn pk_canonical_matches_the_independent_vectors() {
    let vectors = vectors();
    let cases = vectors["pk"].as_array().unwrap();
    assert_eq!(cases.len(), 10);
    for case in cases {
        let pk: [u8; 32] = bytes(&case["pk"]);
        assert_eq!(
            pk_canonical(&pk),
            case["canonical"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
    }
    // Both outcomes are represented.
    assert!(cases.iter().any(|c| c["canonical"] == true));
    assert!(cases.iter().any(|c| c["canonical"] == false));
}

#[test]
fn ek_modulus_check_matches_the_independent_vectors() {
    let vectors = vectors();
    let cases = vectors["ek"].as_array().unwrap();
    assert_eq!(cases.len(), 9);
    for case in cases {
        let ek: [u8; 1184] = bytes(&case["ek"]);
        let expected = match case["first_bad"].as_u64() {
            None => Ok(()),
            Some(index) => Err(index as usize),
        };
        assert_eq!(case["passes"].as_bool().unwrap(), expected.is_ok());
        assert_eq!(ek_modulus_check(&ek), expected, "{}", case["name"]);
        // Through the validation order with a canonical pk.
        let address = Address { pk: [1; 32], ek };
        let result = check_address(&address, &address.key16());
        match expected {
            Ok(()) => assert_eq!(result, Ok(())),
            Err(coefficient) => {
                assert_eq!(result, Err(AddressRefusal::EkModulus { coefficient }))
            }
        }
    }
}
