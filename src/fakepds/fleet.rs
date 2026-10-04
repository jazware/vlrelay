//! Fleet identities: DIDs, signing keys, handles and DID documents, all
//! computed from the fleet seed and a (global host, account) index pair.
//!
//! The DID itself carries the pair, so any fakepds process (or the fake PLC
//! in any of them) can serve any account's document without knowing what
//! the other processes generated. That's what lets one `--plc-url` cover a
//! fleet of independent processes.

use serde_json::{Value as J, json};
use sha2::{Digest, Sha256};
use vlpds::crypto::Keypair;

#[derive(Clone, Debug)]
pub struct Layout {
    seed: String,
    tag: [u8; 4],
    /// Scheme and address the DID documents name, e.g. `http://127.0.0.1`.
    pub advertise: String,
    /// Global host `g` listens on `port_base + g`.
    pub port_base: u16,
}

impl Layout {
    pub fn new(seed: &str, advertise: &str, port_base: u16) -> Layout {
        let h = Sha256::digest(format!("fakepds tag\0{seed}"));
        Layout {
            seed: seed.to_string(),
            tag: [h[0], h[1], h[2], h[3]],
            advertise: advertise.trim_end_matches('/').to_string(),
            port_base,
        }
    }

    fn mac(&self, g: u32, i: u32) -> [u8; 3] {
        let h = Sha256::digest(format!("fakepds mac\0{}\0{g}\0{i}", self.seed));
        [h[0], h[1], h[2]]
    }

    /// 15 bytes (tag, host, account, mac) in base32: the 24 characters of a
    /// real `did:plc` identifier.
    pub fn did(&self, g: u32, i: u32) -> String {
        let mut b = [0u8; 15];
        b[..4].copy_from_slice(&self.tag);
        b[4..8].copy_from_slice(&g.to_be_bytes());
        b[8..12].copy_from_slice(&i.to_be_bytes());
        b[12..].copy_from_slice(&self.mac(g, i));
        format!("did:plc:{}", vlpds::cid::base32_encode(&b))
    }

    pub fn parse_did(&self, did: &str) -> Option<(u32, u32)> {
        let b = vlpds::cid::base32_decode(did.strip_prefix("did:plc:")?)?;
        if b.len() != 15 || b[..4] != self.tag {
            return None;
        }
        let g = u32::from_be_bytes(b[4..8].try_into().ok()?);
        let i = u32::from_be_bytes(b[8..12].try_into().ok()?);
        (b[12..] == self.mac(g, i)).then_some((g, i))
    }

    pub fn key(&self, g: u32, i: u32) -> Keypair {
        let mut n = 0u32;
        loop {
            let h = Sha256::digest(format!("fakepds key\0{}\0{g}\0{i}\0{n}", self.seed));
            if let Ok(k) = Keypair::from_bytes(&h) {
                return k;
            }
            n += 1;
        }
    }

    pub fn host_url(&self, g: u32) -> String {
        format!("{}:{}", self.advertise, self.port_base as u32 + g)
    }

    pub fn handle(&self, g: u32, i: u32) -> String {
        format!("u{i}.h{g}.fakepds.test")
    }

    pub fn doc(&self, g: u32, i: u32) -> J {
        let did = self.did(g, i);
        json!({
            "@context": [
                "https://www.w3.org/ns/did/v1",
                "https://w3id.org/security/multikey/v1",
                "https://w3id.org/security/suites/secp256k1-2019/v1"
            ],
            "id": did,
            "alsoKnownAs": [format!("at://{}", self.handle(g, i))],
            "verificationMethod": [{
                "id": format!("{did}#atproto"),
                "type": "Multikey",
                "controller": did,
                "publicKeyMultibase": self.key(g, i).public_multibase(),
            }],
            "service": [{
                "id": "#atproto_pds",
                "type": "AtprotoPersonalDataServer",
                "serviceEndpoint": self.host_url(g),
            }]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn did_roundtrip() {
        let l = Layout::new("s", "http://127.0.0.1", 30000);
        let d = l.did(7, 123_456);
        assert_eq!(d.len(), "did:plc:".len() + 24);
        assert_eq!(l.parse_did(&d), Some((7, 123_456)));
        assert_eq!(Layout::new("other", "http://x", 1).parse_did(&d), None);
        let doc = l.doc(7, 123_456);
        assert_eq!(vlpds::did_resolver::service_endpoint(&doc, "atproto_pds").unwrap(), "http://127.0.0.1:30007");
        assert!(l.key(7, 123_456).matches_public(&vlpds::did_resolver::signing_key_multibase(&doc).unwrap()));
    }
}
