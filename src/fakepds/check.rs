//! A sync 1.1 checker built only from vlpds's CBOR, CAR, MST and crypto
//! code, so fakepds's frames are checked by an implementation it didn't
//! write itself. Used by `selftest` and `consume --verify`.

use super::fleet::Layout;
use std::collections::HashMap;
use vlpds::car;
use vlpds::cbor::{Value, ValueRef};
use vlpds::cid::Cid;
use vlpds::mst::Tree;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Fail {
    Decode,
    Car,
    Commit,
    /// The DID document names another host than the one the frame came from.
    Foreign,
    Signature,
    RevOrder,
    /// `since` or `prevData` don't match the last commit seen.
    Chain,
    /// Inverting the ops on the CAR's partial tree doesn't give `prevData`.
    Inversion,
}

pub struct Checked {
    pub kind: &'static str,
    pub did: String,
    pub ops: usize,
}

#[derive(Default)]
pub struct Checker {
    /// DID -> (rev, data)
    heads: HashMap<String, (String, Cid)>,
    keys: HashMap<String, [u8; 33]>,
}

fn get<'a>(m: &'a ValueRef<'a>, k: &str) -> Option<&'a ValueRef<'a>> {
    m.get(k)
}

impl Checker {
    fn key(&mut self, layout: &Layout, did: &str) -> Result<[u8; 33], Fail> {
        if let Some(k) = self.keys.get(did) {
            return Ok(*k);
        }
        let (g, i) = layout.parse_did(did).ok_or(Fail::Foreign)?;
        let k = layout.key(g, i).public_key_sec1();
        self.keys.insert(did.to_string(), k);
        Ok(k)
    }

    /// Checks one frame as it came from host `g`. A chain failure still
    /// moves the account's head (as a relay that marks it desynchronized
    /// and carries on would); every other failure leaves it.
    pub fn check(&mut self, layout: &Layout, g: u32, frame: &[u8]) -> Result<Checked, (Fail, String)> {
        let d = |e: String| (Fail::Decode, e);
        let (hdr, n) = ValueRef::decode_prefix(frame).map_err(|e| d(e.to_string()))?;
        if !matches!(get(&hdr, "op"), Some(ValueRef::Int(1))) {
            return Err(d("op is not 1".into()));
        }
        let t = get(&hdr, "t").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let body = ValueRef::decode(&frame[n..]).map_err(|e| d(e.to_string()))?;
        if !matches!(get(&body, "seq"), Some(ValueRef::Int(_))) {
            return Err(d("no seq".into()));
        }
        let s = |k: &str| get(&body, k).and_then(|v| v.as_str()).map(str::to_string);
        if s("time").is_none() {
            return Err(d("no time".into()));
        }
        let kind: &'static str = match t.as_str() {
            "#commit" => "commit",
            "#sync" => "sync",
            "#identity" => "identity",
            "#account" => "account",
            "#info" => return Ok(Checked { kind: "info", did: String::new(), ops: 0 }),
            _ => return Err(d(format!("unknown type {t}"))),
        };
        let did = s("repo").or_else(|| s("did")).ok_or_else(|| d("no did".into()))?;
        let (dg, _) = layout.parse_did(&did).ok_or((Fail::Foreign, "not a fleet DID".into()))?;
        if kind == "identity" || kind == "account" {
            return Ok(Checked { kind, did, ops: 0 });
        }
        if dg != g {
            return Err((Fail::Foreign, format!("{did} belongs to host {dg}, came from {g}")));
        }
        let Some(ValueRef::Bytes(blocks)) = get(&body, "blocks") else {
            return Err(d("no blocks".into()));
        };
        let (roots, bl) = car::read_car(blocks).map_err(|e| (Fail::Car, e.to_string()))?;
        let root = *roots.first().ok_or((Fail::Car, "no root".into()))?;
        let mut map: HashMap<Cid, &[u8]> = HashMap::with_capacity(bl.len());
        for (c, b) in bl {
            if !car::block_matches(&c, b) {
                return Err((Fail::Car, format!("block {c} doesn't match its CID")));
            }
            map.insert(c, b);
        }
        let rev = s("rev").ok_or_else(|| d("no rev".into()))?;
        if kind == "commit" {
            match get(&body, "commit") {
                Some(ValueRef::Link(c)) if *c == root => {}
                _ => return Err((Fail::Car, "CAR root isn't the commit".into())),
            }
        }
        let cb = map.get(&root).ok_or((Fail::Car, "commit block missing".into()))?;
        let commit = Value::decode(cb).map_err(|e| (Fail::Commit, e.to_string()))?;
        let Value::Map(fields) = &commit else {
            return Err((Fail::Commit, "commit isn't a map".into()));
        };
        if commit.get("did").and_then(|v| v.as_str()) != Some(did.as_str())
            || commit.get("rev").and_then(|v| v.as_str()) != Some(rev.as_str())
            || !matches!(commit.get("version"), Some(Value::Int(3)))
        {
            return Err((Fail::Commit, "did, rev or version".into()));
        }
        let Some(Value::Link(data)) = commit.get("data") else {
            return Err((Fail::Commit, "no data".into()));
        };
        let data = *data;
        let Some(Value::Bytes(sig)) = commit.get("sig") else {
            return Err((Fail::Commit, "unsigned".into()));
        };
        let unsigned = Value::Map(fields.iter().filter(|(k, _)| k != "sig").cloned().collect()).to_cbor();
        let key = self.key(layout, &did).map_err(|f| (f, "key".into()))?;
        if !vlpds::crypto::verify_k256(&key, &unsigned, sig).unwrap_or(false) {
            return Err((Fail::Signature, did));
        }
        let prev = self.heads.get(&did).cloned();
        if let Some((prev_rev, _)) = &prev
            && rev <= *prev_rev
        {
            return Err((Fail::RevOrder, format!("{rev} after {prev_rev}")));
        }
        if kind == "sync" {
            self.heads.insert(did.clone(), (rev, data));
            return Ok(Checked { kind, did, ops: 0 });
        }

        let prev_data = match get(&body, "prevData") {
            Some(ValueRef::Link(c)) => Some(*c),
            _ => None,
        };
        let Some(ValueRef::Array(ops)) = get(&body, "ops") else {
            return Err(d("no ops".into()));
        };
        let mut tree = Tree::load_from_blocks(&map, data).map_err(|e| (Fail::Inversion, format!("load: {e:?}")))?;
        let mut parsed = Vec::with_capacity(ops.len());
        for op in ops {
            let action = get(op, "action").and_then(|v| v.as_str()).unwrap_or("");
            let path = get(op, "path").and_then(|v| v.as_str()).unwrap_or("");
            let cid = match get(op, "cid") {
                Some(ValueRef::Link(c)) => Some(*c),
                _ => None,
            };
            let oprev = match get(op, "prev") {
                Some(ValueRef::Link(c)) => Some(*c),
                _ => None,
            };
            parsed.push((action, path, cid, oprev));
        }
        // deletes first, then by path: any order must invert to the same root
        parsed.sort_by(|a, b| (a.0 != "delete", a.1).cmp(&(b.0 != "delete", b.1)));
        for (action, path, cid, oprev) in &parsed {
            let inv = |e| (Fail::Inversion, format!("{action} {path}: {e:?}"));
            let now = tree.get(path.as_bytes()).map_err(inv)?;
            match *action {
                "create" | "update" => {
                    let c = cid.ok_or((Fail::Inversion, format!("{action} without cid")))?;
                    if now != Some(c) || !map.contains_key(&c) {
                        return Err((Fail::Inversion, format!("{path}: record not in tree or CAR")));
                    }
                    match (action, oprev) {
                        (&"create", _) => {
                            tree.remove(path.as_bytes()).map_err(inv)?;
                        }
                        (_, Some(p)) => {
                            tree.insert(path.as_bytes(), *p).map_err(inv)?;
                        }
                        _ => return Err((Fail::Inversion, format!("update {path} without prev"))),
                    }
                }
                "delete" => {
                    let p = oprev.ok_or((Fail::Inversion, format!("delete {path} without prev")))?;
                    if now.is_some() {
                        return Err((Fail::Inversion, format!("{path} still in tree")));
                    }
                    tree.insert(path.as_bytes(), p).map_err(inv)?;
                }
                _ => return Err((Fail::Inversion, format!("action {action}"))),
            }
        }
        let inverted = tree.root_cid().map_err(|e| (Fail::Inversion, format!("{e:?}")))?;
        if Some(inverted) != prev_data {
            return Err((Fail::Inversion, format!("inverted to {inverted}, prevData {prev_data:?}")));
        }
        self.heads.insert(did.clone(), (rev.clone(), data));
        if let Some((prev_rev, prev_d)) = prev
            && (s("since").as_deref() != Some(prev_rev.as_str()) || prev_data != Some(prev_d))
        {
            return Err((Fail::Chain, format!("since {:?} / prevData don't follow {prev_rev}", s("since"))));
        }
        Ok(Checked { kind, did, ops: parsed.len() })
    }
}

/// What a full repo CAR holds once [`check_repo`] accepts it.
pub struct RepoCar {
    pub commit: Cid,
    pub rev: String,
    pub records: usize,
}

/// Checks a getRepo CAR for `did` as a relay bootstrapping from it would:
/// one root, every block hashes to its CID, the commit is the account's
/// and signed by its key, and the tree rebuilt from the records alone has
/// the commit's `data` as its root. Also the streamable order's first two
/// blocks: the commit, then the root node.
pub fn check_repo(layout: &Layout, did: &str, bytes: &[u8]) -> Result<RepoCar, String> {
    let (roots, blocks) = car::read_car(bytes).map_err(|e| format!("CAR: {e}"))?;
    let [root] = roots[..] else {
        return Err(format!("{} roots", roots.len()));
    };
    let mut map: HashMap<Cid, &[u8]> = HashMap::with_capacity(blocks.len());
    for (c, b) in &blocks {
        if !car::block_matches(c, b) {
            return Err(format!("block {c} doesn't match its CID"));
        }
        map.insert(*c, b);
    }
    if blocks.first().map(|b| b.0) != Some(root) {
        return Err("the commit isn't the first block".into());
    }
    let commit = Value::decode(map[&root]).map_err(|e| format!("commit: {e}"))?;
    let Value::Map(fields) = &commit else {
        return Err("commit isn't a map".into());
    };
    if commit.get("did").and_then(|v| v.as_str()) != Some(did) || !matches!(commit.get("version"), Some(Value::Int(3)))
    {
        return Err("commit did or version".into());
    }
    let rev = commit.get("rev").and_then(|v| v.as_str()).ok_or("no rev")?.to_string();
    let Some(Value::Link(data)) = commit.get("data") else {
        return Err("no data".into());
    };
    let Some(Value::Bytes(sig)) = commit.get("sig") else {
        return Err("unsigned".into());
    };
    let (g, i) = layout.parse_did(did).ok_or("not a fleet DID")?;
    let unsigned = Value::Map(fields.iter().filter(|(k, _)| k != "sig").cloned().collect()).to_cbor();
    if !vlpds::crypto::verify_k256(&layout.key(g, i).public_key_sec1(), &unsigned, sig).unwrap_or(false) {
        return Err("bad signature".into());
    }
    if blocks.get(1).map(|b| b.0) != Some(*data) {
        return Err("the root node isn't the second block".into());
    }
    let tree = Tree::load_from_blocks(&map, *data).map_err(|e| format!("load: {e:?}"))?;
    let mut leaves = Vec::new();
    tree.walk(&mut |k, c| leaves.push((k.to_vec(), c)));
    let mut rebuilt = Tree::new();
    for (k, c) in &leaves {
        if !map.contains_key(c) {
            return Err(format!("record {c} missing"));
        }
        rebuilt.insert_no_proof(k, *c).map_err(|e| format!("rebuild: {e:?}"))?;
    }
    let got = rebuilt.root_cid().map_err(|e| format!("rebuild: {e:?}"))?;
    if got != *data {
        return Err(format!("records rebuild to {got}, commit data is {data}"));
    }
    Ok(RepoCar { commit: root, rev, records: leaves.len() })
}
