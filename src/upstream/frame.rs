//! Just enough of a subscribeRepos frame to route it: the header and the
//! body's `seq`. Everything else is the verify workstream's.

use vlpds::cbor::ValueRef;

#[derive(Debug, PartialEq, Eq)]
pub enum Peek<'a> {
    /// `op: 1`. `seq` is absent on frame types that don't carry one.
    Message { t: &'a str, seq: Option<i64> },
    Info { name: &'a str, message: Option<&'a str> },
    Error { error: &'a str, message: Option<&'a str> },
}

#[derive(Debug, thiserror::Error)]
pub enum PeekError {
    #[error("bad frame header: {0}")]
    Header(String),
    #[error("bad frame body: {0}")]
    Body(String),
}

pub fn peek(frame: &[u8]) -> Result<Peek<'_>, PeekError> {
    let (head, n) = ValueRef::decode_prefix(frame).map_err(|e| PeekError::Header(e.to_string()))?;
    let op = match head.get("op") {
        Some(ValueRef::Int(op)) => *op,
        _ => return Err(PeekError::Header("missing op".into())),
    };
    // the body's own length isn't checked here: verify decodes it strictly
    let (body, _) = ValueRef::decode_prefix(&frame[n..]).map_err(|e| PeekError::Body(e.to_string()))?;
    let text = |k: &str| body.get(k).and_then(|v| v.as_str());
    match op {
        1 => {
            let t = head.get("t").and_then(|v| v.as_str()).ok_or_else(|| PeekError::Header("missing t".into()))?;
            if t == "#info" {
                let name = text("name").ok_or_else(|| PeekError::Body("#info without name".into()))?;
                return Ok(Peek::Info { name, message: text("message") });
            }
            let seq = match body.get("seq") {
                Some(ValueRef::Int(s)) => Some(*s),
                None => None,
                Some(_) => return Err(PeekError::Body("seq is not an integer".into())),
            };
            Ok(Peek::Message { t, seq })
        }
        -1 => {
            let error = text("error").ok_or_else(|| PeekError::Body("error frame without error".into()))?;
            Ok(Peek::Error { error, message: text("message") })
        }
        _ => Err(PeekError::Header(format!("unknown op {op}"))),
    }
}

/// A frame as a PDS would write it, for tests and the local fan.
pub fn encode_message(t: &str, body: &[(&str, vlpds::cbor::Value)]) -> Vec<u8> {
    use vlpds::cbor::{Value, write_int, write_map_head, write_text};
    let mut out = Vec::with_capacity(64);
    write_map_head(&mut out, 2);
    write_text(&mut out, "t");
    write_text(&mut out, t);
    write_text(&mut out, "op");
    write_int(&mut out, 1);
    let mut m: Vec<(String, Value)> = body.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
    m.sort_by(|a, b| vlpds::cbor::key_cmp(&a.0, &b.0));
    Value::Map(m).encode(&mut out);
    out
}

pub fn encode_error(error: &str, message: &str) -> Vec<u8> {
    vlpds::events::error_frame(error, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlpds::cbor::Value;

    #[test]
    fn peeks() {
        let f = encode_message(
            "#commit",
            &[("seq", Value::Int(42)), ("repo", Value::Text("did:plc:x".into())), ("blocks", Value::Bytes(vec![0; 9]))],
        );
        assert_eq!(peek(&f).unwrap(), Peek::Message { t: "#commit", seq: Some(42) });
        let f = encode_message("#info", &[("name", Value::Text("OutdatedCursor".into()))]);
        assert_eq!(peek(&f).unwrap(), Peek::Info { name: "OutdatedCursor", message: None });
        let f = encode_error("FutureCursor", "cursor in the future");
        assert_eq!(peek(&f).unwrap(), Peek::Error { error: "FutureCursor", message: Some("cursor in the future") });
        assert!(peek(b"\xa1").is_err());
        let f = encode_message("#commit", &[("seq", Value::Text("1".into()))]);
        assert!(peek(&f).is_err());
    }
}
