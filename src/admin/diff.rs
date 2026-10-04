//! Leaf-by-leaf differences between two JSON values, for the policy audit log.

use serde_json::Value;

/// `path: old → new` for every changed leaf (added and removed keys show `—`).
pub fn diff_json(a: &Value, b: &Value) -> Vec<String> {
    let mut out = Vec::new();
    walk("", a, b, &mut out);
    out
}

fn walk(path: &str, a: &Value, b: &Value, out: &mut Vec<String>) {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut keys: Vec<&String> = x.keys().chain(y.keys()).collect();
            keys.sort();
            keys.dedup();
            for k in keys {
                let p = if path.is_empty() { k.clone() } else { format!("{path}.{k}") };
                match (x.get(k), y.get(k)) {
                    (Some(av), Some(bv)) => walk(&p, av, bv, out),
                    (Some(av), None) => out.push(format!("{p}: {} → —", short(av))),
                    (None, Some(bv)) => out.push(format!("{p}: — → {}", short(bv))),
                    (None, None) => {}
                }
            }
        }
        _ if a != b => out.push(format!("{path}: {} → {}", short(a), short(b))),
        _ => {}
    }
}

fn short(v: &Value) -> String {
    let s = v.to_string();
    if s.chars().count() > 80 { format!("{}…", s.chars().take(80).collect::<String>()) } else { s }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn leaves() {
        let a = json!({"tiers": {"a": {"x": 1, "y": 2}}, "d": "a"});
        let b = json!({"tiers": {"a": {"x": 3, "y": 2}, "b": {"x": 1}}, "d": "a"});
        assert_eq!(diff_json(&a, &b), vec!["tiers.a.x: 1 → 3".to_string(), "tiers.b: — → {\"x\":1}".to_string()]);
        assert!(diff_json(&a, &a).is_empty());
    }
}
