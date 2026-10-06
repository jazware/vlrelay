//! The effective process config for the console's Settings page: every flag
//! with its value and where the value came from, secrets reduced to set or
//! unset.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigEntry {
    /// `--listen`.
    pub flag: String,
    pub env: Option<String>,
    /// None for a secret, or a flag with no value.
    pub value: Option<String>,
    /// `flag`, `env`, `default` or `unset`.
    pub source: String,
    pub default: Option<String>,
    pub secret: bool,
    /// For a secret: whether it has a value.
    pub set: bool,
    /// The flag's help, first paragraph.
    pub help: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsView {
    pub binary: String,
    pub version: String,
    pub entries: Vec<ConfigEntry>,
}

/// Names that carry credentials whatever clap is told about them.
fn secret_name(id: &str) -> bool {
    ["token", "secret", "access_key", "password"].iter().any(|s| id.contains(s))
}

/// Every argument of `cmd` as `m` parsed it.
pub fn from_clap(cmd: &clap::Command, m: &clap::ArgMatches) -> SettingsView {
    use clap::parser::ValueSource;
    let mut entries = Vec::new();
    for a in cmd.get_arguments() {
        let id = a.get_id().as_str();
        if matches!(id, "help" | "version") {
            continue;
        }
        let flag = a.get_long().map(|l| format!("--{l}")).unwrap_or_else(|| id.to_string());
        let raw: Option<Vec<String>> = m.get_raw(id).map(|vs| vs.map(|v| v.to_string_lossy().into_owned()).collect());
        let source = match m.value_source(id) {
            Some(ValueSource::CommandLine) => "flag",
            Some(ValueSource::EnvVariable) => "env",
            Some(ValueSource::DefaultValue) => "default",
            _ => "unset",
        };
        let secret = a.is_hide_env_values_set() || secret_name(id);
        let value = raw.as_ref().map(|v| v.join(",")).filter(|v| !v.is_empty());
        let defaults: Vec<String> = a.get_default_values().iter().map(|v| v.to_string_lossy().into_owned()).collect();
        let help = a
            .get_long_help()
            .or_else(|| a.get_help())
            .map(|h| h.to_string().split("\n\n").next().unwrap_or_default().replace('\n', " "))
            .unwrap_or_default();
        entries.push(ConfigEntry {
            flag,
            env: a.get_env().map(|e| e.to_string_lossy().into_owned()),
            set: value.is_some(),
            value: if secret { None } else { value },
            source: source.into(),
            default: (!defaults.is_empty() && !secret).then(|| defaults.join(",")),
            secret,
            help,
        });
    }
    SettingsView { binary: cmd.get_name().to_string(), version: env!("CARGO_PKG_VERSION").into(), entries }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Arg, ArgAction, Command};

    #[test]
    fn secrets_are_redacted_and_sources_reported() {
        let cmd = Command::new("t")
            .arg(Arg::new("listen").long("listen").default_value("127.0.0.1:1"))
            .arg(Arg::new("admin_token").long("admin-token"))
            .arg(Arg::new("s3_secret_key").long("s3-secret-key").hide_env_values(true))
            .arg(Arg::new("memory").long("memory").action(ArgAction::SetTrue));
        let m = cmd.clone().get_matches_from(["t", "--admin-token", "hunter2", "--memory"]);
        let v = from_clap(&cmd, &m);
        let get = |f: &str| v.entries.iter().find(|e| e.flag == f).unwrap();
        assert_eq!(get("--listen").source, "default");
        assert_eq!(get("--listen").value.as_deref(), Some("127.0.0.1:1"));
        let tok = get("--admin-token");
        assert!(tok.secret && tok.set && tok.value.is_none() && tok.source == "flag");
        let s3 = get("--s3-secret-key");
        assert!(s3.secret && !s3.set && s3.source == "unset");
        assert_eq!(get("--memory").value.as_deref(), Some("true"));
        assert!(!serde_json::to_string(&v).unwrap().contains("hunter2"));
    }
}
