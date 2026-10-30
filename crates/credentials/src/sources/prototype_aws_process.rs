//! Throwaway support for an operator's AWS credential_process.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::{AwsSessionCredentials, CredentialError, Result};

const TIMEOUT: Duration = Duration::from_secs(30);
const OUTPUT_LIMIT: usize = 1024 * 1024;
const ENVIRONMENT: &[&str] = &[
    "HOME",
    "PATH",
    "USER",
    "LOGNAME",
    "LANG",
    "LC_ALL",
    "TMPDIR",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
    "XDG_DATA_HOME",
    "XDG_RUNTIME_DIR",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "KRB5CCNAME",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
];

pub(crate) struct CredentialProcess {
    profile: String,
    program: PathBuf,
    argv0: String,
    arguments: Zeroizing<Vec<String>>,
    environment: BTreeMap<OsString, OsString>,
    identity: (u64, u64, u64, i64, i64),
}

impl std::fmt::Debug for CredentialProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialProcess")
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}

impl CredentialProcess {
    pub(crate) fn capture(profile: &str, command: &str) -> Result<Self> {
        let mut words = split_command(command)?;
        if words.is_empty() || words[0].is_empty() {
            return Err(failure(profile, "the command is empty"));
        }
        let argv0 = words.remove(0);
        let path = PathBuf::from(&argv0);
        let program = if path.is_absolute() {
            path
        } else if path.components().count() == 1 {
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
                .map(|directory| directory.join(&path))
                .find(|candidate| {
                    candidate
                        .metadata()
                        .is_ok_and(|metadata| metadata.is_file() && metadata.mode() & 0o111 != 0)
                })
                .ok_or_else(|| failure(profile, "the executable is absent from PATH"))?
        } else {
            return Err(failure(profile, "use an absolute executable path"));
        };
        let program = program
            .canonicalize()
            .map_err(|_| failure(profile, "the executable cannot be resolved"))?;
        let identity = identity(&program)
            .map_err(|_| failure(profile, "the executable cannot be inspected"))?;
        let environment = ENVIRONMENT
            .iter()
            .filter_map(|name| std::env::var_os(name).map(|value| (OsString::from(name), value)))
            .collect();
        Ok(Self {
            profile: profile.to_string(),
            program,
            argv0,
            arguments: Zeroizing::new(words),
            environment,
            identity,
        })
    }

    pub(crate) fn resolve(&self) -> Result<AwsSessionCredentials> {
        if identity(&self.program).ok() != Some(self.identity) {
            return Err(failure(
                &self.profile,
                "the executable changed; restart Box",
            ));
        }
        let output = self.run()?;
        let mut response: ProcessCredentials = serde_json::from_slice(&output)
            .map_err(|_| failure(&self.profile, "the output is not credential JSON"))?;
        if response.version != 1 {
            return Err(failure(&self.profile, "the output must use Version 1"));
        }
        for value in [&response.access_key_id, &response.secret_access_key] {
            validate(value)
                .map_err(|_| failure(&self.profile, "a required credential field is invalid"))?;
        }
        if let Some(token) = &response.session_token {
            validate(token).map_err(|_| failure(&self.profile, "SessionToken is invalid"))?;
        }
        if let Some(expiration) = &response.expiration {
            let expiry = expiration_seconds(expiration).ok_or_else(|| {
                failure(&self.profile, "Expiration must be an RFC 3339 timestamp")
            })?;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| failure(&self.profile, "the clock is before the Unix epoch"))?
                .as_secs();
            if expiry <= now as i64 {
                return Err(failure(
                    &self.profile,
                    "the returned credentials have expired",
                ));
            }
        }
        Ok(AwsSessionCredentials {
            access_key_id: std::mem::take(&mut response.access_key_id),
            secret_access_key: std::mem::take(&mut response.secret_access_key),
            session_token: response.session_token.take(),
            region: None,
        })
    }

    fn run(&self) -> Result<Zeroizing<Vec<u8>>> {
        let (mut reader, writer) = UnixStream::pair()
            .map_err(|_| failure(&self.profile, "cannot open the output channel"))?;
        reader
            .set_read_timeout(Some(Duration::from_millis(50)))
            .map_err(|_| failure(&self.profile, "cannot set the output timeout"))?;
        let mut command = Command::new(&self.program);
        command
            .arg0(&self.argv0)
            .args(self.arguments.iter())
            .env_clear()
            .envs(&self.environment)
            .current_dir("/")
            .process_group(0)
            .stdin(Stdio::null())
            .stdout(Stdio::from(std::os::fd::OwnedFd::from(writer)))
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|_| failure(&self.profile, "cannot start the executable"))?;
        drop(command);
        let deadline = Instant::now() + TIMEOUT;
        let outcome = (|| {
            let mut output = Zeroizing::new(Vec::new());
            let mut chunk = Zeroizing::new([0_u8; 8192]);
            loop {
                if Instant::now() >= deadline {
                    return Err(failure(&self.profile, "the command exceeded 30 seconds"));
                }
                match reader.read(&mut *chunk) {
                    Ok(0) => break,
                    Ok(count) => {
                        if output.len() + count > OUTPUT_LIMIT {
                            return Err(failure(&self.profile, "the output exceeded 1 MiB"));
                        }
                        output.extend_from_slice(&chunk[..count]);
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock
                                | std::io::ErrorKind::TimedOut
                                | std::io::ErrorKind::Interrupted
                        ) => {}
                    Err(_) => return Err(failure(&self.profile, "cannot read the command output")),
                }
            }
            loop {
                match child.try_wait() {
                    Ok(Some(status)) if status.success() => return Ok(output),
                    Ok(Some(_)) => {
                        return Err(failure(
                            &self.profile,
                            "the command failed; run the profile's credential_process directly for details",
                        ));
                    }
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    _ => {
                        return Err(failure(
                            &self.profile,
                            "the command did not finish within 30 seconds",
                        ));
                    }
                }
            }
        })();
        if outcome.is_err() {
            stop(&mut child);
        }
        outcome
    }
}

fn stop(child: &mut Child) {
    let _ = Command::new("/bin/kill")
        .args(["-KILL", "--", &format!("-{}", child.id())])
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = child.kill();
    let _ = child.wait();
}

fn identity(path: &std::path::Path) -> std::io::Result<(u64, u64, u64, i64, i64)> {
    let metadata = path.metadata()?;
    if !metadata.is_file() || metadata.mode() & 0o111 == 0 {
        return Err(std::io::Error::other("not an executable file"));
    }
    Ok((
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
    ))
}

fn failure(profile: &str, reason: &str) -> CredentialError {
    CredentialError::Credential(format!(
        "AWS profile {profile:?} credential_process: {reason}"
    ))
}

fn validate(value: &str) -> std::result::Result<(), ()> {
    if value.trim().is_empty() || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f) {
        Err(())
    } else {
        Ok(())
    }
}

#[derive(Deserialize, Zeroize, ZeroizeOnDrop)]
#[serde(rename_all = "PascalCase")]
struct ProcessCredentials {
    version: u32,
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
    expiration: Option<String>,
}

fn split_command(text: &str) -> Result<Vec<String>> {
    let mut arguments = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    for character in text.chars() {
        if escaped {
            word.push(character);
            escaped = false;
        } else if character == '\\' && quote != Some('\'') {
            escaped = true;
            started = true;
        } else if quote == Some(character) {
            quote = None;
        } else if quote.is_none() && matches!(character, '\'' | '"') {
            quote = Some(character);
            started = true;
        } else if character.is_whitespace() && quote.is_none() {
            if started {
                arguments.push(std::mem::take(&mut word));
                started = false;
            }
        } else {
            word.push(character);
            started = true;
        }
    }
    if escaped || quote.is_some() {
        return Err(CredentialError::Credential(
            "credential_process contains an unfinished quote or escape".to_string(),
        ));
    }
    if started {
        arguments.push(word);
    }
    Ok(arguments)
}

fn expiration_seconds(text: &str) -> Option<i64> {
    if !text.is_ascii() || text.len() < 20 {
        return None;
    }
    let number = |range: std::ops::Range<usize>| text.get(range)?.parse::<i64>().ok();
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if &text[4..5] != "-"
        || &text[7..8] != "-"
        || &text[10..11] != "T"
        || &text[13..14] != ":"
        || &text[16..17] != ":"
        || !(1..=12).contains(&month)
        || !(0..24).contains(&hour)
        || !(0..60).contains(&minute)
        || !(0..60).contains(&second)
        || day < 1
        || day > days[(month - 1) as usize]
    {
        return None;
    }
    let mut suffix = &text[19..];
    if let Some(fraction) = suffix.strip_prefix('.') {
        let end = fraction.find(|character: char| !character.is_ascii_digit())?;
        if end == 0 {
            return None;
        }
        suffix = &fraction[end..];
    }
    let offset = if suffix == "Z" {
        0
    } else if suffix.len() == 6 && matches!(&suffix[..1], "+" | "-") && &suffix[3..4] == ":" {
        let hours: i64 = suffix[1..3].parse().ok()?;
        let minutes: i64 = suffix[4..6].parse().ok()?;
        if hours > 23 || minutes > 59 {
            return None;
        }
        (hours * 3600 + minutes * 60) * if suffix.starts_with('-') { -1 } else { 1 }
    } else {
        return None;
    };
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let within = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let ordinal = (153 * shifted_month + 2) / 5 + day - 1;
    let days = era * 146097 + within * 365 + within / 4 - within / 100 + ordinal - 719468;
    Some(days * 86400 + hour * 3600 + minute * 60 + second - offset)
}
