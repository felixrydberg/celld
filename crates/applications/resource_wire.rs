//! Typed native binding operations. A binding alias carries no bucket path,
//! tenancy, image reference, credentials or native process authority.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCall {
    pub binding: crate::Name,
    pub operation: ResourceOperation,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceOperation {
    R2 { operation: R2Operation },
    Assets {},
    Container { operation: ContainerOperation },
}

/// The original `ctx.container` effects. The host resolves the container from
/// the issuing object; none of these names an image, address or native object.
/// `run` is the start the guest opened through the synchronous storage op, and
/// process ids are checked against the issuing container's own processes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContainerOperation {
    Start {
        run: u64,
        entrypoint: Option<Vec<String>>,
        env: Vec<(String, String)>,
        enable_internet: bool,
        labels: Vec<(String, String)>,
    },
    Monitor { run: u64 },
    Destroy {},
    Signal { signal: u32 },
    Inactivity { millis: u64 },
    Exec {
        cmd: Vec<String>,
        env: Vec<(String, String)>,
        cwd: Option<String>,
        user: Option<String>,
        combined: bool,
    },
    ExecRead { id: u64, which: u8 },
    ExecWrite { id: u64 },
    ExecClose { id: u64 },
    ExecWait { id: u64 },
    ExecKill { id: u64, signal: u32 },
    ExecDrop { id: u64 },
    /// `getTcpPort(port).fetch`: the request is the call's own HTTP request.
    /// The host dials the container's address for `port`, never the request's.
    PortFetch { port: u16 },
    /// `getTcpPort(port).connect`; sockets belong to the issuing event.
    PortConnect { port: u16 },
    TcpRead { id: u64 },
    TcpWrite { id: u64 },
    TcpShutdown { id: u64 },
    TcpClose { id: u64 },
    TcpStartTls { id: u64, host: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum R2Operation {
    Head { key: String },
    Get { key: String, options: String },
    Put { key: String, options: String },
    Delete { keys: Vec<String> },
    List { options: String },
    PutBegin { key: String, options: String },
    PutChunk { id: u64 },
    PutEnd { id: u64, abort: bool },
    MultipartBegin { key: String, options: String },
    MultipartResume { key: String, id: String },
    MultipartPart { id: u64, part: u32 },
    MultipartComplete { id: u64, parts: Vec<u32> },
    MultipartAbort { id: u64 },
}

impl ResourceCall {
    pub fn validate(&self) -> Result<()> {
        crate::Name::try_from(self.binding.as_str().to_owned())?;
        match &self.operation {
            ResourceOperation::R2 { operation } => operation.validate()?,
            ResourceOperation::Container { operation } => operation.validate()?,
            ResourceOperation::Assets {} => {}
        }
        Ok(())
    }
}

impl ContainerOperation {
    fn validate(&self) -> Result<()> {
        fn argv(list: &[String]) -> Result<()> {
            ensure!(
                list.len() <= 1024 && list.iter().map(|arg| arg.len()).sum::<usize>() <= 1 << 20,
                "container command exceeds limit"
            );
            Ok(())
        }
        fn pairs(list: &[(String, String)], what: &str) -> Result<()> {
            ensure!(
                list.len() <= 1024
                    && list
                        .iter()
                        .all(|(name, _)| !name.is_empty() && name.len() <= 1024)
                    && list
                        .iter()
                        .map(|(name, value)| name.len() + value.len())
                        .sum::<usize>()
                        <= 1 << 20,
                "container {what} exceed limit"
            );
            Ok(())
        }
        let signal = |signal: u32| -> Result<()> {
            ensure!((1..=64).contains(&signal), "invalid container signal");
            Ok(())
        };
        match self {
            Self::Start {
                run,
                entrypoint,
                env,
                labels,
                ..
            } => {
                ensure!(*run > 0, "container start has no run");
                argv(entrypoint.as_deref().unwrap_or_default())?;
                pairs(env, "environment")?;
                pairs(labels, "labels")?;
            }
            Self::Monitor { run } => ensure!(*run > 0, "container monitor has no run"),
            Self::PortFetch { port } | Self::PortConnect { port } => {
                ensure!(*port > 0, "invalid container port")
            }
            Self::TcpStartTls { host, .. } => ensure!(
                !host.is_empty() && host.len() <= 255,
                "invalid TLS server name"
            ),
            Self::Signal { signal: number } | Self::ExecKill { signal: number, .. } => {
                signal(*number)?
            }
            Self::Exec {
                cmd,
                env,
                cwd,
                user,
                ..
            } => {
                ensure!(!cmd.is_empty(), "container exec has no command");
                argv(cmd)?;
                pairs(env, "environment")?;
                ensure!(
                    cwd.iter().chain(user.iter()).all(|text| text.len() <= 4096),
                    "container exec option exceeds limit"
                );
            }
            Self::ExecRead { which, .. } => {
                ensure!(matches!(which, 1 | 2), "invalid container stream")
            }
            Self::Destroy {}
            | Self::Inactivity { .. }
            | Self::ExecWrite { .. }
            | Self::ExecClose { .. }
            | Self::ExecWait { .. }
            | Self::ExecDrop { .. }
            | Self::TcpRead { .. }
            | Self::TcpWrite { .. }
            | Self::TcpShutdown { .. }
            | Self::TcpClose { .. } => {}
        }
        Ok(())
    }
}

impl R2Operation {
    fn validate(&self) -> Result<()> {
        let key = |key: &str| -> Result<()> {
            ensure!(key.len() <= 1024, "R2 key exceeds limit");
            Ok(())
        };
        let options = |options: &str| -> Result<()> {
            ensure!(options.len() <= 1024 * 1024, "R2 options exceed limit");
            Ok(())
        };
        match self {
            Self::Head { key: value } | Self::MultipartResume { key: value, .. } => key(value)?,
            Self::Get {
                key: value,
                options: raw,
            }
            | Self::Put {
                key: value,
                options: raw,
            }
            | Self::PutBegin {
                key: value,
                options: raw,
            }
            | Self::MultipartBegin {
                key: value,
                options: raw,
            } => {
                key(value)?;
                options(raw)?;
            }
            Self::Delete { keys } => {
                for value in keys {
                    key(value)?;
                }
            }
            Self::List { options: raw } => options(raw)?,
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "unit_tests/resource_wire/tests.rs"]
mod tests;
