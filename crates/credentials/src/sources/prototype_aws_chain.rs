//! The Rust SDK credential chain behind the synchronous signing boundary.

use std::sync::mpsc::{self, SyncSender};
use std::time::{Duration, SystemTime};

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::provider_config::ProviderConfig;
use aws_credential_types::provider::ProvideCredentials;
use aws_smithy_http_client::{Builder, tls};
use zeroize::Zeroizing;

use crate::{AwsSessionCredentials, CredentialError, Result};

const LOAD_TIMEOUT: Duration = Duration::from_secs(30);
const REPLY_TIMEOUT: Duration = Duration::from_secs(35);

type Reply = SyncSender<Result<Zeroizing<AwsSessionCredentials>>>;

pub(super) struct CredentialChain {
    requests: SyncSender<Reply>,
}

impl CredentialChain {
    pub(super) fn open() -> Result<Self> {
        let (requests, receiver) = mpsc::sync_channel::<Reply>(1);
        let (ready, initialized) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("box-aws-chain".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(_) => {
                        let _ = ready.send(Err(failure("runtime initialization failed")));
                        return;
                    }
                };
                let chain = runtime.block_on(async {
                    tokio::time::timeout(LOAD_TIMEOUT, async {
                        let http = Builder::new()
                            .tls_provider(tls::Provider::Rustls(
                                tls::rustls_provider::CryptoMode::Ring,
                            ))
                            .build_https();
                        let chain = DefaultCredentialsChain::builder()
                            .configure(ProviderConfig::default().with_http_client(http))
                            .build()
                            .await;
                        resolve(&chain).await?;
                        Ok(chain)
                    })
                    .await
                    .map_err(|_| failure("initialization timed out"))?
                });
                let chain = match chain {
                    Ok(chain) => chain,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                };
                if ready.send(Ok(())).is_err() {
                    return;
                }
                while let Ok(reply) = receiver.recv() {
                    let result = runtime.block_on(async {
                        tokio::time::timeout(LOAD_TIMEOUT, resolve(&chain))
                            .await
                            .map_err(|_| failure("credential resolution timed out"))?
                    });
                    let _ = reply.send(result);
                }
            })
            .map_err(|_| failure("worker could not start"))?;
        initialized
            .recv_timeout(REPLY_TIMEOUT)
            .map_err(|_| failure("worker initialization did not complete"))??;
        Ok(Self { requests })
    }

    pub(super) fn resolve(&self) -> Result<Zeroizing<AwsSessionCredentials>> {
        let (reply, receiver) = mpsc::sync_channel(1);
        self.requests
            .try_send(reply)
            .map_err(|_| failure("worker is busy or unavailable"))?;
        receiver
            .recv_timeout(REPLY_TIMEOUT)
            .map_err(|_| failure("worker did not return credentials"))?
    }
}

async fn resolve(chain: &DefaultCredentialsChain) -> Result<Zeroizing<AwsSessionCredentials>> {
    let credentials = chain
        .provide_credentials()
        .await
        .map_err(|_| failure("no usable credentials; check the trusted AWS configuration"))?;
    if credentials
        .expiry()
        .is_some_and(|expiry| expiry <= SystemTime::now())
    {
        return Err(failure("provider returned expired credentials"));
    }
    for value in [
        Some(credentials.access_key_id()),
        Some(credentials.secret_access_key()),
        credentials.session_token(),
    ]
    .into_iter()
    .flatten()
    {
        if value.trim().is_empty() || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f) {
            return Err(failure("provider returned an invalid credential field"));
        }
    }
    Ok(Zeroizing::new(AwsSessionCredentials {
        access_key_id: credentials.access_key_id().to_string(),
        secret_access_key: credentials.secret_access_key().to_string(),
        session_token: credentials.session_token().map(str::to_string),
        region: None,
    }))
}

fn failure(message: &str) -> CredentialError {
    CredentialError::Credential(format!("AWS SDK credential chain: {message}"))
}
