# AWS credential prototype

This throwaway prototype adds the Rust SDK credential chain and AWS `credential_process` profiles to Box. It is for evaluation, not release. Neither path uses `credsd`.

## Build and run

From the repository root:

```sh
cargo build -p strands-box --all-features --bins --locked
cargo build -p strands-box-containment --all-features \
  --bin strands-box-contain-trampoline --locked
```

To use the standard Rust SDK credential chain:

```toml
[egress.aws]
destinations = ["sts.us-east-1.amazonaws.com"]
secret.ref = "aws-chain://"
```

The reference has no selector. Box uses the standard `aws-config` chain with SSO and credential-process support. Its HTTP client uses Ring, which matches Box's existing TLS provider. The chain checks environment credentials, shared AWS profiles, web identity, container credentials, and EC2 instance metadata, in that order. `AWS_PROFILE` selects the shared profile; exported credentials take precedence. Profile providers include SSO, AssumeRole, and `credential_process`. The SDK's optional `credentials-login` feature is not enabled.

Discovery uses the trusted Box process's environment and AWS configuration. The workload receives example credentials. Box signs permitted requests with the real credentials. `aws-env://default` is no longer accepted; environment credentials now work through the chain.

Run the smoke check with the host's current AWS configuration:

```sh
python3 crates/box/prototype-aws/run.py chain
```

Use `chain --profile bedrock` to set `AWS_PROFILE` for the check. Use `chain --seed-profile bedrock` to fetch credentials through that profile's helper in the parent process and pass them to Box as environment variables. This checks the environment provider without writing keys to a file.

To use the existing explicit profile path:

```toml
[egress.aws]
destinations = ["sts.us-east-1.amazonaws.com"]
secret.ref = "aws://bedrock"
```

The profile remains in the host's AWS configuration. Run:

```sh
python3 crates/box/prototype-aws/run.py profile --profile bedrock
```

Each smoke run makes two STS `GetCallerIdentity` calls from one contained workload. Success requires HTTP 200 for both. The runner creates temporary Box state under `/var/tmp` and removes it afterward. The [verification record](./verification.json) identifies the checked source, binaries, and results. The chain passed with exported credentials, an ADA-backed profile, and a loopback container-credential fixture. The explicit profile path also passed. All four checks used real STS requests and an unusable `CREDSD_SOCKET`.

## Implementation and limits

`prototype_aws_chain.rs` retains one SDK chain on a dedicated worker for each vault that declares it. This bridges the asynchronous SDK to the synchronous signer. Box resolves credentials once at startup, then again for each signed request. Initialization and each resolution have a 30-second deadline. The caller waits at most 35 seconds for a reply. One request can wait behind an active request; further concurrent requests fail closed. The worker exits when its request channel closes. Invalid fields and expired credentials are refused. Raw provider errors are withheld from Box's error messages.

Box adds no outer credential cache. It retains the SDK providers and their internal state, but does not use the SDK service client's identity cache. Providers can obtain fresh credentials on each request. Environment credentials cannot renew themselves, and changing a parent shell does not change an existing Box process.

The SDK chain can access metadata endpoints and run a profile's helper as trusted host code. On Unix, the SDK's helper provider uses `sh -c` and inherits the host environment. It does not use the explicit profile runner's environment filter, output limit, or process-group cleanup. A Box timeout stops waiting for that SDK operation; it does not guarantee termination of a helper or its descendants. SDK internals also do not share all of Box's buffer-wiping rules. These are prototype limits that need review before release.

The separate `aws://PROFILE` path still uses `prototype_aws_process.rs`. Box captures the helper command, executable path, arguments, and allowed environment before starting the workload. It runs the captured helper for every signed request without an implicit shell. The runner has a 30-second deadline and a 1 MiB output limit, discards stderr, and stops the helper process group on failure. Its environment filter excludes AWS keys and unrelated tokens. A profile cannot combine this helper with static keys or role settings.

The explicit runner checks executable metadata, but leaves a race before execution. Scripts, libraries, and helper configuration remain mutable. Its expiration parser also accepts some invalid signed offset components. Both helpers are trusted host code; neither is contained.

Existing endpoint and signing limits still apply. Live checks use only `sts.us-east-1.amazonaws.com` on Linux. They check HTTP status, not the returned identity. Deployment on ECS, EKS, or EC2, SSO, and renewal after actual credential expiry need separate validation.
