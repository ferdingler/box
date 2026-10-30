# AWS credential prototype

This throwaway prototype adds environment credentials and AWS `credential_process` profiles to Box. Both paths signed real STS `GetCallerIdentity` requests from a contained workload. Neither uses `credsd`.

This branch is for evaluation, not release. The verification record names the original local branch and records the checks before publication.

## Build and run

From the repository root:

```sh
cargo build -p strands-box --all-features --bins --locked
cargo build -p strands-box-containment --all-features \
  --bin strands-box-contain-trampoline --locked
```

To use credentials already exported in the host shell:

```toml
[egress.aws]
destinations = ["sts.us-east-1.amazonaws.com"]
secret.ref = "aws-env://default"
```

Box reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN`. The workload receives example credentials. Box signs with the real credentials.

Run the included smoke check after exporting the variables:

```sh
python3 crates/box/prototype-aws/run.py environment
```

To use a profile with `credential_process`, keep the existing profile reference:

```toml
[egress.aws]
destinations = ["sts.us-east-1.amazonaws.com"]
secret.ref = "aws://bedrock"
```

The profile remains in the host's AWS configuration. Run:

```sh
python3 crates/box/prototype-aws/run.py profile --profile bedrock
```

The helper must already work on the host. A helper failure reaches the workload as a generic HTTP 403; run the configured helper directly to check login and access.

The smoke runner also supports `environment --seed-profile bedrock`. It runs the profile's helper in the parent process, keeps the returned credentials in memory, and passes them to Box as environment variables. This simulates the operator exporting temporary credentials.

Each smoke run makes two STS calls from one workload. Success requires HTTP 200 for both. The runner creates temporary Box state under `/var/tmp` and removes it afterward.

The [verification record](./verification.json) contains the results and source and binary hashes. Both sources passed with an unusable `CREDSD_SOCKET`. Separate checks covered command capture, helper environment filtering, invalid output, expired credentials, and the output limit. A helper descendant that held stdout open was stopped after 30.05 seconds.

## Implementation and limits

The environment change is a small configuration adapter. It selects the existing environment provider through a structured locator.

The larger change is the credential helper runner in [`prototype_aws_process.rs`](../../credentials/src/sources/prototype_aws_process.rs). Box captures the command, executable path, arguments, and allowed environment before starting the workload. It runs that command for every signed request without an implicit shell. It requires Version 1 JSON and nonempty key fields without control characters. When `Expiration` is present, it must be later than the current time.

The runner has a 30-second deadline and a 1 MiB output limit. It discards helper stderr and withholds raw stdout from diagnostics. The allowed environment excludes AWS key variables and unrelated token variables. The executable's metadata must match the startup snapshot.

This does not contain the helper. The helper is trusted host code. Its scripts, libraries, and configuration are not frozen, and the metadata check does not remove the race before execution. Those boundaries need a separate design before release.

Box adds no credential cache in this prototype. Each request pays for a helper process; ADA can use its own cache. Environment credentials cannot renew themselves, and changing the parent shell does not change an existing Box process.

Native SSO, AssumeRole, web identity, and metadata discovery remain unsupported. A profile cannot combine `credential_process` with static keys or role settings. Existing endpoint and signing limits still apply. The smoke check covers only `sts.us-east-1.amazonaws.com`. It checks HTTP status, but does not verify the returned identity or credential renewal after expiry. Only Linux was exercised.
