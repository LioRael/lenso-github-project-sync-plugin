# Lenso GitHub Project Sync Plugin

`lenso.github-project-sync.postgres` is a removable, PostgreSQL-owned bridge between Lenso Projects and GitHub Issues / Projects v2. It consumes only portable Lenso Capabilities; it never reads the Projects database and Projects never gains GitHub tables.

## Real workflow

1. An operator registers a GitHub App installation and maps one GitHub repository to one Lenso organization, team, and project.
2. A trusted HTTP adapter passes GitHub's delivery ID, event name, `X-Hub-Signature-256`, and exact raw request bytes to `ingest_webhook`.
3. The plugin validates HMAC-SHA256 before JSON parsing and inserts the delivery into a durable inbox. Reusing a delivery ID with different event or payload bytes is a conflict.
4. A scheduler invokes `run_worker` with a service-account actor assertion accepted by Projects. Workers claim inbox and outbox rows with `FOR UPDATE SKIP LOCKED`, expiring leases, and fencing tokens.
5. Inbound GitHub mutations use stable Projects issue UUIDs, full read-preserve-write requests, `expected_revision`, and bounded CAS retry. Outbound work consumes the exclusive `list_activity(after)` cursor and advances a checkpoint only in the transaction that persists the entire page as unique outbox jobs.

The worker operation is deliberately explicit: the Host chooses scheduling and service-account identity; this Plugin owns synchronization correctness, not a hidden background runtime.

## Capabilities

`lenso.github-project-sync@1`:

- `ingest_webhook`
- `get_binding`
- `list_bindings`

`lenso.github-project-sync-admin@1`:

- `set_app_credentials`
- `set_webhook_secrets`
- `put_installation`
- `revoke_installation`
- `put_mapping`
- `delete_mapping`
- `list_mappings`
- `inspect_delivery`
- `list_dead_letters`
- `replay_dead_letter`
- `run_worker`
- `get_checkpoint`

The linked-native provider requires exactly one each of `lenso.secrets@1`, `lenso.http.client@1`, `lenso.projects@1`, and `lenso.projects-collaboration@1`.

## Synchronized semantics

- Issue title, Markdown body, open/closed state, configured labels, configured milestone, archive-to-close, and comments flow both directions.
- Each GitHub issue is bound to one stable Projects issue UUID. `put_external_link` projects the GitHub URL into Projects; reverse lookup remains owned by this plugin's binding table because Projects v1 intentionally has no external-link query.
- A configured Projects v2 single-select Status option is updated from a Lenso workflow state. `projects_v2_item` webhooks fetch the authoritative item/field value through GraphQL and map it back to the Lenso workflow state.
- Multiple Lenso workflow states may map to GitHub `open`; exactly one configured `inbound_default` for `open` and one for `closed` resolves GitHub's lower-fidelity state.
- Configured conflict policy is `github_wins`, `lenso_wins`, `latest_updated_at`, or `manual`. Only revision conflicts receive bounded read/CAS retry; other Projects domain failures are not blindly replayed.
- Inbound revision receipts suppress their later Projects activity job. Outbound HTML markers suppress webhooks only when the marker also resolves to a locally persisted job receipt.

## GitHub App setup

Repository permissions: Metadata read and Issues read/write. Add organization Projects read/write only for mappings that configure a Projects v2 project. Subscribe only to `issues`, `issue_comment`, `installation`, `installation_repositories`, and `projects_v2_item` (the latter is a GitHub public-preview event).

Configure the HTTP egress provider with the exact same HTTPS origin listed in both `github_api_origin` and `allowed_github_origins`. Installation access tokens are requested for only the mapped repository and narrowed permissions. App private keys, webhook secrets, and installation tokens are resolved through `lenso.secrets@1`; only secret references are stored.

Example Plugin configuration:

```json
{
  "schema": "lenso_github_sync",
  "database_url_secret": "secret://postgres/github-sync",
  "app_id": "123456",
  "private_key_secret_ref": "secret://github/app-private-key",
  "webhook_secret_ref": "secret://github/webhook-current",
  "github_api_origin": "https://api.github.com",
  "github_api_version": "2026-03-10",
  "allowed_github_origins": ["https://api.github.com"],
  "max_webhook_body_bytes": 1048576,
  "webhook_callers": ["github-webhook-gateway"],
  "reader_callers": ["console"],
  "admin_callers": ["organization-admin-api"],
  "worker_callers": ["github-sync-scheduler"]
}
```

Run schema administration explicitly before activation:

```rust,no_run
use lenso_github_project_sync_postgres_plugin::GithubProjectSyncOperator;

# async fn setup(database_url: &str) -> Result<(), Box<dyn std::error::Error>> {
GithubProjectSyncOperator::setup(database_url, "lenso_github_sync").await?;
# Ok(()) }
```

Credential rotation is CAS-controlled. Set `current_secret_ref` and retain one `previous_secret_ref` during the delivery overlap, then remove the previous reference in a second revision. Revoking an installation atomically disables mappings and dead-letters its queued outbound work.

## Delivery guarantees

- Inbox delivery identity is `(delivery_id, event, SHA-256(raw body))`.
- Outbox identity is `(mapping_id, Projects activity_id)`; job IDs and origin markers are deterministic.
- Checkpoints are exclusive high-water cursors and never advance before every activity in the page is durable.
- Expired leases may be reclaimed, but a stale worker cannot complete after the fencing token changes.
- Retry uses bounded exponential backoff and eight attempts. Terminal failures remain inspectable and require explicit replay.
- An uncertain GitHub comment-create transport result is dead-lettered instead of automatically repeated. Issue create retries first reconcile the deterministic origin marker against recent issues.

## Intentional gaps

GitHub cannot faithfully represent Projects cycles, parent/relations, priority, multiple open workflow states, private-team visibility, multi-team project membership, or project updates/health. These are preserved in Projects and never fabricated in GitHub. Pull requests, assignees, reactions, sub-issues, draft Project items, GitHub Projects classic, attachments, and deletion propagation beyond issue archive/close are not declared in v1. One Projects v2 board may belong to only one repository mapping per installation in v1. ProjectV2 webhook payloads are preview API surface and unknown shapes fail closed into the dead-letter queue.

## Verification

```bash
lenso-contract-codegen check crates/lenso-capability-github-project-sync/capability.json --rust crates/lenso-capability-github-project-sync/src/generated.rs
lenso-contract-codegen check crates/lenso-capability-github-project-sync-admin/capability.json --rust crates/lenso-capability-github-project-sync-admin/src/generated.rs
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
./scripts/check-repository-boundary.sh
```

The PostgreSQL acceptance test additionally uses `LENSO_GITHUB_SYNC_TEST_DATABASE_URL`; the database name must start with `lenso_github_sync_test`.
