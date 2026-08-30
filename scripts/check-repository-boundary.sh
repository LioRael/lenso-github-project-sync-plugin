#!/usr/bin/env bash
set -euo pipefail

expected_crates=$'lenso-capability-github-project-sync\nlenso-capability-github-project-sync-admin\nlenso-github-project-sync-postgres-plugin'
actual_crates="$({
  find crates -mindepth 1 -maxdepth 1 -type d -exec basename {} \;
} | LC_ALL=C sort)"

if [[ "$actual_crates" != "$expected_crates" ]]; then
  printf 'unexpected crate ownership:\n%s\n' "$actual_crates" >&2
  exit 1
fi

if rg -n 'sqlx|postgres|lenso-postgres-kit|lenso-capability-secrets|lenso-capability-http-client|lenso-capability-projects' \
  crates/lenso-capability-github-project-sync*/Cargo.toml crates/lenso-capability-github-project-sync*/src \
  --glob '!**/generated.rs'; then
  printf 'portable sync Capability gained an implementation dependency\n' >&2
  exit 1
fi

if rg -n 'reqwest|octocrab|curl|std::net|tokio::net' crates --glob '*.rs' --glob 'Cargo.toml'; then
  printf 'GitHub transport bypasses lenso.http.client@1\n' >&2
  exit 1
fi

if rg -ni 'CREATE TABLE (projects|issues|comments|workflow_states|project_statuses)' \
  crates/lenso-github-project-sync-postgres-plugin/migrations; then
  printf 'sync plugin attempted to own Projects storage\n' >&2
  exit 1
fi

if rg -ni '(private_key|webhook_secret|installation_token|access_token)[[:space:]]+(TEXT|BYTEA)' \
  crates/lenso-github-project-sync-postgres-plugin/migrations; then
  printf 'secret material column found; only secret references may persist\n' >&2
  exit 1
fi

if rg -n 'not_supported|not supported' crates --glob '*.rs'; then
  printf 'declared operation contains a not-supported stub\n' >&2
  exit 1
fi

rg -q 'lenso-capability-http-client' crates/lenso-github-project-sync-postgres-plugin/Cargo.toml
rg -q 'lenso-capability-projects' crates/lenso-github-project-sync-postgres-plugin/Cargo.toml
rg -q 'FOR UPDATE SKIP LOCKED' crates/lenso-github-project-sync-postgres-plugin/src/storage.rs
rg -q 'X-Hub-Signature-256' README.md

printf 'repository boundary is GitHub-sync-owned and Projects-storage-neutral\n'
