CREATE TABLE sync_settings (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    app_id TEXT NOT NULL,
    private_key_secret_ref TEXT NOT NULL,
    webhook_secret_ref TEXT NOT NULL,
    previous_webhook_secret_ref TEXT,
    revision BIGINT NOT NULL CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE github_installations (
    installation_id TEXT PRIMARY KEY,
    account_login TEXT NOT NULL,
    account_id TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    revision BIGINT NOT NULL CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE repository_mappings (
    mapping_id TEXT PRIMARY KEY,
    installation_id TEXT NOT NULL REFERENCES github_installations(installation_id),
    github_repository_id TEXT NOT NULL,
    github_owner TEXT NOT NULL,
    github_repository TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    github_project_id TEXT,
    github_project_status_field_id TEXT,
    inbound_enabled BOOLEAN NOT NULL,
    outbound_enabled BOOLEAN NOT NULL,
    conflict_policy TEXT NOT NULL CHECK (conflict_policy IN ('github_wins','lenso_wins','latest_updated_at','manual')),
    state_mappings JSONB NOT NULL,
    label_mappings JSONB NOT NULL,
    milestone_mappings JSONB NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    revision BIGINT NOT NULL CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (installation_id, github_repository_id),
    UNIQUE (organization_id, project_id, github_repository_id)
);
CREATE UNIQUE INDEX repository_mappings_project_unique
    ON repository_mappings (installation_id, github_project_id)
    WHERE github_project_id IS NOT NULL;

CREATE TABLE sync_checkpoints (
    mapping_id TEXT PRIMARY KEY REFERENCES repository_mappings(mapping_id) ON DELETE CASCADE,
    activity_cursor TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE github_issue_bindings (
    mapping_id TEXT NOT NULL REFERENCES repository_mappings(mapping_id) ON DELETE RESTRICT,
    github_repository_id TEXT NOT NULL,
    github_issue_number BIGINT NOT NULL CHECK (github_issue_number > 0),
    github_node_id TEXT NOT NULL,
    lenso_issue_id TEXT NOT NULL,
    github_project_item_id TEXT,
    last_github_updated_at TIMESTAMPTZ,
    last_lenso_revision BIGINT,
    revision BIGINT NOT NULL CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (mapping_id, github_issue_number),
    UNIQUE (mapping_id, github_node_id),
    UNIQUE (mapping_id, lenso_issue_id)
);

CREATE TABLE github_comment_bindings (
    mapping_id TEXT NOT NULL REFERENCES repository_mappings(mapping_id) ON DELETE RESTRICT,
    lenso_comment_id TEXT NOT NULL,
    github_comment_id TEXT NOT NULL,
    issue_number BIGINT NOT NULL CHECK (issue_number > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (mapping_id, lenso_comment_id),
    UNIQUE (mapping_id, github_comment_id)
);

CREATE TABLE webhook_deliveries (
    delivery_id TEXT PRIMARY KEY,
    event TEXT NOT NULL,
    payload BYTEA NOT NULL,
    payload_sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','processing','retry','succeeded','dead')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    fence BIGINT NOT NULL DEFAULT 0 CHECK (fence >= 0),
    last_error_code TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);
CREATE INDEX webhook_deliveries_due ON webhook_deliveries (next_attempt_at, delivery_id)
    WHERE state IN ('pending','retry','processing');

CREATE TABLE outbound_jobs (
    job_id TEXT PRIMARY KEY,
    mapping_id TEXT NOT NULL REFERENCES repository_mappings(mapping_id) ON DELETE RESTRICT,
    activity_id TEXT NOT NULL,
    entity_kind TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    issue_id TEXT,
    operation TEXT NOT NULL,
    lenso_revision BIGINT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL CHECK (state IN ('pending','processing','retry','succeeded','dead','suppressed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    fence BIGINT NOT NULL DEFAULT 0 CHECK (fence >= 0),
    last_error_code TEXT,
    remote_receipt JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (mapping_id, activity_id)
);
CREATE INDEX outbound_jobs_due ON outbound_jobs (next_attempt_at, job_id)
    WHERE state IN ('pending','retry','processing');

CREATE TABLE origin_markers (
    marker TEXT PRIMARY KEY,
    mapping_id TEXT NOT NULL REFERENCES repository_mappings(mapping_id) ON DELETE RESTRICT,
    job_id TEXT NOT NULL UNIQUE REFERENCES outbound_jobs(job_id) ON DELETE CASCADE,
    github_node_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE inbound_effects (
    delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(delivery_id) ON DELETE CASCADE,
    effect_key TEXT NOT NULL,
    result JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (delivery_id, effect_key)
);

CREATE TABLE inbound_suppressions (
    mapping_id TEXT NOT NULL REFERENCES repository_mappings(mapping_id) ON DELETE RESTRICT,
    entity_kind TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(delivery_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (mapping_id, entity_kind, entity_id, revision)
);
