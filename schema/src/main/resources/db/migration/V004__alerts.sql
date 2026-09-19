-- Alert lifecycle, written by the Flink JDBC sink (single writer) and acked by humans / tooling.
CREATE TABLE alerts (
    alert_id       UUID PRIMARY KEY,
    alert_key      TEXT NOT NULL,              -- ruleId|subjectType|subjectId
    rule_id        TEXT NOT NULL,              -- rule ids may come from non-Postgres producers
    rule_version   INT NOT NULL,
    rule_name      TEXT NOT NULL,
    kind           TEXT NOT NULL,
    severity       TEXT NOT NULL,
    subject_type   TEXT NOT NULL,
    subject_id     TEXT NOT NULL,
    group_ids      TEXT[] NOT NULL DEFAULT '{}',
    status         TEXT NOT NULL,              -- OPEN | RESOLVED
    opened_at      TIMESTAMPTZ NOT NULL,
    resolved_at    TIMESTAMPTZ,
    resolve_reason TEXT,
    context        JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_seq       BIGINT NOT NULL,
    acked_by       TEXT,
    acked_at       TIMESTAMPTZ,
    ack_note       TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT alerts_status_chk CHECK (status IN ('OPEN', 'RESOLVED'))
);

CREATE INDEX alerts_key_idx ON alerts (alert_key);
CREATE INDEX alerts_status_severity_idx ON alerts (status, severity);
CREATE INDEX alerts_subject_idx ON alerts (subject_id);
CREATE INDEX alerts_opened_at_idx ON alerts (opened_at DESC);
