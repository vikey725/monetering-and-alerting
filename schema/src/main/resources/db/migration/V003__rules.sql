-- Alert rule definitions. Edited via SQL / future admin API; streamed to Flink through Debezium CDC.
-- Durations are ISO-8601 text (PT10M) so Debezium and Flink need no interval conversion.
CREATE TABLE rules (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL,
    kind               TEXT NOT NULL,          -- EVENT | ABSENCE | STATE_DURATION | SEQUENCE | GROUP_AGGREGATE
    subject_type       TEXT NOT NULL,          -- STATION | GROUP
    target_group_ids   TEXT[] NOT NULL DEFAULT '{}',
    station_filter     JSONB,                  -- condition AST or NULL
    spec               JSONB NOT NULL,         -- kind-specific spec
    grace_window       TEXT NOT NULL DEFAULT 'PT0S',
    suppression_window TEXT NOT NULL DEFAULT 'PT0S',
    auto_resolve_after TEXT,
    severity           TEXT NOT NULL,          -- CRITICAL | HIGH | MEDIUM | LOW | INFO
    channels           TEXT[] NOT NULL DEFAULT '{}',
    enabled            BOOLEAN NOT NULL DEFAULT TRUE,
    version            INT NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT rules_kind_chk CHECK (kind IN ('EVENT', 'ABSENCE', 'STATE_DURATION', 'SEQUENCE', 'GROUP_AGGREGATE')),
    CONSTRAINT rules_subject_chk CHECK (subject_type IN ('STATION', 'GROUP')),
    CONSTRAINT rules_severity_chk CHECK (severity IN ('CRITICAL', 'HIGH', 'MEDIUM', 'LOW', 'INFO'))
);

CREATE OR REPLACE FUNCTION rules_bump_version() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.version    := OLD.version + 1;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER rules_bump_version_trg
    BEFORE UPDATE ON rules
    FOR EACH ROW
    EXECUTE FUNCTION rules_bump_version();

-- Debezium needs full row images for deletes / compaction keys.
ALTER TABLE rules REPLICA IDENTITY FULL;
