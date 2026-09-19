-- Group hierarchy (site > region > country ...) mirrored from the `groups` topic.
CREATE TABLE station_groups (
    group_id   TEXT PRIMARY KEY,
    parent_id  TEXT REFERENCES station_groups (group_id) ON DELETE SET NULL,
    name       TEXT,
    level      TEXT,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    deleted    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX station_groups_parent_idx ON station_groups (parent_id);

-- Direct memberships only; ancestors are derived from station_groups.parent_id.
CREATE TABLE station_group_members (
    station_id TEXT NOT NULL,
    group_id   TEXT NOT NULL,
    PRIMARY KEY (station_id, group_id)
);

CREATE INDEX station_group_members_group_idx ON station_group_members (group_id);

-- Atomic upsert used by the Flink JDBC mirror sink (procedures so they batch cleanly via CALL).
-- p: {"stationId":..,"name":..,"vendor":..,"model":..,"firmware":..,"ocppVersion":..,
--     "attributes":{..},"groupIds":[..],"deleted":false}
CREATE OR REPLACE PROCEDURE upsert_station(p JSONB)
LANGUAGE plpgsql AS $$
DECLARE
    v_station_id TEXT := p ->> 'stationId';
BEGIN
    INSERT INTO stations (station_id, name, vendor, model, firmware, ocpp_version, attributes, deleted, updated_at)
    VALUES (v_station_id,
            p ->> 'name',
            p ->> 'vendor',
            p ->> 'model',
            p ->> 'firmware',
            p ->> 'ocppVersion',
            COALESCE(p -> 'attributes', '{}'::jsonb),
            COALESCE((p ->> 'deleted')::BOOLEAN, FALSE),
            now())
    ON CONFLICT (station_id) DO UPDATE SET
        name         = EXCLUDED.name,
        vendor       = EXCLUDED.vendor,
        model        = EXCLUDED.model,
        firmware     = EXCLUDED.firmware,
        ocpp_version = EXCLUDED.ocpp_version,
        attributes   = EXCLUDED.attributes,
        deleted      = EXCLUDED.deleted,
        updated_at   = now();

    DELETE FROM station_group_members WHERE station_id = v_station_id;

    INSERT INTO station_group_members (station_id, group_id)
    SELECT v_station_id, g
    FROM jsonb_array_elements_text(COALESCE(p -> 'groupIds', '[]'::jsonb)) AS g
    ON CONFLICT DO NOTHING;
END;
$$;

-- p: {"groupId":..,"parentId":..,"name":..,"level":..,"attributes":{..},"deleted":false}
CREATE OR REPLACE PROCEDURE upsert_group(p JSONB)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO station_groups (group_id, parent_id, name, level, attributes, deleted, updated_at)
    VALUES (p ->> 'groupId',
            p ->> 'parentId',
            p ->> 'name',
            p ->> 'level',
            COALESCE(p -> 'attributes', '{}'::jsonb),
            COALESCE((p ->> 'deleted')::BOOLEAN, FALSE),
            now())
    ON CONFLICT (group_id) DO UPDATE SET
        parent_id  = EXCLUDED.parent_id,
        name       = EXCLUDED.name,
        level      = EXCLUDED.level,
        attributes = EXCLUDED.attributes,
        deleted    = EXCLUDED.deleted,
        updated_at = now();
END;
$$;
