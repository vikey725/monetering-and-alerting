-- Mirror of the upstream station registry (fed by the Flink JDBC sink from the `stations` topic).
CREATE TABLE stations (
    station_id   TEXT PRIMARY KEY,
    name         TEXT,
    vendor       TEXT,
    model        TEXT,
    firmware     TEXT,
    ocpp_version TEXT,
    attributes   JSONB NOT NULL DEFAULT '{}'::jsonb,
    deleted      BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX stations_vendor_model_idx ON stations (vendor, model);
