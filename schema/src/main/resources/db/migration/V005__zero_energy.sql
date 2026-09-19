-- Tumbling-window history (hourly, daily) per station / group.
CREATE TABLE zero_energy_aggregates (
    subject_type  TEXT NOT NULL,               -- STATION | GROUP
    subject_id    TEXT NOT NULL,
    window_name   TEXT NOT NULL,               -- hourly | daily | ...
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    session_count INT NOT NULL,
    as_of         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (subject_type, subject_id, window_name, window_start)
);

CREATE INDEX zero_energy_aggregates_window_idx ON zero_energy_aggregates (window_name, window_start DESC);

-- Current value of rolling windows (rolling7d, rolling30d, ...).
CREATE TABLE zero_energy_rolling (
    subject_type  TEXT NOT NULL,
    subject_id    TEXT NOT NULL,
    window_name   TEXT NOT NULL,
    session_count INT NOT NULL,
    as_of         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (subject_type, subject_id, window_name)
);
