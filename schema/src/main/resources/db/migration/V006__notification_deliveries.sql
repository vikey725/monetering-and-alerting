-- Notifier delivery ledger: idempotency across Kafka redelivery + retry bookkeeping.
CREATE TABLE notification_deliveries (
    alert_event_id  UUID NOT NULL,
    channel_ref     TEXT NOT NULL,             -- e.g. slack:#ops
    status          TEXT NOT NULL,             -- CLAIMED | DELIVERED | FAILED
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (alert_event_id, channel_ref)
);

CREATE INDEX notification_deliveries_status_idx ON notification_deliveries (status, next_attempt_at);
