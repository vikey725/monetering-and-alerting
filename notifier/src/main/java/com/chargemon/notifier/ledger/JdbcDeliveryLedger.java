package com.chargemon.notifier.ledger;

import com.chargemon.alert.ChannelRef;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcDeliveryLedger implements DeliveryLedger {

    private static final String CLAIM = """
        INSERT INTO notification_deliveries (alert_event_id, channel_ref, status, attempts, updated_at)
        VALUES (?, ?, 'CLAIMED', 1, now())
        ON CONFLICT (alert_event_id, channel_ref) DO UPDATE
            SET status = 'CLAIMED', attempts = notification_deliveries.attempts + 1, updated_at = now()
            WHERE notification_deliveries.status <> 'DELIVERED'
        """;

    private final JdbcTemplate jdbc;

    public JdbcDeliveryLedger(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean claim(String alertEventId, ChannelRef channel) {
        return jdbc.update(CLAIM, UUID.fromString(alertEventId), channel.toString()) > 0;
    }

    @Override
    public void markDelivered(String alertEventId, ChannelRef channel) {
        jdbc.update("UPDATE notification_deliveries SET status = 'DELIVERED', delivered_at = now(), last_error = NULL, "
                        + "updated_at = now() WHERE alert_event_id = ? AND channel_ref = ?",
                UUID.fromString(alertEventId), channel.toString());
    }

    @Override
    public void markFailed(String alertEventId, ChannelRef channel, String error, boolean willRetry) {
        jdbc.update("UPDATE notification_deliveries SET status = ?, last_error = ?, updated_at = now() "
                        + "WHERE alert_event_id = ? AND channel_ref = ?",
                willRetry ? "RETRYING" : "FAILED", error, UUID.fromString(alertEventId), channel.toString());
    }
}
