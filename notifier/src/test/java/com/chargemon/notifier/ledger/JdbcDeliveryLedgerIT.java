package com.chargemon.notifier.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.chargemon.alert.ChannelRef;
import com.chargemon.schema.SchemaMigrator;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class JdbcDeliveryLedgerIT {

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine");
    static JdbcTemplate jdbc;

    @BeforeAll
    static void migrate() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());
        SchemaMigrator.migrate(ds);
        jdbc = new JdbcTemplate(ds);
    }

    @Test
    void claimIsIdempotentAfterDeliveryAndRetriableAfterFailure() {
        JdbcDeliveryLedger ledger = new JdbcDeliveryLedger(jdbc);
        String id = UUID.randomUUID().toString();
        ChannelRef slack = ChannelRef.parse("slack:#ops");

        assertThat(ledger.claim(id, slack)).isTrue();
        ledger.markFailed(id, slack, "503", true);
        assertThat(ledger.claim(id, slack)).isTrue();                       // retry allowed
        ledger.markDelivered(id, slack);
        assertThat(ledger.claim(id, slack)).isFalse();                      // duplicate record: no second delivery
        assertThat(ledger.claim(id, ChannelRef.parse("email:a@b"))).isTrue(); // other channel independent
        Integer attempts = jdbc.queryForObject(
                "SELECT attempts FROM notification_deliveries WHERE alert_event_id = ?::uuid AND channel_ref = 'slack:#ops'",
                Integer.class, id);
        assertThat(attempts).isEqualTo(2);
    }
}
