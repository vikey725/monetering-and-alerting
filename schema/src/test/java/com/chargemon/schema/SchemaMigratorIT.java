package com.chargemon.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class SchemaMigratorIT {

    @Container
    static final PostgreSQLContainer<?> PG = new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    void migratesFromScratchAndUpsertsStation() throws Exception {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(PG.getJdbcUrl());
        ds.setUser(PG.getUsername());
        ds.setPassword(PG.getPassword());

        assertThat(SchemaMigrator.migrate(ds).migrationsExecuted).isGreaterThanOrEqualTo(6);

        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.execute("CALL upsert_station('{\"stationId\":\"ST-1\",\"vendor\":\"ACME\",\"groupIds\":[\"site:1\",\"site:2\"]}'::jsonb)");
            s.execute("CALL upsert_station('{\"stationId\":\"ST-1\",\"vendor\":\"ACME\",\"groupIds\":[\"site:2\"]}'::jsonb)");
            try (ResultSet rs = s.executeQuery("SELECT count(*) FROM station_group_members WHERE station_id = 'ST-1'")) {
                rs.next();
                assertThat(rs.getInt(1)).isEqualTo(1);
            }
        }
    }
}
