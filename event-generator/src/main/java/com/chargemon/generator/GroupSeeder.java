package com.chargemon.generator;

import com.chargemon.ocpp.codec.fixtures.Frames;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/** Writes the group hierarchy into Postgres {@code station_groups}; Debezium streams it on to the `groups` topic. */
final class GroupSeeder {

    private final String url;
    private final String user;
    private final String password;

    GroupSeeder(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    void seed(Fleet fleet) throws SQLException {
        try (Connection c = DriverManager.getConnection(url, user, password);
             PreparedStatement ps = c.prepareStatement("CALL upsert_group(?::jsonb)")) {
            for (Map.Entry<String, String> g : fleet.groups().entrySet()) {
                String[] parts = g.getKey().split(":", 2);
                ps.setString(1, Frames.obj("groupId", g.getKey(), "parentId", g.getValue(), "name", parts[1], "level", parts[0]).toString());
                ps.addBatch();
            }
            ps.executeBatch();
        }
        System.out.printf("seeded %d groups into %s%n", fleet.groups().size(), url);
    }
}
