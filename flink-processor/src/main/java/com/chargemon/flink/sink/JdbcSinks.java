package com.chargemon.flink.sink;

import com.chargemon.alert.AlertEvent;
import com.chargemon.common.json.JsonMapperFactory;
import com.chargemon.flink.config.JobConfig;
import com.chargemon.flink.model.AggregateSnapshot;
import com.chargemon.rules.window.WindowSpec;
import com.chargemon.ocpp.model.station.GroupRecord;
import com.chargemon.ocpp.model.station.StationRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;

/** Postgres bookkeeping writers. All statements are idempotent upserts. */
public final class JdbcSinks {

    static final String ALERT_UPSERT = """
        INSERT INTO alerts (alert_id, alert_key, rule_id, rule_version, rule_name, kind, severity, subject_type,
                            subject_id, group_ids, status, opened_at, resolved_at, resolve_reason, context, last_seq, updated_at)
        VALUES (?::uuid, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, now())
        ON CONFLICT (alert_id) DO UPDATE SET
            status = EXCLUDED.status, resolved_at = EXCLUDED.resolved_at, resolve_reason = EXCLUDED.resolve_reason,
            context = EXCLUDED.context, last_seq = EXCLUDED.last_seq, rule_version = EXCLUDED.rule_version,
            group_ids = EXCLUDED.group_ids, updated_at = now()
        WHERE alerts.last_seq < EXCLUDED.last_seq
        """;

    private final JobConfig cfg;

    public JdbcSinks(JobConfig cfg) {
        this.cfg = cfg;
    }

    public JdbcSink<AlertEvent> alerts() {
        return sink(ALERT_UPSERT, (ps, a) -> {
            ps.setString(1, a.alertId());
            ps.setString(2, a.alertKey());
            ps.setString(3, a.ruleId());
            ps.setInt(4, a.ruleVersion());
            ps.setString(5, a.ruleName());
            ps.setString(6, a.kind());
            ps.setString(7, a.severity().name());
            ps.setString(8, a.subjectType().name());
            ps.setString(9, a.subjectId());
            ps.setArray(10, ps.getConnection().createArrayOf("text", a.groupIds().toArray()));
            ps.setString(11, a.type() == com.chargemon.alert.AlertEventType.OPENED ? "OPEN" : "RESOLVED");
            ps.setTimestamp(12, Timestamp.from(a.openedAt()));
            if (a.resolvedAt() == null) {
                ps.setNull(13, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                ps.setTimestamp(13, Timestamp.from(a.resolvedAt()));
            }
            ps.setString(14, a.resolveReason() == null ? null : a.resolveReason().name());
            ps.setString(15, toJson(a.context()));
            ps.setLong(16, a.seq());
        });
    }

    static final String TUMBLING_UPSERT = """
        INSERT INTO zero_energy_aggregates (subject_type, subject_id, window_name, window_start, window_end, session_count, as_of)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (subject_type, subject_id, window_name, window_start) DO UPDATE SET
            session_count = EXCLUDED.session_count, as_of = EXCLUDED.as_of
        WHERE zero_energy_aggregates.as_of <= EXCLUDED.as_of
        """;

    static final String ROLLING_UPSERT = """
        INSERT INTO zero_energy_rolling (subject_type, subject_id, window_name, session_count, as_of)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (subject_type, subject_id, window_name) DO UPDATE SET
            session_count = EXCLUDED.session_count, as_of = EXCLUDED.as_of
        WHERE zero_energy_rolling.as_of <= EXCLUDED.as_of
        """;

    /** One snapshot row per (subject, window); tumbling windows keep history, rolling windows keep the latest. */
    public record WindowRow(String subjectType, String subjectId, String window, boolean rolling, long startHour,
                            long endHour, long count, java.time.Instant asOf) {
    }

    public JdbcSink<WindowRow> tumblingAggregates() {
        return sink(TUMBLING_UPSERT, (ps, r) -> {
            ps.setString(1, r.subjectType());
            ps.setString(2, r.subjectId());
            ps.setString(3, r.window());
            ps.setTimestamp(4, Timestamp.from(java.time.Instant.ofEpochSecond(r.startHour() * 3600)));
            ps.setTimestamp(5, Timestamp.from(java.time.Instant.ofEpochSecond(r.endHour() * 3600)));
            ps.setLong(6, r.count());
            ps.setTimestamp(7, Timestamp.from(r.asOf()));
        });
    }

    public JdbcSink<WindowRow> rollingAggregates() {
        return sink(ROLLING_UPSERT, (ps, r) -> {
            ps.setString(1, r.subjectType());
            ps.setString(2, r.subjectId());
            ps.setString(3, r.window());
            ps.setLong(4, r.count());
            ps.setTimestamp(5, Timestamp.from(r.asOf()));
        });
    }

    /** Explodes a snapshot into per-window rows using the configured window specs. */
    public static java.util.List<WindowRow> rows(AggregateSnapshot s, java.util.List<WindowSpec> specs) {
        long hour = Math.floorDiv(s.asOf().getEpochSecond(), 3600L);
        java.util.List<WindowRow> out = new java.util.ArrayList<>(specs.size());
        for (WindowSpec w : specs) {
            Long v = s.windows().get(w.name());
            if (v == null) {
                continue;
            }
            out.add(new WindowRow(s.subjectType().name(), s.subjectId(), w.name(), w.isRolling(),
                    w.windowStartHour(hour), w.windowEndHour(hour), v, s.asOf()));
        }
        return out;
    }

    public JdbcSink<StationRecord> stationMirror() {
        return sink("CALL upsert_station(?::jsonb)", (ps, s) -> ps.setString(1, toJson(s)));
    }

    public JdbcSink<GroupRecord> groupMirror() {
        return sink("CALL upsert_group(?::jsonb)", (ps, g) -> ps.setString(1, toJson(g)));
    }

    private <T> JdbcSink<T> sink(String sql, JdbcStatementBuilder<T> builder) {
        return JdbcSink.<T>builder()
                .withQueryStatement(sql, builder)
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(500)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(3)
                        .build())
                .buildAtLeastOnce(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(cfg.dbUrl())
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(cfg.dbUser())
                        .withPassword(cfg.dbPassword())
                        .withConnectionCheckTimeoutSeconds(30)
                        .build());
    }

    private static String toJson(Object o) {
        try {
            return JsonMapperFactory.standard().writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
