package com.chargemon.flink.config;

import com.chargemon.common.config.Env;
import java.io.Serializable;
import java.time.Duration;

/** All tunables of the job, read once from the environment and shipped to operators. */
public record JobConfig(
        String kafkaBootstrap,
        String consumerGroup,
        String eventsTopic,
        String stationsTopic,
        String groupsTopic,
        String rulesTopic,
        String alertsTopic,
        String deadLetterTopic,
        String lateEventsTopic,
        Duration correlationTimeout,
        Duration maxOutOfOrderness,
        Duration sourceIdleness,
        Duration pendingCallTtl,
        Duration stationStateTtl,
        Duration sessionTrackTtl,
        Duration checkpointInterval,
        int parallelism,
        String dbUrl,
        String dbUser,
        String dbPassword,
        String zeroEnergyWindows) implements Serializable {

    public static JobConfig fromEnv(Env env) {
        return new JobConfig(
                env.get("KAFKA_BOOTSTRAP", "localhost:9092"),
                env.get("KAFKA_GROUP", "chargemon-processor"),
                env.get("TOPIC_EVENTS", "common-broker"),
                env.get("TOPIC_STATIONS", "stations"),
                env.get("TOPIC_GROUPS", "groups"),
                env.get("TOPIC_RULES", "rules"),
                env.get("TOPIC_ALERTS", "alerts"),
                env.get("TOPIC_DEAD_LETTER", "dead-letter"),
                env.get("TOPIC_LATE_EVENTS", "late-events"),
                env.getDuration("CORRELATION_TIMEOUT", Duration.ofSeconds(60)),
                env.getDuration("MAX_OUT_OF_ORDERNESS", Duration.ofMinutes(5)),
                env.getDuration("SOURCE_IDLENESS", Duration.ofMinutes(1)),
                env.getDuration("PENDING_CALL_TTL", Duration.ofMinutes(5)),
                env.getDuration("STATION_STATE_TTL", Duration.ofDays(30)),
                env.getDuration("SESSION_TRACK_TTL", Duration.ofHours(48)),
                env.getDuration("CHECKPOINT_INTERVAL", Duration.ofSeconds(30)),
                env.getInt("PARALLELISM", 0),
                env.get("DB_URL", "jdbc:postgresql://localhost:5432/chargemon"),
                env.get("DB_USER", "chargemon"),
                env.get("DB_PASSWORD", "chargemon"),
                env.get("ZERO_ENERGY_WINDOWS", "hourly=TUMBLING_HOUR,daily=TUMBLING_DAY,rolling7d=ROLLING:P7D,rolling30d=ROLLING:P30D"));
    }

    public static JobConfig defaults() {
        return fromEnv(new Env(java.util.Map.of()));
    }
}
