package com.chargemon.generator;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Produces realistic OCPP envelopes (both directions) for N simulated stations.
 *
 * <pre>
 * event-generator --stations 1000 --rate 500 --profiles normal,heartbeat-drop,zero-energy --seed-master
 * </pre>
 */
@Command(name = "event-generator", mixinStandardHelpOptions = true, description = "OCPP traffic simulator")
public final class GeneratorMain implements Callable<Integer> {

    @Option(names = "--bootstrap", defaultValue = "localhost:9092")
    String bootstrap;

    @Option(names = "--topic", defaultValue = "common-broker")
    String topic;

    @Option(names = "--stations-topic", defaultValue = "stations")
    String stationsTopic;

    @Option(names = "--db-url", defaultValue = "jdbc:postgresql://localhost:5432/chargemon",
            description = "Postgres holding station_groups (groups reach Kafka via Debezium)")
    String dbUrl;

    @Option(names = "--db-user", defaultValue = "chargemon")
    String dbUser;

    @Option(names = "--db-password", defaultValue = "chargemon")
    String dbPassword;

    @Option(names = "--stations", defaultValue = "100", description = "number of simulated stations")
    int stations;

    @Option(names = "--rate", defaultValue = "100", description = "target envelopes per second")
    int rate;

    @Option(names = "--profiles", split = ",", defaultValue = "normal",
            description = "comma list of: normal, heartbeat-drop, stuck-preparing, zero-energy, boot-rejected, call-error")
    List<String> profiles;

    @Option(names = "--v16-ratio", defaultValue = "0.2", description = "fraction of stations speaking OCPP 1.6")
    double v16Ratio;

    @Option(names = "--duration", defaultValue = "PT0S", description = "ISO-8601 run time; PT0S = forever")
    Duration duration;

    @Option(names = "--speedup", defaultValue = "1", description = "simulated seconds per wall second")
    int speedup;

    @Option(names = "--seed-master", defaultValue = "false", description = "seed groups (Postgres) + stations (Kafka) first")
    boolean seedMaster;

    @Option(names = "--seed", defaultValue = "42")
    long seed;

    public static void main(String[] args) {
        System.exit(new CommandLine(new GeneratorMain()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        try (Publisher publisher = new Publisher(bootstrap)) {
            Fleet fleet = Fleet.build(stations, profiles, v16Ratio, seed);
            if (seedMaster) {
                new GroupSeeder(dbUrl, dbUser, dbPassword).seed(fleet);
                publisher.publishStations(fleet, stationsTopic);
            }
            new Simulation(fleet, publisher, topic, rate, speedup).run(duration);
        }
        return 0;
    }
}
