package com.chargemon.generator;

import com.chargemon.ocpp.codec.envelope.RawEnvelope;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Round-robins the fleet, publishing whatever each station has due, throttled to
 * the target rate. {@code speedup} compresses simulated time for quick demos.
 */
final class Simulation {

    private final Fleet fleet;
    private final Publisher publisher;
    private final String topic;
    private final int rate;
    private final int speedup;

    Simulation(Fleet fleet, Publisher publisher, String topic, int rate, int speedup) {
        this.fleet = fleet;
        this.publisher = publisher;
        this.topic = topic;
        this.rate = Math.max(1, rate);
        this.speedup = Math.max(1, speedup);
    }

    void run(Duration duration) throws InterruptedException {
        long wallStart = System.currentTimeMillis();
        long wallEnd = duration.isZero() ? Long.MAX_VALUE : wallStart + duration.toMillis();
        long sent = 0;
        long lastReport = wallStart;
        while (System.currentTimeMillis() < wallEnd) {
            long wallNow = System.currentTimeMillis();
            Instant simNow = Instant.ofEpochMilli(wallStart + (wallNow - wallStart) * speedup);
            int produced = 0;
            for (StationSim s : fleet.stations()) {
                List<RawEnvelope> due = s.tick(simNow);
                for (RawEnvelope e : due) {
                    publisher.envelope(topic, e);
                }
                produced += due.size();
            }
            sent += produced;
            if (wallNow - lastReport >= 5000) {
                System.out.printf("sent=%d rate=%.0f/s simTime=%s%n", sent, sent * 1000.0 / (wallNow - wallStart), simNow);
                lastReport = wallNow;
            }
            // Throttle: sleep so that the cumulative rate does not exceed the target.
            long expectedMillis = sent * 1000 / rate;
            long elapsed = System.currentTimeMillis() - wallStart;
            long sleep = Math.max(20, expectedMillis - elapsed);
            Thread.sleep(Math.min(sleep, 1000));
        }
        publisher.flush();
        System.out.printf("done, sent=%d%n", sent);
    }
}
