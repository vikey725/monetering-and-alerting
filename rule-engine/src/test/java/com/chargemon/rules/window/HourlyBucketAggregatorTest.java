package com.chargemon.rules.window;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class HourlyBucketAggregatorTest {

    private static final List<WindowSpec> SPECS = WindowSpec.parseList(
            "hourly=TUMBLING_HOUR,daily=TUMBLING_DAY,rolling7d=ROLLING:P7D,rolling30d=ROLLING:P30D", ZoneOffset.UTC);
    private final HourlyBucketAggregator agg = new HourlyBucketAggregator(SPECS);

    @Test
    void parsesSpecs() {
        assertThat(SPECS).extracting(WindowSpec::name).containsExactly("hourly", "daily", "rolling7d", "rolling30d");
        assertThat(SPECS.get(2).lengthHours()).isEqualTo(168);
        assertThat(agg.retentionHours()).isEqualTo(720);
    }

    @Test
    void computesWindowsAndRollsOver() {
        TreeMap<Long, Integer> b = new TreeMap<>();
        long h0 = HourlyBucketAggregator.hourOf(Instant.parse("2026-03-10T23:00:00Z"));
        b.put(h0, 2);
        b.put(h0 - 1, 1);
        b.put(h0 - 24 * 3, 5);          // three days ago
        b.put(h0 - 24 * 10, 7);         // ten days ago

        Map<String, Long> v = agg.values(b, h0);
        assertThat(v).containsEntry("hourly", 2L).containsEntry("daily", 3L)
                .containsEntry("rolling7d", 8L).containsEntry("rolling30d", 15L);

        long h1 = h0 + 1;               // next day 00:00
        v = agg.values(b, h1);
        assertThat(v).containsEntry("hourly", 0L).containsEntry("daily", 0L).containsEntry("rolling7d", 8L);
        assertThat(agg.nextChangeHour(b, h0)).hasValue(h0 + 1);
    }

    @Test
    void expiresOnlyBucketsOutsideEveryWindow() {
        TreeMap<Long, Integer> b = new TreeMap<>();
        long now = 1_000_000L;
        b.put(now - 719, 1);
        b.put(now - 720, 1);
        b.put(now - 800, 1);
        assertThat(agg.expire(b, now)).isEqualTo(2);
        assertThat(b).containsOnlyKeys(now - 719);
        assertThat(agg.accepts(now - 719, now)).isTrue();
        assertThat(agg.accepts(now - 720, now)).isFalse();
    }

    @Test
    void nextChangeHourIsWhenOldestRollingBucketDrops() {
        HourlyBucketAggregator rolling = new HourlyBucketAggregator(WindowSpec.parseList("r=ROLLING:PT3H", ZoneOffset.UTC));
        TreeMap<Long, Integer> b = new TreeMap<>();
        b.put(100L, 1);
        assertThat(rolling.values(b, 102).get("r")).isEqualTo(1);
        assertThat(rolling.nextChangeHour(b, 102)).hasValue(103);
        assertThat(rolling.values(b, 103).get("r")).isEqualTo(0);
        assertThat(rolling.nextChangeHour(b, 103)).isEmpty();
    }

    @Test
    void matchesBruteForceReference() {
        Random rnd = new Random(7);
        for (int trial = 0; trial < 200; trial++) {
            TreeMap<Long, Integer> b = new TreeMap<>();
            long now = 500_000L + rnd.nextInt(1000);
            for (int i = 0; i < 40; i++) {
                b.merge(now - rnd.nextInt(800), 1 + rnd.nextInt(3), Integer::sum);
            }
            Map<String, Long> v = agg.values(b, now);
            for (WindowSpec w : SPECS) {
                long expected = 0;
                for (Map.Entry<Long, Integer> e : b.entrySet()) {
                    long h = e.getKey();
                    boolean in = switch (w.kind()) {
                        case TUMBLING_HOUR -> h == now;
                        case TUMBLING_DAY -> Math.floorDiv(h, 24) == Math.floorDiv(now, 24);
                        case ROLLING -> h > now - w.lengthHours() && h <= now;
                    };
                    if (in) {
                        expected += e.getValue();
                    }
                }
                assertThat(v.get(w.name())).as(w.name() + " trial " + trial).isEqualTo(expected);
            }
        }
    }
}
