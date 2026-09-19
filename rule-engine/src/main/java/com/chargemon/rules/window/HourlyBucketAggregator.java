package com.chargemon.rules.window;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.SortedMap;

/**
 * Keeps one counter per hour bucket and derives every configured window from
 * them. Avoids Flink sliding windows (which duplicate state per pane) and lets
 * rolling 7d/30d coexist with tumbling hour/day in one small map.
 *
 * <p>All times are epoch-hours. "Now" is the hour the caller considers current
 * (max of event hour and watermark hour).
 */
public final class HourlyBucketAggregator {

    private final List<WindowSpec> windows;
    private final long maxHours;

    public HourlyBucketAggregator(List<WindowSpec> windows) {
        if (windows.isEmpty()) {
            throw new IllegalArgumentException("at least one window is required");
        }
        this.windows = List.copyOf(windows);
        this.maxHours = windows.stream().mapToLong(w -> w.kind() == WindowSpec.Kind.TUMBLING_DAY ? 25 : w.lengthHours())
                .max().orElse(1);
    }

    public List<WindowSpec> windows() {
        return windows;
    }

    /** Hours to retain; buckets older than {@code nowHour - retentionHours} can be dropped. */
    public long retentionHours() {
        return maxHours;
    }

    public static long hourOf(java.time.Instant t) {
        return Math.floorDiv(t.getEpochSecond(), 3600L);
    }

    /** True if an event in {@code eventHour} can still affect any window as of {@code nowHour}. */
    public boolean accepts(long eventHour, long nowHour) {
        return eventHour > nowHour - maxHours;
    }

    /** Current value of every window given the buckets. */
    public Map<String, Long> values(SortedMap<Long, Integer> buckets, long nowHour) {
        Map<String, Long> out = new LinkedHashMap<>();
        for (WindowSpec w : windows) {
            long start = w.windowStartHour(nowHour);
            long end = w.windowEndHour(nowHour);
            long sum = 0;
            for (Map.Entry<Long, Integer> e : buckets.subMap(start, end).entrySet()) {
                sum += e.getValue();
            }
            out.put(w.name(), sum);
        }
        return out;
    }

    /** Drops buckets that no window can ever include again. Returns number removed. */
    public int expire(SortedMap<Long, Integer> buckets, long nowHour) {
        SortedMap<Long, Integer> old = buckets.headMap(nowHour - maxHours + 1);
        int n = old.size();
        old.clear();
        return n;
    }

    /**
     * Next epoch-hour at which some window value changes without new input
     * (a tumbling boundary passes, or the oldest bucket leaves a rolling window).
     */
    public OptionalLong nextChangeHour(SortedMap<Long, Integer> buckets, long nowHour) {
        if (buckets.isEmpty()) {
            return OptionalLong.empty();
        }
        long best = Long.MAX_VALUE;
        for (WindowSpec w : windows) {
            long start = w.windowStartHour(nowHour);
            long end = w.windowEndHour(nowHour);
            SortedMap<Long, Integer> inWindow = buckets.subMap(start, end);
            if (inWindow.isEmpty()) {
                continue;
            }
            long candidate = w.isRolling() ? inWindow.firstKey() + w.lengthHours() : end;
            best = Math.min(best, candidate);
        }
        return best == Long.MAX_VALUE ? OptionalLong.empty() : OptionalLong.of(best);
    }
}
