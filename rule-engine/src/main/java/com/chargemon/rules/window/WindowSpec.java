package com.chargemon.rules.window;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * A named aggregation window. Tumbling windows align to the hour/day in {@code zone};
 * rolling windows cover the trailing {@code length} up to and including the current hour.
 *
 * <p>Config syntax: {@code hourly=TUMBLING_HOUR,daily=TUMBLING_DAY,rolling7d=ROLLING:P7D}.
 */
public record WindowSpec(String name, Kind kind, Duration length, ZoneId zone) implements java.io.Serializable {

    public enum Kind { TUMBLING_HOUR, TUMBLING_DAY, ROLLING }

    public static List<WindowSpec> parseList(String spec, ZoneId zone) {
        List<WindowSpec> out = new ArrayList<>();
        for (String part : spec.split(",")) {
            if (part.isBlank()) {
                continue;
            }
            String[] kv = part.trim().split("=", 2);
            if (kv.length != 2) {
                throw new IllegalArgumentException("window spec must be name=KIND[:duration]: " + part);
            }
            String[] kd = kv[1].split(":", 2);
            Kind kind = Kind.valueOf(kd[0].trim().toUpperCase(Locale.ROOT));
            Duration len = switch (kind) {
                case TUMBLING_HOUR -> Duration.ofHours(1);
                case TUMBLING_DAY -> Duration.ofDays(1);
                case ROLLING -> {
                    if (kd.length < 2) {
                        throw new IllegalArgumentException("ROLLING window needs a duration: " + part);
                    }
                    yield Duration.parse(kd[1].trim());
                }
            };
            out.add(new WindowSpec(kv[0].trim(), kind, len, zone));
        }
        return List.copyOf(out);
    }

    public long lengthHours() {
        return Math.max(1, length.toHours());
    }

    /** Start of the window that contains {@code hour} (epoch-hour), as epoch-hour. */
    public long windowStartHour(long hour) {
        return switch (kind) {
            case TUMBLING_HOUR -> hour;
            case TUMBLING_DAY -> {
                ZonedDateTime z = Instant.ofEpochSecond(hour * 3600).atZone(zone);
                yield z.toLocalDate().atStartOfDay(zone).toEpochSecond() / 3600;
            }
            case ROLLING -> hour - lengthHours() + 1;
        };
    }

    /** Exclusive end (epoch-hour) of the window containing {@code hour}. */
    public long windowEndHour(long hour) {
        return switch (kind) {
            case TUMBLING_HOUR -> hour + 1;
            case TUMBLING_DAY -> {
                ZonedDateTime z = Instant.ofEpochSecond(hour * 3600).atZone(zone);
                yield z.toLocalDate().plusDays(1).atStartOfDay(zone).toEpochSecond() / 3600;
            }
            case ROLLING -> hour + 1;
        };
    }

    public boolean isRolling() {
        return kind == Kind.ROLLING;
    }
}
