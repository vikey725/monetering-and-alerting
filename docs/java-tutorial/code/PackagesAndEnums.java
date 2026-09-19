// Lesson 03. Run with:  java PackagesAndEnums.java
import java.util.Locale;    // pull in a class from another package
import java.util.Objects;

public class PackagesAndEnums {

    // An enum: a type with a fixed list of allowed values. Nothing else is possible.
    enum Direction {
        STATION_TO_CSMS,
        CSMS_TO_STATION;

        // Enums can have methods too
        Direction opposite() {
            return this == STATION_TO_CSMS ? CSMS_TO_STATION : STATION_TO_CSMS;
        }
    }

    // Turn free text into a Direction. Throws (crashes on purpose) if the text is unknown.
    static Direction parse(String raw) {
        Objects.requireNonNull(raw, "raw");   // crash early with a clear message if null
        String s = raw.trim().toUpperCase(Locale.ROOT);
        return switch (s) {
            case "IN", "INBOUND" -> Direction.STATION_TO_CSMS;
            case "OUT", "OUTBOUND" -> Direction.CSMS_TO_STATION;
            default -> throw new IllegalArgumentException("Unsupported direction: " + raw);
        };
    }

    public static void main(String[] args) {
        Direction d = parse("  inbound ");
        System.out.println("parsed: " + d);
        System.out.println("opposite: " + d.opposite());

        // switch over an enum: the compiler knows both values, so no "default" is needed
        String who = switch (d) {
            case STATION_TO_CSMS -> "the station sent it";
            case CSMS_TO_STATION -> "the backend sent it";
        };
        System.out.println(who);

        // Every enum value has a name() and a position
        for (Direction each : Direction.values()) {
            System.out.println(each.name() + " is number " + each.ordinal());
        }
    }
}
