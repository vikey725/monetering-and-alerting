// Lesson 05. Run with:  java Records.java
import java.util.Objects;

public class Records {

    // A record: a class whose only job is to carry data.
    // Java writes the constructor, the getters, equals, hashCode and toString for you.
    record Station(String id, String vendor) { }

    // A record with a "compact constructor": the block after the name, no parameter list.
    // Use it to check or fix the values BEFORE they are stored.
    record Reading(String stationId, int connectorId, Integer watts) {
        Reading {
            Objects.requireNonNull(stationId, "stationId");
            if (connectorId < 0) {
                throw new IllegalArgumentException("connectorId must be >= 0");
            }
            if (watts == null) {
                watts = 0;    // fill in a default. You assign the PARAMETER, not a field.
            }
        }

        // Records are immutable, so "changing" one means building a copy with one difference.
        Reading withWatts(int newWatts) {
            return new Reading(stationId, connectorId, newWatts);
        }
    }

    public static void main(String[] args) {
        Station s = new Station("ST-001", "ABB");
        System.out.println(s);                 // toString for free
        System.out.println(s.id());            // accessor is id(), not getId()

        Station same = new Station("ST-001", "ABB");
        System.out.println("equal? " + s.equals(same));   // equals for free: compares fields

        Reading r = new Reading("ST-001", 2, null);
        System.out.println(r);                 // watts became 0
        Reading r2 = r.withWatts(7200);
        System.out.println(r2);
        System.out.println("original untouched: " + r);
    }
}
