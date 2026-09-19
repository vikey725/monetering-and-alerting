// Lesson 07. Run with:  java Optionals.java
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Optionals {

    static Map<String, String> vendorByStation = new HashMap<>();

    // Old style: returns null when not found. The caller can forget to check.
    static String vendorOrNull(String stationId) {
        return vendorByStation.get(stationId);
    }

    // New style: returns a box that is either full or empty. The caller MUST decide.
    static Optional<String> vendor(String stationId) {
        return Optional.ofNullable(vendorByStation.get(stationId));
    }

    public static void main(String[] args) {
        vendorByStation.put("ST-001", "ABB");

        Optional<String> found = vendor("ST-001");
        Optional<String> notFound = vendor("ST-999");

        System.out.println(found);                       // Optional[ABB]
        System.out.println(notFound);                    // Optional.empty
        System.out.println(found.isPresent());           // true
        System.out.println(notFound.isEmpty());          // true

        // orElse: the value if present, otherwise the fallback
        System.out.println(found.orElse("unknown"));
        System.out.println(notFound.orElse("unknown"));

        // map: change the value if present; an empty box stays empty
        Optional<Integer> length = found.map(v -> v.length());
        System.out.println(length);                      // Optional[3]
        System.out.println(notFound.map(v -> v.length()));   // Optional.empty

        // Read this aloud: "if vendor exists, upper-case it, otherwise 'n/a'"
        System.out.println(vendor("ST-001").map(String::toUpperCase).orElse("n/a"));
        System.out.println(vendor("ST-999").map(String::toUpperCase).orElse("n/a"));

        // The null way, for comparison. Forget the check and you get a NullPointerException.
        String v = vendorOrNull("ST-999");
        if (v != null) {
            System.out.println(v.toUpperCase());
        } else {
            System.out.println("n/a (null checked by hand)");
        }
    }
}
