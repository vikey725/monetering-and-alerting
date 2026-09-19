// Lesson 08. Run with:  java Exceptions.java
import java.io.IOException;
import java.time.Duration;
import java.time.format.DateTimeParseException;

public class Exceptions {

    // Throws an UNCHECKED exception (IllegalArgumentException). No "throws" needed in the header.
    static Duration parse(String iso) {
        try {
            return Duration.parse(iso.trim());
        } catch (DateTimeParseException e) {
            // Wrap the library's error in our own, with a clearer message
            throw new IllegalArgumentException("Invalid ISO-8601 duration: '" + iso + "'", e);
        }
    }

    // Throws a CHECKED exception (IOException). The header MUST say "throws IOException",
    // and every caller must either catch it or declare it too.
    static String readConfig(boolean exists) throws IOException {
        if (!exists) {
            throw new IOException("config file not found");
        }
        return "timeout=30s";
    }

    public static void main(String[] args) {
        System.out.println(parse("PT10M"));   // works: 10 minutes

        try {
            parse("10 minutes");              // throws
            System.out.println("this line never runs");
        } catch (IllegalArgumentException e) {
            System.out.println("caught: " + e.getMessage());
        } finally {
            System.out.println("finally always runs");
        }

        // The compiler forces us to handle the checked one
        try {
            System.out.println(readConfig(true));
            System.out.println(readConfig(false));
        } catch (IOException e) {
            System.out.println("caught checked: " + e.getMessage());
        }

        // Throwing is slow: every exception records the whole call stack. Count the frames.
        Exception probe = new Exception("probe");
        System.out.println("stack frames captured: " + probe.getStackTrace().length);
    }
}
