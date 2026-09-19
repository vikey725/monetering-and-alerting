// Lesson 11. Run with:  java Lambdas.java
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Lambdas {

    // A functional interface: exactly ONE abstract method. Any such interface can be
    // implemented on the spot with a lambda instead of writing a whole class.
    @FunctionalInterface
    interface Fact {
        Optional<Object> get(String path);
    }

    public static void main(String[] args) {
        // Lambda syntax:  (parameters) -> result      One parameter needs no brackets.
        Fact empty = path -> Optional.empty();
        Fact fake = path -> path.equals("event.status") ? Optional.of("Faulted") : Optional.empty();
        System.out.println(empty.get("event.status"));
        System.out.println(fake.get("event.status"));

        // Three standard functional interfaces from java.util.function
        Function<String, Integer> length = s -> s.length();      // takes A, returns B
        Predicate<Integer> big = n -> n > 5;                      // takes A, returns boolean
        Supplier<String> now = () -> "generated value";           // takes nothing, returns T
        System.out.println(length.apply("Kempower") + " " + big.test(8) + " " + now.get());

        // Method reference: a lambda that only calls one existing method
        List<String> found = new ArrayList<>();
        List<String> source = List.of("ABB", "Kempower", "Alpitronic");
        source.forEach(found::add);            // same as  source.forEach(x -> found.add(x))
        System.out.println(found);

        // A stream: a pipeline over a collection. Read left to right.
        List<String> longNamesUpper = source.stream()
                .filter(v -> v.length() > 3)   // keep only these
                .map(String::toUpperCase)      // transform each one
                .toList();                     // collect back into a list
        System.out.println(longNamesUpper);

        // The same thing as a plain loop. This repo often prefers the loop on hot paths.
        List<String> loopVersion = new ArrayList<>();
        for (String v : source) {
            if (v.length() > 3) {
                loopVersion.add(v.toUpperCase());
            }
        }
        System.out.println(loopVersion);
    }
}
