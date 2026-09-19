// Lesson 09. Run with:  java Generics.java
import java.util.ArrayList;
import java.util.List;

public class Generics {

    // A generic class: "T" is a placeholder for a type chosen later by the user of Box.
    static class Box<T> {
        private final T content;

        Box(T content) {
            this.content = content;
        }

        T get() {
            return content;
        }
    }

    // A generic METHOD: the <T> before the return type says "this method has its own T".
    static <T> T first(List<T> items) {
        return items.get(0);
    }

    // A bounded type: S must be a Number or a subtype (Integer, Double, ...).
    static <S extends Number> double doubled(S n) {
        return n.doubleValue() * 2;
    }

    // Wildcard "?": "a list of something, I do not care what". Read-only use.
    static int count(List<?> anything) {
        return anything.size();
    }

    // "? extends Number": a list of Number or any subtype. Safe to READ numbers from it.
    static double sum(List<? extends Number> numbers) {
        double total = 0;
        for (Number n : numbers) {
            total += n.doubleValue();
        }
        return total;
    }

    public static void main(String[] args) {
        Box<String> words = new Box<>("hello");     // T = String here
        Box<Integer> number = new Box<>(42);        // T = Integer here
        String w = words.get();                      // no cast needed: compiler knows it is a String
        System.out.println(w + " / " + number.get());

        List<String> names = new ArrayList<>();      // a list OF strings only
        names.add("ABB");
        names.add("Kempower");
        // names.add(42);   <- uncomment: compile error. That is the whole point of generics.
        System.out.println("first: " + first(names));

        List<Integer> ints = List.of(1, 2, 3);
        List<Double> doubles = List.of(1.5, 2.5);
        System.out.println("doubled: " + doubled(21));
        System.out.println("count: " + count(names) + " and " + count(ints));
        System.out.println("sum ints: " + sum(ints) + ", sum doubles: " + sum(doubles));
    }
}
