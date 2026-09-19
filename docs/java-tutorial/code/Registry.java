// Lesson 12, part 1. Run with:  java Registry.java
import java.util.LinkedHashMap;
import java.util.Map;

public class Registry {

    // The "plugin" contract: an operator has a name and does one comparison
    interface Operator {
        String name();
        boolean test(int left, int right);
    }

    static class Eq implements Operator {
        public String name() { return "eq"; }
        public boolean test(int l, int r) { return l == r; }
    }

    static class Gt implements Operator {
        public String name() { return "gt"; }
        public boolean test(int l, int r) { return l > r; }
    }

    public static void main(String[] args) {
        // Hand-rolled registry: a map from name to plugin.
        // Problem: to add an operator you must EDIT THIS FILE and add a line here.
        Map<String, Operator> byName = new LinkedHashMap<>();
        byName.put("eq", new Eq());
        byName.put("gt", new Gt());

        // A "rule" arrives as text, and we look the operator up by name at run time
        String[][] rules = { { "eq", "3", "3" }, { "gt", "2", "9" }, { "lt", "1", "2" } };
        for (String[] rule : rules) {
            Operator op = byName.get(rule[0]);
            if (op == null) {
                System.out.println(rule[0] + ": unknown operator");
                continue;
            }
            boolean result = op.test(Integer.parseInt(rule[1]), Integer.parseInt(rule[2]));
            System.out.println(rule[0] + "(" + rule[1] + ", " + rule[2] + ") = " + result);
        }
        System.out.println("Lesson 12 part 2 removes the put(...) lines with ServiceLoader.");
    }
}
