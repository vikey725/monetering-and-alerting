package app;

import java.util.ServiceLoader;

// The app. It never names a single plugin class. It asks ServiceLoader to find them.
public class Main {
    public static void main(String[] args) {
        System.out.println("Operators found on the classpath:");
        for (Operator op : ServiceLoader.load(Operator.class)) {
            System.out.println("  " + op.name() + " -> " + op.getClass().getName()
                    + "   test(3, 3) = " + op.test(3, 3));
        }
    }
}
