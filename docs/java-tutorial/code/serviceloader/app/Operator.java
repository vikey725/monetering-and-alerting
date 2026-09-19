package app;

// The contract. Plugins implement this. Lives in the "app".
public interface Operator {
    String name();
    boolean test(int left, int right);
}
