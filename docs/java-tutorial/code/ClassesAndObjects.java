// Lesson 02. Run with:  java ClassesAndObjects.java
public class ClassesAndObjects {

    // A class is a blueprint. This one describes what every Station looks like.
    static class Station {
        // Fields: the data each Station carries. "final" = set once, never changed.
        final String id;
        final String vendor;
        int heartbeats;   // not final, so it can change

        // Constructor: how to build a Station. Runs when you write "new Station(...)".
        Station(String id, String vendor) {
            this.id = id;           // "this.id" = the field; "id" = the parameter
            this.vendor = vendor;
            this.heartbeats = 0;
        }

        // A method that changes this object's own data
        void recordHeartbeat() {
            heartbeats = heartbeats + 1;
        }

        // A method that only reads data
        String describe() {
            return id + " by " + vendor + ", heartbeats=" + heartbeats;
        }
    }

    public static void main(String[] args) {
        // Two objects built from one blueprint. Each has its own fields.
        Station a = new Station("ST-001", "ABB");
        Station b = new Station("ST-002", "Kempower");

        a.recordHeartbeat();
        a.recordHeartbeat();

        System.out.println(a.describe());
        System.out.println(b.describe());

        // null = "no object here". Calling a method on null crashes the program.
        Station missing = null;
        if (missing == null) {
            System.out.println("missing is null, so we do not call describe() on it");
        }
    }
}
