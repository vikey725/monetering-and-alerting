// Lesson 04. Run with:  java Interfaces.java
public class Interfaces {

    // An interface is a promise: "anything that is a Notifier can send(msg)".
    // It says WHAT, not HOW. No method bodies (except "default" ones).
    interface Notifier {
        void send(String message);

        // A default method has a body. Every implementation gets it for free.
        default void sendTwice(String message) {
            send(message);
            send(message);
        }
    }

    // Two different HOWs. Each class "implements" the promise in its own way.
    static class ConsoleNotifier implements Notifier {
        public void send(String message) {
            System.out.println("[console] " + message);
        }
    }

    static class ShoutingNotifier implements Notifier {
        public void send(String message) {
            System.out.println("[shout] " + message.toUpperCase() + "!!!");
        }
    }

    // This method does not care which implementation it gets. It only knows the promise.
    static void alert(Notifier n, String what) {
        n.send("ALERT: " + what);
    }

    public static void main(String[] args) {
        Notifier quiet = new ConsoleNotifier();   // variable type = interface, object = class
        Notifier loud = new ShoutingNotifier();

        alert(quiet, "connector 2 faulted");
        alert(loud, "connector 2 faulted");

        quiet.sendTwice("heartbeat missing");     // default method, never written in ConsoleNotifier
    }
}
