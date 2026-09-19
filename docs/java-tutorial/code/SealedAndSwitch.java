// Lesson 06. Run with:  java SealedAndSwitch.java
public class SealedAndSwitch {

    // "sealed" + "permits": the COMPLETE list of types allowed to be a TrafficLight.
    // Nobody can add a fourth one elsewhere. The compiler knows the list is closed.
    sealed interface TrafficLight permits Red, Amber, Green { }

    record Red() implements TrafficLight { }
    record Amber(int secondsLeft) implements TrafficLight { }
    record Green() implements TrafficLight { }

    // switch on the TYPE of the value. Each case tests the type AND names the value.
    // No "default": the compiler checks that all three permitted types are covered.
    static String advice(TrafficLight light) {
        return switch (light) {
            case Red r -> "stop";
            case Amber(int s) -> "wait " + s + " more seconds";   // pull the field out directly
            case Green g -> "go";
        };
    }

    // The same idea in an "if": test the type and bind a variable in one step.
    static String describe(Object anything) {
        if (anything instanceof Amber a) {
            return "an amber light with " + a.secondsLeft() + "s left";
        }
        if (anything instanceof String text) {
            return "a piece of text: " + text;
        }
        return "something else: " + anything;
    }

    public static void main(String[] args) {
        TrafficLight[] lights = { new Red(), new Amber(4), new Green() };
        for (TrafficLight light : lights) {
            System.out.println(light + " -> " + advice(light));
        }
        System.out.println(describe(new Amber(9)));
        System.out.println(describe("hello"));
        System.out.println(describe(42));
    }
}
