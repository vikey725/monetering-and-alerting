// Lesson 01. Run with:  java FirstProgram.java
public class FirstProgram {

    // Java starts every program here. Ignore the words before "main" for now.
    public static void main(String[] args) {
        // A variable: a named box that holds one value. "int" = whole number.
        int stations = 3;
        String name = "chargemon";   // String = text
        boolean healthy = true;      // boolean = true or false

        System.out.println("Hello from " + name);   // print one line
        System.out.println("Stations: " + stations);

        // if: run the block only when the condition is true
        if (healthy) {
            System.out.println("All good");
        } else {
            System.out.println("Something is wrong");
        }

        // for: repeat. i starts at 1, runs while i <= stations, adds 1 each time
        for (int i = 1; i <= stations; i++) {
            System.out.println("Checking station " + i);
        }

        // Call our own method (below) and store what it gives back
        int seconds = minutesToSeconds(10);
        System.out.println("10 minutes = " + seconds + " seconds");
    }

    // A method: a named block of code you can call. Takes an int, gives back an int.
    static int minutesToSeconds(int minutes) {
        return minutes * 60;
    }
}
