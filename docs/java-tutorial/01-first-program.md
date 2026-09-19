# 01. Your first program

## In one sentence

A Java program is a text file with a class in it, and Java starts running at the method called
`main`.

## Why you need it

Every file in this repo is a class. Every class has methods. Every method has variables, `if`
statements and loops. You cannot read chapter 02 without knowing what those words mean, so this
lesson gives you the vocabulary. Nothing here is specific to chargemon.

## Toy program

File: [code/FirstProgram.java](code/FirstProgram.java)

```java
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
```

Run it:

```bash
cd docs/java-tutorial/code
java FirstProgram.java
```

Output:

```
Hello from chargemon
Stations: 3
All good
Checking station 1
Checking station 2
Checking station 3
10 minutes = 600 seconds
```

## Read it line by line

1. `// ...` is a **comment**. Java ignores it. It is there for humans.
2. `public class FirstProgram {` opens a **class**. For now, think of a class as "the file". The
   class name and the file name must match. The `{` opens a block and the matching `}` at the
   bottom closes it. Everything the class owns sits between them.
3. `public static void main(String[] args) {` is the **main method**. Java looks for exactly this
   line to know where to start. `public` means anyone may call it. `static` means it belongs to the
   class itself, not to any object (lesson 02 explains objects). `void` means it gives nothing
   back. `String[] args` is the list of words typed after the command; we ignore it.
4. `int stations = 3;` declares a **variable**. Read it right to left: take the value `3`, put it
   in a box named `stations`, and the box can only ever hold an `int` (a whole number). Every
   statement ends with `;`.
5. `String name = "chargemon";` is a box for text. Text goes in double quotes. `String` starts
   with a capital letter because it is a class, not a built-in number type. The built-in ones you
   will meet are `int`, `long` (bigger whole number), `double` (decimal), `boolean` (true/false).
6. `System.out.println(...)` prints one line. The `+` between text and a variable glues them into
   one string.
7. `if (healthy) { ... } else { ... }` runs the first block when the condition inside `( )` is
   true, otherwise the second block. Conditions you will see: `==` equal, `!=` not equal, `<`,
   `<=`, `>`, `>=`, `&&` and, `||` or, `!` not.
8. `for (int i = 1; i <= stations; i++) { ... }` is a **loop**. Three parts separated by `;`:
   start (`i = 1`), keep going while (`i <= stations`), after each round (`i++` means add 1).
9. `int seconds = minutesToSeconds(10);` **calls** a method. The value `10` goes in, the method's
   `return` value comes out and lands in `seconds`.
10. `static int minutesToSeconds(int minutes) { return minutes * 60; }` **defines** the method.
    `int` before the name is the type it gives back. `int minutes` in the brackets is the
    **parameter**, a variable that receives whatever the caller passes. `return` ends the method
    and hands the value back. It is `static` for the same reason `main` is: no object needed.

## Now in chargemon

Open
[IdsAndDurationsTest.java](../../common/src/test/java/com/chargemon/common/IdsAndDurationsTest.java)
and look at line 24:

```java
assertThat(Durations.parse("PT10M")).isEqualTo(Duration.ofMinutes(10));
```

You can now read this. `Durations.parse("PT10M")` calls a static method named `parse` on a class
named `Durations`, passing the text `"PT10M"`. `Duration.ofMinutes(10)` calls a static method
`ofMinutes` on a class `Duration`, passing `10`. `assertThat(...)` and `.isEqualTo(...)` are two
more method calls that check the two results are equal. Lesson 13 explains where they come from.

The method being called lives in
[Durations.java](../../common/src/main/java/com/chargemon/common/time/Durations.java):

```java
public static Duration parse(String iso) {
    try {
        return Duration.parse(iso.trim());
    } catch (DateTimeParseException e) {
        throw new IllegalArgumentException("Invalid ISO-8601 duration: '" + iso + "'", e);
    }
}
```

Same shape as `minutesToSeconds`: `public static`, a return type (`Duration`), a name, one
parameter (`String iso`), a body with `return`. The `try`/`catch` part is lesson 08.

## Try it

Change the `for` loop so it counts down from `stations` to 1, and make the method return seconds
as a `long` instead of an `int`. Run it again.

Expected: the three "Checking station" lines appear in reverse order and the last line is
unchanged. If you forgot to change the type of the `seconds` variable to `long` as well, the
compiler stops you with `incompatible types: possible lossy conversion from long to int`. That is
Java protecting you: a `long` might not fit in an `int`.

Hint: counting down is `for (int i = stations; i >= 1; i--)`.

## Check yourself

1. Which line does Java run first, and how does it know?
2. What is the difference between `int minutes` inside `static int minutesToSeconds(int minutes)`
   and `int seconds` inside `main`?
3. What does `static` mean on a method?

<details><summary>Answers</summary>

1. The first line inside `public static void main(String[] args)`. Java always looks for a method
   with exactly that signature.
2. Both are variables of type `int`. `minutes` is a parameter: it is filled in by the caller each
   time the method runs. `seconds` is a local variable: it is filled in by the code that declares
   it.
3. The method belongs to the class, not to an object. You call it as `ClassName.method(...)`
   without creating anything first. Lesson 02 shows the non-static kind.

</details>

## Next

[02. Classes and objects](02-classes-and-objects.md). In chapter 02 this lesson corresponds to the
first paragraph of
[Classes, interfaces, enums, packages](../02-java-21-for-this-repo.md#classes-interfaces-enums-packages).
