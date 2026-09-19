# 08. Exceptions

## In one sentence

An exception is Java's way of saying "something went wrong, stop here and jump to whoever is
prepared to deal with it".

## Why you need it

You have already seen `throw new IllegalArgumentException(...)` in lessons 03 and 05, and
`try`/`catch` in `Durations.parse`. Chapter 02 draws a line between *checked* and *unchecked*
exceptions and then says the repo avoids exceptions on hot paths. To understand that decision you
need to know what throwing actually does and what it costs.

## Toy program

File: [code/Exceptions.java](code/Exceptions.java)

```java
// Lesson 08. Run with:  java Exceptions.java
import java.io.IOException;
import java.time.Duration;
import java.time.format.DateTimeParseException;

public class Exceptions {

    // Throws an UNCHECKED exception (IllegalArgumentException). No "throws" needed in the header.
    static Duration parse(String iso) {
        try {
            return Duration.parse(iso.trim());
        } catch (DateTimeParseException e) {
            // Wrap the library's error in our own, with a clearer message
            throw new IllegalArgumentException("Invalid ISO-8601 duration: '" + iso + "'", e);
        }
    }

    // Throws a CHECKED exception (IOException). The header MUST say "throws IOException",
    // and every caller must either catch it or declare it too.
    static String readConfig(boolean exists) throws IOException {
        if (!exists) {
            throw new IOException("config file not found");
        }
        return "timeout=30s";
    }

    public static void main(String[] args) {
        System.out.println(parse("PT10M"));   // works: 10 minutes

        try {
            parse("10 minutes");              // throws
            System.out.println("this line never runs");
        } catch (IllegalArgumentException e) {
            System.out.println("caught: " + e.getMessage());
        } finally {
            System.out.println("finally always runs");
        }

        // The compiler forces us to handle the checked one
        try {
            System.out.println(readConfig(true));
            System.out.println(readConfig(false));
        } catch (IOException e) {
            System.out.println("caught checked: " + e.getMessage());
        }

        // Throwing is slow: every exception records the whole call stack. Count the frames.
        Exception probe = new Exception("probe");
        System.out.println("stack frames captured: " + probe.getStackTrace().length);
    }
}
```

Run it:

```bash
java Exceptions.java
```

Output:

```
PT10M
caught: Invalid ISO-8601 duration: '10 minutes'
finally always runs
timeout=30s
caught checked: config file not found
stack frames captured: 6
```

## Read it line by line

1. `throw new IllegalArgumentException("...", e);` **throws**: it creates an exception object and
   abandons the current method immediately. Execution jumps up the chain of callers until one of
   them has a matching `catch`. If none does, the program prints the stack trace and dies. The
   second argument `e` attaches the original error as the *cause*, so nothing is lost.
2. `try { ... } catch (DateTimeParseException e) { ... }` **guards** a block. If anything inside
   `try` throws a `DateTimeParseException` (or a subtype), execution jumps to the `catch` block
   with the exception in `e`. Other exception types pass through untouched. Here `parse` catches
   the library's exception and rethrows a clearer one of its own.
3. `finally { ... }` runs **always**: after the `try` finished normally, or after a `catch`, or
   even while an uncaught exception is flying past. It is for cleanup such as closing files.
4. `"this line never runs"`: once `parse("10 minutes")` throws, the rest of the `try` block is
   skipped. That is what "stop here" means.
5. `static String readConfig(boolean exists) throws IOException` declares a **checked**
   exception. Java has two families:
   - **Unchecked**: `RuntimeException` and everything that extends it, for example
     `IllegalArgumentException`, `NullPointerException`, `IllegalStateException`. You may throw
     them anywhere without announcing it. They mean "programmer error or bad input".
   - **Checked**: everything else that extends `Exception`, for example `IOException`,
     `SQLException`. A method that can throw one *must* say so with `throws` in its header, and
     every caller *must* either `catch` it or add `throws` to its own header. Delete the
     `try`/`catch` around `readConfig` and the compiler says
     `unreported exception IOException; must be caught or declared to be thrown`. They mean
     "something outside your control failed, and you must plan for it".
6. `probe.getStackTrace().length` shows the hidden cost. Creating an exception records every
   method currently on the call stack, here 6 frames. Inside a Flink operator the stack is
   hundreds of frames deep, and doing that for every bad message among millions per second is
   real work. This is why chapter 02 says throwing is "expensive" and why lesson 10 exists.

Words you will meet: **stack trace** is the printed list of frames; **unwinding** is the jump
from the throw up to the catch; **swallowing** an exception is catching it and doing nothing,
which hides bugs and the repo avoids.

## Now in chargemon

[Durations.java](../../common/src/main/java/com/chargemon/common/time/Durations.java) is the toy's
`parse`, line for line:

```java
public static Duration parse(String iso) {
    try {
        return Duration.parse(iso.trim());
    } catch (DateTimeParseException e) {
        throw new IllegalArgumentException("Invalid ISO-8601 duration: '" + iso + "'", e);
    }
}
```

It throws unchecked because a bad duration string in a rule definition is a configuration
mistake: the right response is to fail loudly, not to carry on. The test at line 26 of
[IdsAndDurationsTest.java](../../common/src/test/java/com/chargemon/common/IdsAndDurationsTest.java)
checks exactly that:

```java
assertThatThrownBy(() -> Durations.parse("10 minutes")).isInstanceOf(IllegalArgumentException.class);
```

"Running this code must throw, and what it throws must be an `IllegalArgumentException`."
`() -> ...` is a lambda with no parameters (lesson 11); `assertThatThrownBy` is AssertJ (lesson 13).

For contrast, the envelope parser in
[EnvelopeParser.java](../../ocpp-codec/src/main/java/com/chargemon/ocpp/codec/envelope/EnvelopeParser.java)
runs on the hot path and returns `Result<RawEnvelope, String>` instead of throwing. A malformed
message is not a programmer error and not rare, so building a stack trace for each one would be
waste. Lesson 10 builds that `Result` type from scratch.

## Try it

Change `main` so the call `parse("10 minutes")` is *not* inside a `try`. Run it and read the
output carefully.

Expected: the program prints `PT10M`, then dies with a stack trace that starts

```
Exception in thread "main" java.lang.IllegalArgumentException: Invalid ISO-8601 duration: '10 minutes'
```

followed by `at Exceptions.parse(Exceptions.java:...)` and `at Exceptions.main(...)`, then
`Caused by: java.time.format.DateTimeParseException`. The `Caused by` is the `e` we attached.
Nothing after the throw ran.

Then put the `try` back, and instead remove `throws IOException` from `readConfig`. Expected:
compile error `unreported exception IOException; must be caught or declared to be thrown` at the
`throw new IOException` line. That is the checked rule enforced inside the method itself.

Hint: for the second part the error is at the `throw`, not at the caller, because the method
itself now throws something it did not declare.

## Check yourself

1. What is the difference between a checked and an unchecked exception, in one sentence each?
2. What does `finally` guarantee?
3. Why does the repo throw in `Durations.parse` but return `Result` in `EnvelopeParser.parse`?

<details><summary>Answers</summary>

1. Checked: extends `Exception` but not `RuntimeException`; must be declared with `throws` or
   caught, or the code does not compile. Unchecked: extends `RuntimeException`; may be thrown
   anywhere without declaration.
2. Its block runs no matter how the `try` ends: normal completion, caught exception, or an
   exception that keeps flying.
3. A bad duration is a rare configuration error, so fail loudly and the stack-trace cost does not
   matter. A bad envelope happens constantly on the hot path, so a cheap value that says "error"
   is better than an exception per message.

</details>

## Next

[09. Generics](09-generics.md). In chapter 02 this lesson covers
[Checked versus unchecked exceptions, and the Result alternative](../02-java-21-for-this-repo.md#checked-versus-unchecked-exceptions-and-the-result-alternative).
