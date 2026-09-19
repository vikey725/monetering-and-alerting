# 03. Packages, imports, enums

## In one sentence

A package is the folder a class lives in, `import` lets you use a class from another folder, and
an enum is a type with a fixed list of allowed values.

## Why you need it

The first line of every repo file is `package com.chargemon.something;` and the next ten are
`import` lines. You have to know what they are so you can skip them with confidence. Enums appear
everywhere in chargemon: `Direction`, `OcppVersion`, `RuleKind`, `AlertState`. Chapter 02 mentions
them in one sentence.

## Toy program

File: [code/PackagesAndEnums.java](code/PackagesAndEnums.java)

```java
// Lesson 03. Run with:  java PackagesAndEnums.java
import java.util.Locale;    // pull in a class from another package
import java.util.Objects;

public class PackagesAndEnums {

    // An enum: a type with a fixed list of allowed values. Nothing else is possible.
    enum Direction {
        STATION_TO_CSMS,
        CSMS_TO_STATION;

        // Enums can have methods too
        Direction opposite() {
            return this == STATION_TO_CSMS ? CSMS_TO_STATION : STATION_TO_CSMS;
        }
    }

    // Turn free text into a Direction. Throws (crashes on purpose) if the text is unknown.
    static Direction parse(String raw) {
        Objects.requireNonNull(raw, "raw");   // crash early with a clear message if null
        String s = raw.trim().toUpperCase(Locale.ROOT);
        return switch (s) {
            case "IN", "INBOUND" -> Direction.STATION_TO_CSMS;
            case "OUT", "OUTBOUND" -> Direction.CSMS_TO_STATION;
            default -> throw new IllegalArgumentException("Unsupported direction: " + raw);
        };
    }

    public static void main(String[] args) {
        Direction d = parse("  inbound ");
        System.out.println("parsed: " + d);
        System.out.println("opposite: " + d.opposite());

        // switch over an enum: the compiler knows both values, so no "default" is needed
        String who = switch (d) {
            case STATION_TO_CSMS -> "the station sent it";
            case CSMS_TO_STATION -> "the backend sent it";
        };
        System.out.println(who);

        // Every enum value has a name() and a position
        for (Direction each : Direction.values()) {
            System.out.println(each.name() + " is number " + each.ordinal());
        }
    }
}
```

Run it:

```bash
java PackagesAndEnums.java
```

Output:

```
parsed: STATION_TO_CSMS
opposite: CSMS_TO_STATION
the station sent it
STATION_TO_CSMS is number 0
CSMS_TO_STATION is number 1
```

## Read it line by line

1. `import java.util.Locale;` says "when I write `Locale` in this file, I mean the class
   `Locale` in the package `java.util`". A **package** is a named folder of classes. The full
   name `java.util.Locale` is the **fully qualified name**. Without the import you would have to
   write the full name every time. Classes in `java.lang` (`String`, `System`, `Integer`) need no
   import.
2. Our toy has no `package` line, so it lives in the nameless default package. Every repo file
   starts with one, for example `package com.chargemon.ocpp.model;`, and the file sits in the
   matching folder `com/chargemon/ocpp/model/`. Package names are just reversed domain names
   plus a path; there is nothing more to them.
3. `enum Direction { STATION_TO_CSMS, CSMS_TO_STATION; ... }` declares an **enum**. It is a class
   whose only possible objects are the ones listed, by convention in capitals. You cannot write
   `new Direction()`. You refer to a value as `Direction.STATION_TO_CSMS`.
4. `Direction opposite() { return this == STATION_TO_CSMS ? ... : ...; }` shows an enum can have
   methods. `this` is the value the method was called on. `a ? b : c` is a one-line `if`: if `a`
   then `b` else `c`. Inside the enum you may drop the `Direction.` prefix.
5. `Objects.requireNonNull(raw, "raw");` is a helper from `java.util.Objects`. If `raw` is `null`
   it throws a `NullPointerException` with the message `raw`; otherwise it does nothing. It turns
   a confusing crash somewhere later into a clear crash right here. The repo uses it in almost
   every constructor.
6. `raw.trim().toUpperCase(Locale.ROOT)` chains two method calls on a `String`. `trim()` removes
   spaces at both ends, `toUpperCase(Locale.ROOT)` upper-cases in a language-independent way.
7. `return switch (s) { case "IN", "INBOUND" -> ...; default -> throw ...; };` is a **switch
   expression**: it *produces a value*, which we return. Each `case` lists one or more matching
   values, then `->`, then the result. A `String` can match anything, so a `default` branch is
   required; here it throws (lesson 08).
8. `String who = switch (d) { case STATION_TO_CSMS -> ...; case CSMS_TO_STATION -> ...; };`
   switches over an enum. The compiler knows the enum has exactly two values, both are listed,
   so no `default` is needed. If you later add a third value to the enum, this switch stops
   compiling until you handle it. Remember this; lesson 06 makes it the main idea.
9. `Direction.values()` gives all values in order; `name()` is the text, `ordinal()` the position.

## Now in chargemon

[Direction.java](../../ocpp-model/src/main/java/com/chargemon/ocpp/model/Direction.java) is
almost exactly the toy:

```java
package com.chargemon.ocpp.model;

import java.util.Locale;

public enum Direction {
    STATION_TO_CSMS,
    CSMS_TO_STATION;

    public static Direction parse(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("direction is null");
        }
        String s = raw.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        return switch (s) {
            case "STATION_TO_CSMS", "FROM_STATION", "INBOUND", "IN", "CP_TO_CS", "STATION" -> STATION_TO_CSMS;
            case "CSMS_TO_STATION", "TO_STATION", "OUTBOUND", "OUT", "CS_TO_CP", "CSMS" -> CSMS_TO_STATION;
            default -> throw new IllegalArgumentException("Unsupported direction: " + raw);
        };
    }

    public Direction opposite() {
        return this == STATION_TO_CSMS ? CSMS_TO_STATION : STATION_TO_CSMS;
    }
}
```

Line 1 is the package. The file lives at `ocpp-model/src/main/java/com/chargemon/ocpp/model/`,
which is the package name with dots turned into slashes. `parse` is `static` because you call it
as `Direction.parse("in")` before you have any `Direction`.

[OcppVersion.java](../../ocpp-model/src/main/java/com/chargemon/ocpp/model/OcppVersion.java) shows
one more enum trick: each value carries data.

```java
public enum OcppVersion {
    V16("1.6"),
    V201("2.0.1");

    private final String wire;

    OcppVersion(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }
```

`V16("1.6")` calls the enum's constructor with `"1.6"`, which is stored in the `final` field
`wire`. So `OcppVersion.V16.wire()` returns `"1.6"`. Same constructor and `this.` pattern as
lesson 02, just inside an enum.

## Try it

Add a third value `UNKNOWN` to the toy enum. Run it.

Expected: the compiler refuses with

```
error: the switch expression does not cover all possible input values
```

pointing at the `switch (d)` in `main`. Add `case UNKNOWN -> "nobody knows";` to that switch and it
compiles again. Also decide what `opposite()` should return for `UNKNOWN`; the current line
happens to return `STATION_TO_CSMS`, which is probably wrong.

Hint: the `parse` switch is over a `String`, so it does not care about the new value. Only the
switch over the enum does.

## Check yourself

1. Where on disk does a file that starts with `package com.chargemon.rules.eval;` live, relative
   to `src/main/java`?
2. Why does the switch over `String` need a `default` while the switch over `Direction` does not?
3. What does `Objects.requireNonNull(x, "x")` do when `x` is not null?

<details><summary>Answers</summary>

1. `com/chargemon/rules/eval/`. Dots become directory separators.
2. There are infinitely many possible strings, so the compiler cannot know they are all covered.
   An enum has a fixed, known list of values, so the compiler can check every one is present.
3. Nothing. It returns `x` unchanged. It only throws when `x` is null.

</details>

## Next

[04. Interfaces](04-interfaces.md). In chapter 02 this lesson covers the package and enum
sentences of
[Classes, interfaces, enums, packages](../02-java-21-for-this-repo.md#classes-interfaces-enums-packages).
