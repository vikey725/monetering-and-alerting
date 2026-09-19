# 02. Classes and objects

## In one sentence

A class is a blueprint that says what data and behaviour something has; an object is one real
thing built from that blueprint.

## Why you need it

chargemon watches a million stations. It cannot have a million copies of the code, so it has one
class `Station` (or `EventMeta`, or `Alert`) and builds one **object** per real station, event or
alert. Chapter 02 talks about fields, constructors, `final` and immutability in its first section
without explaining them. This lesson does.

## Toy program

File: [code/ClassesAndObjects.java](code/ClassesAndObjects.java)

```java
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
```

Run it:

```bash
java ClassesAndObjects.java
```

Output:

```
ST-001 by ABB, heartbeats=2
ST-002 by Kempower, heartbeats=0
missing is null, so we do not call describe() on it
```

## Read it line by line

1. `static class Station {` declares a second class inside the first. Real code puts each class in
   its own file; we nest it here so one file runs. Ignore the `static` on a nested class; it only
   means "does not need an outer object".
2. `final String id;` is a **field**: a variable that belongs to each object. Every `Station` has
   its own `id`, `vendor` and `heartbeats`. **`final`** means the field is assigned exactly once
   and never changes afterwards. The compiler enforces it.
3. `Station(String id, String vendor) { ... }` is the **constructor**: a special method with the
   same name as the class and no return type. It runs once when an object is created. Its job is
   to fill in the fields.
4. `this.id = id;` The parameter and the field share the name `id`. **`this`** means "the object
   being built right now", so `this.id` is the field and plain `id` is the parameter. Without
   `this.` the line would assign the parameter to itself and do nothing.
5. `void recordHeartbeat() { heartbeats = heartbeats + 1; }` is a method **without `static`**. It
   works on one object's data, so it needs an object to run on. You call it as `a.recordHeartbeat()`,
   and inside the method `heartbeats` means `a`'s heartbeats.
6. `Station a = new Station("ST-001", "ABB");` **`new`** creates an object: it reserves memory,
   runs the constructor with those arguments, and hands back a reference that you store in `a`.
   The variable's type is `Station`, the class name.
7. `a.recordHeartbeat();` twice changes only `a`. `b` still has 0. That is the whole point: one
   blueprint, independent objects.
8. `Station missing = null;` **`null`** means "this variable points at no object". If you write
   `missing.describe()` Java stops with a `NullPointerException`. Most Java bugs are exactly this,
   which is why lesson 07 shows how the repo avoids `null`.

Two more words chapter 02 uses:

- **Immutable**: a type whose fields are all `final` and which has no method that changes them.
  `Station` is *not* immutable, because `heartbeats` changes. If you deleted `recordHeartbeat` and
  made `heartbeats` final, it would be. An immutable object can be handed to any number of threads
  and nobody can break it for the others. That is why chargemon events are immutable.
- **`toString`**: every object can be turned into text. If you print `a` directly you get
  something like `ClassesAndObjects$Station@1b6d3586`, which is useless. Classes override
  `toString` to print their fields. Lesson 05 shows a way to get that for free.

## Now in chargemon

[EventMeta.java](../../ocpp-model/src/main/java/com/chargemon/ocpp/model/EventMeta.java) is the
metadata attached to every event. It is a record (lesson 05), but a record is only a shortcut
for a class. Written the long way, its start would be:

```java
public final class EventMeta {
    private final String stationId;
    private final OcppVersion version;
    private final Direction direction;
    private final String uniqueId;
    private final String action;
    private final Instant receivedAt;
    private final Instant eventTime;
    private final String sourceRef;

    public EventMeta(String stationId, OcppVersion version, Direction direction, String uniqueId,
                     String action, Instant receivedAt, Instant eventTime, String sourceRef) {
        this.stationId = stationId;
        this.version = version;
        // ... six more lines like this
    }

    public String stationId() { return stationId; }
    // ... seven more accessors, plus equals, hashCode and toString
}
```

Eight `final` fields, a constructor that copies eight parameters into them with `this.`, and no
method that changes anything afterwards. That is an immutable class. Two things are new:
`private` on a field means only code inside this class may touch it directly, and `final` on the
class itself means no other class may extend it. The real file is 40 lines because the record
keyword writes all of this for you; lesson 05 shows the real file.

## Try it

Add a third field `boolean online` to `Station`, set it to `true` in the constructor, and add a
method `void goOffline()` that sets it to `false`. Print it in `describe()`. In `main`, call
`b.goOffline()` before printing.

Expected output:

```
ST-001 by ABB, heartbeats=2, online=true
ST-002 by Kempower, heartbeats=0, online=false
missing is null, so we do not call describe() on it
```

Then try making `online` `final`. The compiler refuses `goOffline()` with
`cannot assign a value to final variable online`. That is `final` doing its job.

## Check yourself

1. In `Station a = new Station("ST-001", "ABB");` which part is the class and which part is the
   object?
2. Why does the constructor write `this.id = id;` and not `id = id;`?
3. What one change would make `Station` immutable?

<details><summary>Answers</summary>

1. `Station` (both times) is the class. The object is the thing `new` creates; `a` is a variable
   that refers to it.
2. Parameter and field have the same name. `this.id` picks the field; plain `id` is the parameter.
   `id = id;` would assign the parameter to itself and leave the field empty (`null`).
3. Remove every way to change a field after construction: delete `recordHeartbeat()` and mark
   `heartbeats` as `final`.

</details>

## Next

[03. Packages, imports, enums](03-packages-imports-enums.md). In chapter 02 this lesson covers the
`static`, `final` and immutable paragraph of
[Classes, interfaces, enums, packages](../02-java-21-for-this-repo.md#classes-interfaces-enums-packages).
