# 05. Records

## In one sentence

A record is a class whose only job is to carry data, and Java writes the boring parts
(constructor, getters, `equals`, `toString`) for you.

## Why you need it

Almost every data type in chargemon is a record: `EventMeta`, every event, every rule spec, every
alert. Chapter 02's "Records" section talks about canonical and compact constructors and `withX`
methods. After this lesson those are three small things, not three mysteries.

## Toy program

File: [code/Records.java](code/Records.java)

```java
// Lesson 05. Run with:  java Records.java
import java.util.Objects;

public class Records {

    // A record: a class whose only job is to carry data.
    // Java writes the constructor, the getters, equals, hashCode and toString for you.
    record Station(String id, String vendor) { }

    // A record with a "compact constructor": the block after the name, no parameter list.
    // Use it to check or fix the values BEFORE they are stored.
    record Reading(String stationId, int connectorId, Integer watts) {
        Reading {
            Objects.requireNonNull(stationId, "stationId");
            if (connectorId < 0) {
                throw new IllegalArgumentException("connectorId must be >= 0");
            }
            if (watts == null) {
                watts = 0;    // fill in a default. You assign the PARAMETER, not a field.
            }
        }

        // Records are immutable, so "changing" one means building a copy with one difference.
        Reading withWatts(int newWatts) {
            return new Reading(stationId, connectorId, newWatts);
        }
    }

    public static void main(String[] args) {
        Station s = new Station("ST-001", "ABB");
        System.out.println(s);                 // toString for free
        System.out.println(s.id());            // accessor is id(), not getId()

        Station same = new Station("ST-001", "ABB");
        System.out.println("equal? " + s.equals(same));   // equals for free: compares fields

        Reading r = new Reading("ST-001", 2, null);
        System.out.println(r);                 // watts became 0
        Reading r2 = r.withWatts(7200);
        System.out.println(r2);
        System.out.println("original untouched: " + r);
    }
}
```

Run it:

```bash
java Records.java
```

Output:

```
Station[id=ST-001, vendor=ABB]
ST-001
equal? true
Reading[stationId=ST-001, connectorId=2, watts=0]
Reading[stationId=ST-001, connectorId=2, watts=7200]
original untouched: Reading[stationId=ST-001, connectorId=2, watts=0]
```

## Read it line by line

1. `record Station(String id, String vendor) { }` is a complete class. The parts in brackets are
   the **components**. From this one line Java generates:
   - two `private final` fields `id` and `vendor`;
   - a constructor `Station(String id, String vendor)` that stores them (the **canonical
     constructor**);
   - an **accessor** per component, named exactly like it: `id()` and `vendor()`. Not `getId()`.
     This is the naming you see all over the repo: `meta.stationId()`, `call.action()`;
   - `equals` and `hashCode` that compare all components, so two records with the same values
     are equal;
   - `toString` that prints `Station[id=ST-001, vendor=ABB]`.

   Compare with the 25-line `Station` class in lesson 02. Same thing, one line. And it is
   immutable by construction: the fields are `final` and there are no setters.
2. `record Reading(String stationId, int connectorId, Integer watts) { Reading { ... } ... }` has
   a **compact constructor**: the record name followed directly by `{`, with *no* parameter list.
   Its body runs *before* the components are stored. It sees the parameters by their component
   names.
3. `Objects.requireNonNull(stationId, "stationId");` rejects a null station id. Lesson 03.
4. `watts = 0;` inside the compact constructor assigns the **parameter**, not a field. After the
   block ends, Java copies the (possibly changed) parameters into the fields. So this line means
   "if the caller passed null, store 0 instead". `Integer` (capital I) is the object version of
   `int` that can be `null`; plain `int` cannot.
5. `Reading withWatts(int newWatts) { return new Reading(stationId, connectorId, newWatts); }`
   Records cannot change, so to "change" one you build a new one with all the same values except
   one. By convention such methods are named `withX`. The original is untouched, which the last
   output line proves.
6. `s.equals(same)` is `true` because records compare by value, not by "is it the same object in
   memory". A lesson-02 class would say `false` here unless you wrote `equals` yourself.

## Now in chargemon

[EventMeta.java](../../ocpp-model/src/main/java/com/chargemon/ocpp/model/EventMeta.java), whole
file minus the comment:

```java
public record EventMeta(
        String stationId,
        OcppVersion version,
        Direction direction,
        String uniqueId,
        String action,
        Instant receivedAt,
        Instant eventTime,
        String sourceRef) {

    public EventMeta {
        Objects.requireNonNull(stationId, "stationId");
        Objects.requireNonNull(version, "version");
        Objects.requireNonNull(direction, "direction");
        Objects.requireNonNull(receivedAt, "receivedAt");
        if (eventTime == null) {
            eventTime = receivedAt;
        }
    }

    public EventMeta withAction(String newAction) {
        return new EventMeta(stationId, version, direction, uniqueId, newAction, receivedAt, eventTime, sourceRef);
    }

    public EventMeta withEventTime(Instant newEventTime) {
        return new EventMeta(stationId, version, direction, uniqueId, action, receivedAt, newEventTime, sourceRef);
    }
}
```

Match it to the toy:

| Toy `Reading` | Repo `EventMeta` |
|---|---|
| 3 components | 8 components, one per line for readability |
| `Reading { ... }` compact constructor | `public EventMeta { ... }` compact constructor |
| `requireNonNull(stationId)` | four `requireNonNull` calls for the required fields |
| `if (watts == null) watts = 0;` | `if (eventTime == null) eventTime = receivedAt;` |
| `withWatts` copies with one change | `withAction`, `withEventTime` copy with one change |

`Instant` is a point in time from `java.time`. `OcppVersion` and `Direction` are the enums from
lesson 03.

The smallest record in the repo is a whole event:

```java
public record Heartbeat(EventMeta meta) implements StationMessage { }
```

One component, `meta`, so Java generates `meta()`, and that single accessor is what fulfils the
`EventMeta meta();` promise of the `OcppEvent` interface from lesson 04. Nothing else needed.

A compact constructor is also used to make a list safe in
[RuleSpec.java](../../rule-engine/src/main/java/com/chargemon/rules/definition/RuleSpec.java):

```java
record SequenceSpec(List<String> allOf, Duration within) implements RuleSpec {
    public SequenceSpec {
        allOf = allOf == null ? List.of() : List.copyOf(allOf);
    }
```

If the caller passes `null`, store an empty list. Otherwise store an unmodifiable *copy*, so the
caller cannot change the record's list afterwards by changing their own. `List<String>` is
lesson 09.

## Try it

Add a `withVendor(String newVendor)` method to `Station` and print `s.withVendor("Kempower")` and
then `s` again.

Expected:

```
Station[id=ST-001, vendor=Kempower]
Station[id=ST-001, vendor=ABB]
```

Then try to write `s.vendor = "X";` in `main`. The compiler answers
`cannot assign a value to final variable vendor`. There is no way in: that is immutability.

Hint: look at `withWatts`; yours is one line shorter.

## Check yourself

1. Name four things Java generates from `record Station(String id, String vendor)`.
2. In a compact constructor, what does `eventTime = receivedAt;` assign to?
3. If records cannot change, how does `withAction` "change" the action?

<details><summary>Answers</summary>

1. The canonical constructor, the accessors `id()` and `vendor()`, `equals`/`hashCode`, and
   `toString`. Also the two private final fields.
2. The parameter `eventTime`. The fields do not exist yet; they are filled from the parameters
   after the compact constructor body finishes.
3. It does not. It builds a brand-new `EventMeta` with the same seven values and the new action,
   and returns that. The original is unchanged.

</details>

## Next

[06. Sealed interfaces and switch](06-sealed-and-switch.md). In chapter 02 this lesson covers
[Records](../02-java-21-for-this-repo.md#records) and
[A record with a compact constructor: EventMeta](../02-java-21-for-this-repo.md#a-record-with-a-compact-constructor-eventmeta).
