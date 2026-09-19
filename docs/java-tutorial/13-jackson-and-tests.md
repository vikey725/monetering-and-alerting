# 13. Jackson and tests

## In one sentence

Jackson turns JSON text into Java objects and back; JUnit runs test methods; AssertJ gives those
methods readable checks.

## Why you need it

Every OCPP message arrives as JSON, every alert leaves as JSON, and every claim in these lessons
is checked by a test. Chapter 02 covers Jackson and the three test libraries in two short
sections and assumes you have seen an annotation before. Neither Jackson nor the test libraries
are part of the JDK, so this lesson has no single-file toy. Instead you read a JSON document and
the record it becomes, then run a real repo test.

## Part 1: Jackson

### What an annotation is

You have seen `@FunctionalInterface` (lesson 11) and `@Override` in `RawFrame`. An
**annotation** is a label starting with `@` that you attach to a class, method or field. It does
nothing by itself. Some other code, the compiler or a library, reads the label and changes its
behaviour. Jackson's annotations tell Jackson how to map JSON to your types.

### From a record to JSON and back

Take lesson 05's record and imagine Jackson is on the classpath:

```java
record Station(String id, String vendor) { }
```

Writing an object out (**serialization**):

```java
ObjectMapper mapper = new ObjectMapper();
String json = mapper.writeValueAsString(new Station("ST-001", "ABB"));
// json is now:  {"id":"ST-001","vendor":"ABB"}
```

Reading it back (**deserialization**):

```java
Station s = mapper.readValue("{\"id\":\"ST-001\",\"vendor\":\"ABB\"}", Station.class);
// s is now:  Station[id=ST-001, vendor=ABB]
```

`ObjectMapper` is the one class you talk to. `writeValueAsString` walks the record's accessors
(`id()`, `vendor()`) and writes one JSON field per component. `readValue` does the reverse: it
reads the JSON fields and calls the canonical constructor with them, matching by name. That name
matching only works if the compiler kept the parameter names in the class file, which is what
chapter 02 means by "the build passes `-parameters`". You can see that flag at line 29 of
[chargemon.java-library.gradle.kts](../../buildSrc/src/main/kotlin/chargemon.java-library.gradle.kts).

### When you do not know the shape: `JsonNode`

Sometimes the JSON has no fixed record, for example a raw OCPP payload whose fields depend on the
action. Then you read it into a **`JsonNode`**, a generic tree:

```java
JsonNode root = mapper.readTree("{\"status\":\"Faulted\",\"connectorId\":2}");
String status = root.get("status").asText();     // "Faulted"
int connector = root.get("connectorId").asInt(); // 2
JsonNode missing = root.get("nope");             // null: the field is absent
```

`get(name)` returns the child node or `null`; `asText()`, `asInt()`, `asBoolean()` convert it;
`isArray()`, `isObject()`, `isTextual()` ask what kind it is. This is exactly what
`FieldPathResolver.node` (lesson 06) walks through, one path segment at a time.

### Sealed interfaces need a type tag

Here is the problem: given `{"meta":{...}}`, Jackson cannot know whether to build a `Heartbeat`
or a `BootNotification`; both have a `meta`. So the JSON carries a **type tag**, and two
annotations on the interface teach Jackson to read it:

```java
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = BootNotification.class, name = "BootNotification"),
    @JsonSubTypes.Type(value = Heartbeat.class, name = "Heartbeat"),
    // ... one line per permitted record
})
public sealed interface OcppEvent permits StationMessage, CorrelatedEvent, GenericOcppEvent {
```

`@JsonTypeInfo` says "the property named `type` holds a name". `@JsonSubTypes` maps each name to
a class. So `{"type":"Heartbeat","meta":{...}}` becomes a `Heartbeat`, and writing a `Heartbeat`
out adds `"type":"Heartbeat"`. Chapter 02 notes the tag doubles as the action name rules filter
on. `RawFrame` (lesson 06) does the same with a property named `kind`.

## Now in chargemon: Jackson

[JsonMapperFactory.java](../../common/src/main/java/com/chargemon/common/json/JsonMapperFactory.java)
builds the one `ObjectMapper` every module shares:

```java
private static JsonMapper.Builder configure(JsonMapper.Builder builder) {
    return builder
            .addModule(new JavaTimeModule())
            .addModule(new Jdk8Module())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .serializationInclusion(JsonInclude.Include.NON_NULL);
}
```

Each line, in plain words:

| Line | Meaning |
|---|---|
| `addModule(new JavaTimeModule())` | teach Jackson `Instant` and `Duration`, which it does not know by default |
| `addModule(new Jdk8Module())` | teach it `Optional` (lesson 07) |
| `disable(WRITE_DATES_AS_TIMESTAMPS)` | write times as `"2026-01-01T00:00:00Z"`, not as a number of milliseconds |
| `disable(FAIL_ON_UNKNOWN_PROPERTIES)` | if the JSON has a field the record does not, ignore it instead of failing |
| `disable(ADJUST_DATES_TO_CONTEXT_TIME_ZONE)` | keep the time zone the producer wrote |
| `serializationInclusion(NON_NULL)` | leave out fields that are `null`, so records are smaller |

The fourth one is the important design decision. It means a producer can add a new field to an
event before every consumer is upgraded; old consumers just skip it. Chapter 02's self-check
question 5 asks about it.

The builder pattern itself, one `.something(...)` per line returning the builder, is how many
Java libraries configure objects. Read it top to bottom as a list of settings.

[EnvelopeParser.java](../../ocpp-codec/src/main/java/com/chargemon/ocpp/codec/envelope/EnvelopeParser.java)
shows `readTree` on the hot path, combined with lesson 10's `Result`:

```java
public Result<RawEnvelope, String> parse(String json, String sourceRef) {
    try {
        return parse(mapper.readTree(json), sourceRef);
    } catch (IOException e) {
        return Result.err("envelope is not valid JSON: " + e.getMessage());
    }
}
```

Jackson throws a checked `IOException` (lesson 08) on bad JSON. The parser catches it at the
boundary and turns it into an `Err` value, and from there on nothing throws.

## Part 2: tests

A **test** is a method that runs a piece of your code and checks the answer. Three libraries
share the work:

- **JUnit 5** finds and runs the tests. A test is any method annotated `@Test` in a class under
  `src/test/java`. It reports pass or fail per method.
- **AssertJ** provides the checks. `assertThat(actual).isEqualTo(expected)` fails the test with a
  clear message if the two differ. It reads as a sentence, which is why the repo uses it instead
  of JUnit's own `assertEquals(expected, actual)`.
- **jqwik** generates many random inputs and checks a rule holds for all of them. Instead of one
  hand-picked example, you describe the *shape* of valid inputs.

## Now in chargemon: tests

[IdsAndDurationsTest.java](../../common/src/test/java/com/chargemon/common/IdsAndDurationsTest.java):

```java
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class IdsAndDurationsTest {

    @Test
    void parsesIsoDurations() {
        assertThat(Durations.parse("PT10M")).isEqualTo(Duration.ofMinutes(10));
        assertThat(Durations.parseOptional(" ")).isEmpty();
        assertThatThrownBy(() -> Durations.parse("10 minutes")).isInstanceOf(IllegalArgumentException.class);
    }
}
```

Read it with everything you now know:

1. `import static ...Assertions.assertThat;` A **static import** lets you write `assertThat(...)`
   instead of `Assertions.assertThat(...)`. It is only for readability.
2. `@Test void parsesIsoDurations()` JUnit sees the annotation and runs this method. The name is
   a sentence about what should be true. No `static`, no `public` needed.
3. `assertThat(Durations.parse("PT10M")).isEqualTo(Duration.ofMinutes(10));` "The result of
   parsing `PT10M` should equal ten minutes." Lesson 01 already read this line.
4. `assertThat(Durations.parseOptional(" ")).isEmpty();` AssertJ knows about `Optional` (lesson
   07): a blank string should give an empty box.
5. `assertThatThrownBy(() -> Durations.parse("10 minutes")).isInstanceOf(IllegalArgumentException.class);`
   "Running this lambda (lesson 11) should throw, and the exception should be an
   `IllegalArgumentException` (lesson 08)."

Run it:

```bash
cd /home/vikash/projects/monetering-and-alerting
export JAVA_HOME=~/.jdks/jdk-21.0.12.1+1
./gradlew :common:test --tests '*IdsAndDurations*'
```

Expected: the build ends with `BUILD SUCCESSFUL`. Gradle prints little for passing tests; the
HTML report at `common/build/reports/tests/test/index.html` lists both methods as passed.
Chapter 03 explains the `:common:test` syntax.

The jqwik example,
[AlertLifecycleProperties.java](../../rule-engine/src/test/java/com/chargemon/rules/lifecycle/AlertLifecycleProperties.java),
starts with a sealed interface you can now read on sight:

```java
sealed interface Step permits Trigger, Clear, Wait {
}

record Trigger() implements Step {
}

record Clear() implements Step {
}

record Wait(int seconds) implements Step {
}
```

That is lesson 06's `TrafficLight` shape: three records, one with data. Further down:

```java
@Property(tries = 300)
void openedAndResolvedAlternate_noOpenWhileSuppressed_noOpenBeforeGrace(
        @ForAll("steps") List<Step> steps,
        @ForAll("grace") int graceSec,
        @ForAll("suppression") int suppressionSec) {
```

`@Property(tries = 300)` replaces `@Test`: run this 300 times. Each `@ForAll` parameter is filled
with a fresh random value from a generator named in the string; `steps` produces random lists of
`Trigger`, `Clear` and `Wait`. The method body then replays those steps through the alert
lifecycle and asserts the rules in its name hold every time. Chapter 16 walks through the body.

## Try it

Make the repo test fail on purpose, watch what a failure looks like, then fix it. In
`IdsAndDurationsTest.java` change `Duration.ofMinutes(10)` to `Duration.ofMinutes(11)` and run
the same Gradle command.

Expected: `2 tests completed, 1 failed` and `BUILD FAILED`, and above them a block that includes

```
IdsAndDurationsTest > parsesIsoDurations() FAILED
    org.opentest4j.AssertionFailedError:
    expected: 11M
     but was: 10M
```

That "expected / but was" is AssertJ's message (it prints a `Duration` as `10M`, not `PT10M`).
Do not add `-q` to the command, or Gradle hides that block and only the HTML report shows it. Change the `11` back to `10`, rerun, and confirm
`BUILD SUCCESSFUL`. Then run `git status` to make sure the file is unchanged.

Hint: Gradle caches passing tests. If it says `UP-TO-DATE` and runs nothing after your fix, add
`--rerun-tasks` to the command.

## Check yourself

1. Why can Jackson build a record from JSON without any annotation on the record?
2. What does `disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)` buy the project?
3. What is the difference between `@Test` and `@Property`?

<details><summary>Answers</summary>

1. It matches JSON field names to the canonical constructor's parameter names, and the build's
   `-parameters` flag keeps those names in the class file.
2. A consumer running old code ignores fields it does not know, so a producer can add fields to
   an event before every consumer is upgraded.
3. `@Test` runs a method once with the inputs written in it. `@Property` runs it many times,
   each with random inputs supplied to its `@ForAll` parameters, and fails if any run breaks the
   assertion.

</details>

## Next

You are done with the tutorial. Go back to [chapter 02](../02-java-21-for-this-repo.md) and read it
top to bottom; every section now maps to a lesson here. Then do its three exercises: exercise 1 is
lesson 06's Try it, exercise 2 is lesson 12 inside the real build, and exercise 3 is lesson 07 in
reverse. This lesson covered
[Jackson](../02-java-21-for-this-repo.md#jackson),
[Tests: JUnit 5, AssertJ, jqwik](../02-java-21-for-this-repo.md#tests-junit-5-assertj-jqwik),
[One Jackson configuration for everything](../02-java-21-for-this-repo.md#one-jackson-configuration-for-everything-jsonmapperfactory)
and [A JUnit 5 test in the repo](../02-java-21-for-this-repo.md#a-junit-5-test-in-the-repo).
