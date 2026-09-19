# 11. Lambdas and streams

## In one sentence

A lambda is a method without a name that you write in place, and it can stand in for any
interface that has exactly one abstract method.

## Why you need it

You have been reading lambdas since lesson 07: `v -> v.length()`, `path -> Optional.empty()`,
`() -> Durations.parse("10 minutes")`, `w -> checkPositive(w)`. Chapter 02 uses the words
"functional interface", "method reference" and "stream" in one paragraph. After this lesson you
can read all three and know when the repo chooses a plain loop instead.

## Toy program

File: [code/Lambdas.java](code/Lambdas.java)

```java
// Lesson 11. Run with:  java Lambdas.java
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Lambdas {

    // A functional interface: exactly ONE abstract method. Any such interface can be
    // implemented on the spot with a lambda instead of writing a whole class.
    @FunctionalInterface
    interface Fact {
        Optional<Object> get(String path);
    }

    public static void main(String[] args) {
        // Lambda syntax:  (parameters) -> result      One parameter needs no brackets.
        Fact empty = path -> Optional.empty();
        Fact fake = path -> path.equals("event.status") ? Optional.of("Faulted") : Optional.empty();
        System.out.println(empty.get("event.status"));
        System.out.println(fake.get("event.status"));

        // Three standard functional interfaces from java.util.function
        Function<String, Integer> length = s -> s.length();      // takes A, returns B
        Predicate<Integer> big = n -> n > 5;                      // takes A, returns boolean
        Supplier<String> now = () -> "generated value";           // takes nothing, returns T
        System.out.println(length.apply("Kempower") + " " + big.test(8) + " " + now.get());

        // Method reference: a lambda that only calls one existing method
        List<String> found = new ArrayList<>();
        List<String> source = List.of("ABB", "Kempower", "Alpitronic");
        source.forEach(found::add);            // same as  source.forEach(x -> found.add(x))
        System.out.println(found);

        // A stream: a pipeline over a collection. Read left to right.
        List<String> longNamesUpper = source.stream()
                .filter(v -> v.length() > 3)   // keep only these
                .map(String::toUpperCase)      // transform each one
                .toList();                     // collect back into a list
        System.out.println(longNamesUpper);

        // The same thing as a plain loop. This repo often prefers the loop on hot paths.
        List<String> loopVersion = new ArrayList<>();
        for (String v : source) {
            if (v.length() > 3) {
                loopVersion.add(v.toUpperCase());
            }
        }
        System.out.println(loopVersion);
    }
}
```

Run it:

```bash
java Lambdas.java
```

Output:

```
Optional.empty
Optional[Faulted]
8 true generated value
[ABB, Kempower, Alpitronic]
[KEMPOWER, ALPITRONIC]
[KEMPOWER, ALPITRONIC]
```

## Read it line by line

1. `@FunctionalInterface interface Fact { Optional<Object> get(String path); }` is an ordinary
   interface (lesson 04) with exactly **one abstract method**. That makes it a **functional
   interface**. The annotation is optional; it only asks the compiler to complain if someone adds
   a second abstract method. (Default methods do not count.)
2. `Fact empty = path -> Optional.empty();` is a **lambda**. Compare with lesson 04, where
   implementing `Notifier` needed a whole class with a method inside. Here, because `Fact` has
   only one method, Java knows the lambda *is* that method: `path` is its parameter, the
   expression after `->` is its return value. The type of `path` is not written; the compiler
   reads it from the interface (`String`).
3. Lambda syntax rules: `x -> expr` for one parameter; `(x, y) -> expr` for two; `() -> expr`
   for none; `x -> { statements; return value; }` when you need more than one line.
4. `Function<String, Integer> length = s -> s.length();` The JDK ships ready-made functional
   interfaces in `java.util.function` so you do not declare one per use. **`Function<A, B>`**
   takes an `A`, returns a `B`; call it with `apply`. **`Predicate<A>`** takes an `A`, returns
   `boolean`; call with `test`. **`Supplier<T>`** takes nothing, returns a `T`; call with `get`.
   There is also `Consumer<A>` (takes an `A`, returns nothing; `accept`). `Result.map` in lesson
   10 took a `Function<T, U>`; now you know what that is.
5. `source.forEach(found::add);` is a **method reference**: `object::method` is shorthand for a
   lambda that just calls that method with the same arguments. `found::add` means
   `x -> found.add(x)`. The other form is `Class::method`, as in `String::toUpperCase`, which
   means `s -> s.toUpperCase()`. It is only spelling; nothing new happens.
6. `source.stream().filter(...).map(...).toList()` is a **stream**: a pipeline that pulls each
   element through a series of steps. `filter(predicate)` keeps elements where the predicate is
   true; `map(function)` replaces each element with the function's result; `toList()` runs the
   pipeline and collects the output. Nothing happens until the last step. Other finishing steps
   you will meet: `count()`, `findFirst()` (returns an `Optional`), `anyMatch(predicate)`,
   `collect(...)`.
7. The loop version does the same work. Streams read well for short transformations; loops are
   easier to step through in a debugger and avoid a little allocation per element. Chapter 02
   says the repo "uses streams sparingly and prefers plain loops where it matters for speed",
   meaning inside Flink operators that run per message. You should be able to read both.

## Now in chargemon

[Fact.java](../../rule-engine/src/main/java/com/chargemon/rules/condition/Fact.java) is the toy's
`Fact`, with the lambda inside a static factory:

```java
@FunctionalInterface
public interface Fact {

    Optional<Object> get(String path);

    static Fact empty() {
        return path -> Optional.empty();
    }
}
```

`Fact.empty()` returns a `Fact` built from a one-line lambda: whatever path you ask for, the
answer is the empty box. Elsewhere the rule engine builds real facts the same way, from a lambda
that looks the path up in an event.

[EvaluatorRegistry.java](../../rule-engine/src/main/java/com/chargemon/rules/eval/EvaluatorRegistry.java)
has the method reference:

```java
List<StationRuleKindEvaluator<?>> found = new ArrayList<>();
ServiceLoader.load(StationRuleKindEvaluator.class).forEach(found::add);
```

"For each evaluator the loader finds, add it to `found`." Identical to the toy's
`source.forEach(found::add)`. `ServiceLoader` is the next lesson.

And from lesson 08, the test line `assertThatThrownBy(() -> Durations.parse("10 minutes"))` passes
a zero-argument lambda. AssertJ runs it inside its own `try`/`catch` and checks what came out.
Passing code *as a value* to be run later is the thing lambdas make cheap.

## Try it

Add a fourth lambda to the toy, `Predicate<String> startsWithA = s -> s.startsWith("A");`, and
use it in the stream instead of the length filter: `.filter(startsWithA)`. Then write the same
result with a loop.

Expected, for both:

```
[ABB, ALPITRONIC]
```

Hint: a variable that holds a `Predicate` can be passed straight to `filter`, because `filter`
asks for a `Predicate`.

## Check yourself

1. What makes an interface usable with a lambda?
2. Rewrite `found::add` and `String::toUpperCase` as full lambdas.
3. In `source.stream().filter(p).map(f).toList()`, which step actually does the work?

<details><summary>Answers</summary>

1. Exactly one abstract method. Default and static methods do not count against it.
2. `x -> found.add(x)` and `s -> s.toUpperCase()`.
3. `toList()`. `filter` and `map` only describe the pipeline; the terminal step pulls the elements
   through.

</details>

## Next

[12. ServiceLoader](12-serviceloader.md). In chapter 02 this lesson covers
[Lambdas, functional interfaces, streams](../02-java-21-for-this-repo.md#lambdas-functional-interfaces-streams).
