# 09. Generics

## In one sentence

Generics let a class or method work with "some type T" that the caller chooses, so the compiler
can check types instead of you casting and hoping.

## Why you need it

Angle brackets are everywhere in chapter 02: `List<String>`, `Optional<T>`, `Result<T, E>`,
`StationRuleKindEvaluator<S extends RuleSpec>`, `List<StationRuleKindEvaluator<?>>`,
`Map<String, Class<? extends Condition>>`. Each one follows the same handful of rules. This
lesson gives you those rules and a way to read any of them out loud.

## Toy program

File: [code/Generics.java](code/Generics.java)

```java
// Lesson 09. Run with:  java Generics.java
import java.util.ArrayList;
import java.util.List;

public class Generics {

    // A generic class: "T" is a placeholder for a type chosen later by the user of Box.
    static class Box<T> {
        private final T content;

        Box(T content) {
            this.content = content;
        }

        T get() {
            return content;
        }
    }

    // A generic METHOD: the <T> before the return type says "this method has its own T".
    static <T> T first(List<T> items) {
        return items.get(0);
    }

    // A bounded type: S must be a Number or a subtype (Integer, Double, ...).
    static <S extends Number> double doubled(S n) {
        return n.doubleValue() * 2;
    }

    // Wildcard "?": "a list of something, I do not care what". Read-only use.
    static int count(List<?> anything) {
        return anything.size();
    }

    // "? extends Number": a list of Number or any subtype. Safe to READ numbers from it.
    static double sum(List<? extends Number> numbers) {
        double total = 0;
        for (Number n : numbers) {
            total += n.doubleValue();
        }
        return total;
    }

    public static void main(String[] args) {
        Box<String> words = new Box<>("hello");     // T = String here
        Box<Integer> number = new Box<>(42);        // T = Integer here
        String w = words.get();                      // no cast needed: compiler knows it is a String
        System.out.println(w + " / " + number.get());

        List<String> names = new ArrayList<>();      // a list OF strings only
        names.add("ABB");
        names.add("Kempower");
        // names.add(42);   <- uncomment: compile error. That is the whole point of generics.
        System.out.println("first: " + first(names));

        List<Integer> ints = List.of(1, 2, 3);
        List<Double> doubles = List.of(1.5, 2.5);
        System.out.println("doubled: " + doubled(21));
        System.out.println("count: " + count(names) + " and " + count(ints));
        System.out.println("sum ints: " + sum(ints) + ", sum doubles: " + sum(doubles));
    }
}
```

Run it:

```bash
java Generics.java
```

Output:

```
hello / 42
first: ABB
doubled: 42.0
count: 2 and 3
sum ints: 6.0, sum doubles: 4.0
```

## Read it line by line

1. `static class Box<T>` declares a class with a **type parameter** `T`. Inside the class, `T`
   is used like a type name (`T content`, `T get()`), but nobody knows yet what it is. The letter
   is convention: `T` for type, `E` for element or error, `K`/`V` for key/value, `S`/`U` when you
   need more.
2. `Box<String> words = new Box<>("hello");` chooses `T = String` for this one box. Now the
   compiler knows `words.get()` returns a `String`, so `String w = words.get();` needs no cast.
   The empty `<>` on the right (the "diamond") means "same as on the left".
3. `Box<Integer> number = new Box<>(42);` is a different box with `T = Integer`. `Integer` not
   `int`: type parameters must be classes, so the built-in number types have object twins
   (`Integer`, `Long`, `Double`, `Boolean`). Java converts between them automatically.
4. `List<String> names` is "a list of strings". `List` is the JDK's generic list interface;
   `ArrayList` is its most common implementation. Because the type says `String`, the commented
   line `names.add(42)` would not compile. Without generics, lists held `Object` and every `get`
   needed a cast that could blow up at run time. Generics move that failure to compile time.
5. `static <T> T first(List<T> items)` is a **generic method**. The `<T>` before the return
   type introduces a `T` that belongs to this method alone. Read it: "for any T, take a list of T
   and return one T". Calling `first(names)` makes `T = String` without you writing it. Chapter 02
   shows `static <T, E> Result<T, E> ok(T value)`; same shape with two letters.
6. `static <S extends Number> double doubled(S n)` is a **bounded** type parameter. `S` may be
   `Number` or anything that extends it, and nothing else. The bound is what lets the body call
   `n.doubleValue()`: the compiler knows every `S` has it. Read `<S extends RuleSpec>` in the repo
   the same way: "S is some kind of RuleSpec".
7. `List<?>` uses the **wildcard**. Read `?` as "something, I do not care what". You may read
   elements as `Object` and ask the size, but you may not `add` (the compiler cannot know what
   type would be safe). Use it when the method genuinely does not care.
8. `List<? extends Number>` is a **bounded wildcard**: a list of `Number` or any subtype. So a
   `List<Integer>` and a `List<Double>` are both accepted, and you may read each element as a
   `Number`. Note that a plain `List<Number>` parameter would *reject* a `List<Integer>`; a list
   of integers is not a list of numbers, because you could then add a `Double` to it. The
   wildcard exists exactly to express "read-only, any subtype".

Cheat sheet for reading any generic type out loud:

| You see | Say |
|---|---|
| `List<String>` | a list of strings |
| `Map<K, V>` | a map from K to V |
| `Optional<T>` | maybe one T |
| `Result<T, E>` | a result with value T or error E |
| `<T> T f(List<T>)` | for any T, f takes a list of T and returns a T |
| `<S extends X>` | S is some kind of X |
| `Foo<?>` | a Foo of something, do not care what |
| `Foo<? extends X>` | a Foo of X or any subtype, for reading |
| `Foo<? super X>` | a Foo of X or any supertype, for writing |

## Now in chargemon

[StationRuleKindEvaluator.java](../../rule-engine/src/main/java/com/chargemon/rules/eval/StationRuleKindEvaluator.java)
starts:

```java
public interface StationRuleKindEvaluator<S extends RuleSpec> {

    RuleKind kind();

    Class<S> specType();

    void onInput(RuleDefinition rule, S spec, StationInput input, RuleContext ctx);
```

"An evaluator for some kind of RuleSpec, `S`." Each concrete evaluator picks its `S`: the event
evaluator is a `StationRuleKindEvaluator<EventSpec>`, so its `onInput` receives an `EventSpec`
and nothing else can be passed in. `Class<S>` is "the class object for S", used to check the
spec's type at run time.

[EvaluatorRegistry.java](../../rule-engine/src/main/java/com/chargemon/rules/eval/EvaluatorRegistry.java)
stores all of them together:

```java
public static EvaluatorRegistry fromServiceLoader() {
    List<StationRuleKindEvaluator<?>> found = new ArrayList<>();
    ServiceLoader.load(StationRuleKindEvaluator.class).forEach(found::add);
    return of(found);
}

public static EvaluatorRegistry of(Collection<? extends StationRuleKindEvaluator<?>> evaluators) {
```

`List<StationRuleKindEvaluator<?>>` is "a list of evaluators, each for some spec type I do not
care about here". The registry only needs `kind()` from each, which every evaluator has regardless
of `S`, so the wildcard is exactly right. `Collection<? extends StationRuleKindEvaluator<?>>` is
the same idea one level out: "any collection of any such evaluators, for reading".

The chapter's last example, `Map<String, Class<? extends Condition>>` in `BuiltinOperators`, now
reads: "a map from operator name to the class of some kind of Condition". The class object is
stored so the parser can build a condition of that type later.

## Try it

Add a method `static <T> Box<T> boxFirst(List<T> items)` that returns `new Box<>(items.get(0))`,
and print `boxFirst(names).get()` and `boxFirst(ints).get()`.

Expected:

```
ABB
1
```

Then try `sum(names)`. Expected: a compile error whose last line reads
`reason: argument mismatch; List<String> cannot be converted to List<? extends Number>`. The
bound did its job: strings are not numbers.

Hint: `boxFirst` is `first` with the result wrapped in a `Box`.

## Check yourself

1. Why does `String w = words.get();` compile without a cast?
2. Read `Optional<StationRuleKindEvaluator<?>> station(RuleKind kind)` out loud.
3. Why does `sum` take `List<? extends Number>` rather than `List<Number>`?

<details><summary>Answers</summary>

1. `words` is a `Box<String>`, so the compiler substitutes `String` for `T` and knows `get()`
   returns a `String`.
2. "Given a rule kind, return maybe one evaluator for some spec type." It is empty when no
   evaluator is registered for that kind.
3. A `List<Integer>` is not a `List<Number>` (you could otherwise add a `Double` into a list of
   integers). `? extends Number` accepts a list of any Number subtype for reading only, which is
   all `sum` needs.

</details>

## Next

[10. The Result type](10-result-type.md). In chapter 02 this lesson covers
[Generics](../02-java-21-for-this-repo.md#generics) and
[Generics you need to read EvaluatorRegistry](../02-java-21-for-this-repo.md#generics-you-need-to-read-evaluatorregistry).
