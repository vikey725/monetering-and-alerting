// Lesson 10. Run with:  java ResultType.java
import java.util.function.Function;

public class ResultType {

    // Our own Result, built from lessons 05 (records), 06 (sealed + switch) and 09 (generics).
    // T = the type of a good value, E = the type of an error.
    sealed interface Result<T, E> {

        record Ok<T, E>(T value) implements Result<T, E> { }

        record Err<T, E>(E error) implements Result<T, E> { }

        // map: if Ok, apply f to the value; if Err, pass the error through untouched
        default <U> Result<U, E> map(Function<T, U> f) {
            return switch (this) {
                case Ok<T, E> ok -> new Ok<>(f.apply(ok.value()));
                case Err<T, E> err -> new Err<>(err.error());
            };
        }

        // flatMap: like map, but f itself may fail and returns a Result
        default <U> Result<U, E> flatMap(Function<T, Result<U, E>> f) {
            return switch (this) {
                case Ok<T, E> ok -> f.apply(ok.value());
                case Err<T, E> err -> new Err<>(err.error());
            };
        }
    }

    // A parser that never throws. Bad input becomes an Err value instead.
    static Result<Integer, String> parseWatts(String text) {
        try {
            return new Result.Ok<>(Integer.parseInt(text.trim()));
        } catch (NumberFormatException e) {
            return new Result.Err<>("not a number: '" + text + "'");
        }
    }

    // A second step that can also fail
    static Result<Integer, String> checkPositive(int watts) {
        return watts > 0 ? new Result.Ok<>(watts) : new Result.Err<>("watts must be > 0, got " + watts);
    }

    public static void main(String[] args) {
        String[] inputs = { " 7200 ", "abc", "-5" };
        for (String in : inputs) {
            Result<String, String> r = parseWatts(in)      // Ok(7200) / Err / Ok(-5)
                    .flatMap(w -> checkPositive(w))        // Ok(7200) / Err / Err
                    .map(w -> w / 1000.0)                  // Ok(7.2)  / Err / Err
                    .map(kw -> kw + " kW");                // Ok("7.2 kW") / Err / Err

            // The caller MUST handle both cases; the switch will not compile otherwise.
            String line = switch (r) {
                case Result.Ok<String, String> ok -> "OK   " + ok.value();
                case Result.Err<String, String> err -> "FAIL " + err.error();
            };
            System.out.println(line);
        }
    }
}
