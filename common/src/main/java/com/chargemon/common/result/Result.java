package com.chargemon.common.result;

import java.util.function.Function;

/** Minimal railway-style result to keep parse/validation paths exception-free on hot loops. */
public sealed interface Result<T, E> {

    record Ok<T, E>(T value) implements Result<T, E> {
    }

    record Err<T, E>(E error) implements Result<T, E> {
    }

    static <T, E> Result<T, E> ok(T value) {
        return new Ok<>(value);
    }

    static <T, E> Result<T, E> err(E error) {
        return new Err<>(error);
    }

    default boolean isOk() {
        return this instanceof Ok<T, E>;
    }

    default <U> Result<U, E> map(Function<? super T, ? extends U> f) {
        return switch (this) {
            case Ok<T, E> ok -> new Ok<>(f.apply(ok.value()));
            case Err<T, E> err -> new Err<>(err.error());
        };
    }

    default <U> Result<U, E> flatMap(Function<? super T, Result<U, E>> f) {
        return switch (this) {
            case Ok<T, E> ok -> f.apply(ok.value());
            case Err<T, E> err -> new Err<>(err.error());
        };
    }

    default T orElseThrow(Function<? super E, ? extends RuntimeException> f) {
        return switch (this) {
            case Ok<T, E> ok -> ok.value();
            case Err<T, E> err -> throw f.apply(err.error());
        };
    }
}
