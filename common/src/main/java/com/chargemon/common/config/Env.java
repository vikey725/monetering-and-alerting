package com.chargemon.common.config;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/** Typed access to environment/system properties with sane defaults. */
public final class Env {

    private final Map<String, String> source;

    public Env(Map<String, String> source) {
        this.source = Map.copyOf(source);
    }

    public static Env system() {
        return new Env(System.getenv());
    }

    public Optional<String> get(String key) {
        String v = source.get(key);
        if (v == null) {
            v = System.getProperty(key);
        }
        return Optional.ofNullable(v).filter(s -> !s.isBlank());
    }

    public String get(String key, String def) {
        return get(key).orElse(def);
    }

    public int getInt(String key, int def) {
        return get(key).map(Integer::parseInt).orElse(def);
    }

    public long getLong(String key, long def) {
        return get(key).map(Long::parseLong).orElse(def);
    }

    public boolean getBool(String key, boolean def) {
        return get(key).map(Boolean::parseBoolean).orElse(def);
    }

    public Duration getDuration(String key, Duration def) {
        return get(key).map(Duration::parse).orElse(def);
    }

    public String require(String key) {
        return get(key).orElseThrow(() -> new IllegalStateException("Missing required config: " + key));
    }
}
