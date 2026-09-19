package com.chargemon.rules.fixtures;

import com.chargemon.rules.condition.Fact;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Fact backed by a flat map of dotted paths. */
public final class MapFact implements Fact {

    private final Map<String, Object> values = new HashMap<>();

    public static MapFact of(Object... kv) {
        MapFact f = new MapFact();
        for (int i = 0; i < kv.length; i += 2) {
            f.values.put((String) kv[i], kv[i + 1]);
        }
        return f;
    }

    public MapFact with(String path, Object value) {
        values.put(path, value);
        return this;
    }

    @Override
    public Optional<Object> get(String path) {
        return Optional.ofNullable(values.get(path));
    }
}
