package com.chargemon.flink.serde;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/** Entry point for Jackson-backed Flink type information. */
public final class JsonTypes {

    private JsonTypes() {
    }

    public static <T> TypeInformation<T> of(Class<T> type) {
        return new JsonTypeInformation<>(type);
    }
}
