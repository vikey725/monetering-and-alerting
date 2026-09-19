package com.chargemon.common.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Single place that configures Jackson for the whole platform, so every module
 * serializes time, optionals and unknown fields the same way.
 */
public final class JsonMapperFactory {

    private static final ObjectMapper STANDARD = configure(JsonMapper.builder()).build();
    private static final ObjectMapper SMILE = configure(JsonMapper.builder(new SmileFactory())).build();

    private JsonMapperFactory() {
    }

    /** Text JSON mapper for Kafka payloads, config and logs. */
    public static ObjectMapper standard() {
        return STANDARD;
    }

    /** Binary (Smile) mapper for compact state / in-flight serialization. */
    public static ObjectMapper smile() {
        return SMILE;
    }

    private static JsonMapper.Builder configure(JsonMapper.Builder builder) {
        return builder
                .addModule(new JavaTimeModule())
                .addModule(new Jdk8Module())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
                .serializationInclusion(JsonInclude.Include.NON_NULL);
    }
}
