package com.chargemon.flink.serde;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public final class JsonTypeInformation<T> extends TypeInformation<T> {

    private final Class<T> type;

    public JsonTypeInformation(Class<T> type) {
        this.type = type;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<T> getTypeClass() {
        return type;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(SerializerConfig config) {
        return new JsonSerializer<>(type);
    }

    @Override
    @SuppressWarnings("deprecation")
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return new JsonSerializer<>(type);
    }

    @Override
    public String toString() {
        return "Json<" + type.getName() + ">";
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof JsonTypeInformation<?> other && other.type.equals(type);
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof JsonTypeInformation;
    }
}
