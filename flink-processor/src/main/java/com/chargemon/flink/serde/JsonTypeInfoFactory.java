package com.chargemon.flink.serde;

import java.lang.reflect.Type;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Annotate a carrier type with {@code @TypeInfo(JsonTypeInfoFactory.class)} to make
 * Flink serialize it with Jackson (Smile) instead of falling back to Kryo.
 */
public final class JsonTypeInfoFactory<T> extends TypeInfoFactory<T> {

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<T> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        Class<T> raw = (Class<T>) (t instanceof Class<?> c ? c : ((java.lang.reflect.ParameterizedType) t).getRawType());
        return new JsonTypeInformation<>(raw);
    }
}
