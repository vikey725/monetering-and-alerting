package com.chargemon.flink.serde;

import com.chargemon.common.json.JsonMapperFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Length-prefixed Smile encoding. Tolerant to additive schema changes because the
 * mapper ignores unknown properties. All carried types are immutable, so copies are identity.
 */
public final class JsonSerializer<T> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> type;
    private transient ObjectMapper mapper;

    public JsonSerializer(Class<T> type) {
        this.type = type;
    }

    public Class<T> type() {
        return type;
    }

    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = JsonMapperFactory.smile();
        }
        return mapper;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return this;
    }

    @Override
    public T createInstance() {
        return null;
    }

    @Override
    public T copy(T from) {
        return from;
    }

    @Override
    public T copy(T from, T reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        byte[] bytes = mapper().writeValueAsBytes(record);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        int len = source.readInt();
        byte[] bytes = new byte[len];
        source.readFully(bytes);
        return mapper().readValue(bytes, type);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int len = source.readInt();
        target.writeInt(len);
        byte[] bytes = new byte[len];
        source.readFully(bytes);
        target.write(bytes);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof JsonSerializer<?> other && other.type.equals(type);
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new JsonSerializerSnapshot<>(type);
    }
}
