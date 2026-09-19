package com.chargemon.flink.serde;

import java.io.IOException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/** Records the carried class; the JSON format itself tolerates additive evolution. */
public final class JsonSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    private static final int VERSION = 1;

    private Class<T> type;

    @SuppressWarnings("unused")
    public JsonSerializerSnapshot() {
        // for reflective instantiation
    }

    JsonSerializerSnapshot(Class<T> type) {
        this.type = type;
    }

    @Override
    public int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        out.writeUTF(type.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        String name = in.readUTF();
        try {
            type = (Class<T>) Class.forName(name, true, userCodeClassLoader);
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot restore JsonSerializer for missing class " + name, e);
        }
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return new JsonSerializer<>(type);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializerSnapshot<T> oldSnapshot) {
        if (oldSnapshot instanceof JsonSerializerSnapshot<T> old && old.type.equals(type)) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
        return TypeSerializerSchemaCompatibility.incompatible();
    }
}
