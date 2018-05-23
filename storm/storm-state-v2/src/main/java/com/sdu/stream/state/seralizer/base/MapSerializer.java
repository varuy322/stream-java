package com.sdu.stream.state.seralizer.base;

import com.google.common.collect.Maps;
import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class MapSerializer<K, V> extends TypeSerializer<Map<K, V>> {

    /** The serializer for the keys in the map */
    private final TypeSerializer<K> keySerializer;

    /** The serializer for the values in the map */
    private final TypeSerializer<V> valueSerializer;

    public MapSerializer(TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }


    @Override
    public void serializer(Map<K, V> record, DataOutput target) throws IOException {
        if (record == null) {
            target.writeBoolean(true);
        } else {
            target.writeBoolean(false);
            target.writeInt(record.size());
            for (Map.Entry<K, V> entry : record.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                keySerializer.serializer(key, target);
                if (value == null) {
                    target.writeBoolean(true);
                } else {
                    target.writeBoolean(false);
                    valueSerializer.serializer(value, target);
                }
            }
        }
    }

    @Override
    public Map<K, V> deserialize(DataInput source) throws IOException {
        boolean isNull = source.readBoolean();
        if (isNull) {
            return null;
        } else {
            int size = source.readInt();
            Map<K, V> record = Maps.newHashMapWithExpectedSize(size);
            for (int i = 0; i < size; ++i) {
                K key = keySerializer.deserialize(source);
                boolean isNullValue = source.readBoolean();
                V value = isNullValue ? null : valueSerializer.deserialize(source);
                record.put(key, value);
            }
            return record;
        }
    }

    @Override
    public int hashCode() {
        return keySerializer.hashCode() * 31 + valueSerializer.hashCode();
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }
}
