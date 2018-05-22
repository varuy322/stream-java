package com.sdu.storm.state.typeutils.base;

import com.google.common.collect.Maps;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

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
    public Map<K, V> createInstance() {
        return Maps.newHashMap();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public void serialize(Map<K, V> record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeBoolean(true);
        } else {
            target.writeBoolean(false);
            target.writeInt(record.size());
            for (Map.Entry<K, V> entry : record.entrySet()) {
                K key = entry.getKey();
                V value = entry.getValue();
                keySerializer.serialize(key, target);
                if (value == null) {
                    target.writeBoolean(true);
                } else {
                    target.writeBoolean(false);
                    valueSerializer.serialize(value, target);
                }
            }
        }
    }

    @Override
    public Map<K, V> deserialize(DataInputView source) throws IOException {
        boolean isNull = source.readBoolean();
        if (isNull) {
            return null;
        }
        int size = source.readInt();
        Map<K, V> record = Maps.newHashMapWithExpectedSize(size);
        for (int i = 0; i < size; ++i) {
            K key = keySerializer.deserialize(source);
            boolean isNullValue = source.readBoolean();
            V value = null;
            if (!isNullValue) {
                value = valueSerializer.deserialize(source);
            }
            record.put(key, value);
        }
        return record;
    }

    @Override
    public TypeSerializer<Map<K, V>> duplicate() {
        TypeSerializer<K> keyTypeSerializer = keySerializer.duplicate();
        TypeSerializer<V> valueTypeSerializer = valueSerializer.duplicate();
        return (keyTypeSerializer == keySerializer) && (valueTypeSerializer == valueSerializer)
                ? this
                : new MapSerializer<>(keyTypeSerializer, valueTypeSerializer);
    }

    @Override
    public Map<K, V> copy(Map<K, V> from) {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
                (obj != null && obj.getClass() == getClass() &&
                        keySerializer.equals(((MapSerializer<?, ?>) obj).getKeySerializer()) &&
                        valueSerializer.equals(((MapSerializer<?, ?>) obj).getValueSerializer()));
    }

    @Override
    public boolean canEqual(Object obj) {
        return (obj != null && obj.getClass() == getClass());
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
