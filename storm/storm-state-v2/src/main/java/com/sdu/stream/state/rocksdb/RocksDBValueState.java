package com.sdu.stream.state.rocksdb;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.sdu.stream.state.InternalValueState;
import com.sdu.stream.state.seralizer.TypeSerializer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class RocksDBValueState<N, K, T>
        extends AbstractRocksDBState<N, K, T>
        implements InternalValueState<N, K, T>{

    private final TypeSerializer<T> valueSerializer;

    public RocksDBValueState(ColumnFamilyHandle columnFamily,
                             TypeSerializer<N> namespaceSerializer,
                             TypeSerializer<T> valueSerializer,
                             RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, backend);
        this.valueSerializer = valueSerializer;
    }

    @Override
    public T value(N namespace, K userKey) throws IOException {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream(64);
            ByteArrayDataOutput output = ByteStreams.newDataOutput(stream);
            byte[] storeKey = serializerNamespaceAndKey(namespace, userKey, output);

            byte[] storeValue = backend.db.get(columnFamily, storeKey);
            if (storeKey.length == 0) {
                return null;
            }

            return valueSerializer.deserialize(ByteStreams.newDataInput(storeValue));
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while retrieving data from RocksDB.", e);
        }
    }

    @Override
    public void update(N namespace, K userKey, T value) throws IOException {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream(64);
            ByteArrayDataOutput output = ByteStreams.newDataOutput(stream);
            byte[] storeKey = serializerNamespaceAndKey(namespace, userKey, output);

            stream.reset();
            valueSerializer.serializer(value, ByteStreams.newDataOutput(stream));
            byte[] storeValue = stream.toByteArray();

            backend.db.put(columnFamily, writeOptions, storeKey, storeValue);
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while adding data to RocksDB", e);
        }
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeyTypeSerializer();
    }
}
