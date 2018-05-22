package com.sdu.stream.state.rocksdb;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.sdu.stream.state.InternalListState;
import com.sdu.stream.state.seralizer.TypeSerializer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

public class RocksDBListState<N, K, T>
        extends AbstractRocksDBState<N, K, T>
        implements InternalListState<N, K, T> {

    private final TypeSerializer<K> userKeySerializer;
    private final TypeSerializer<T> elementSerializer;

    /** value element delimiter */
    private static final byte DELIMITER = ',';

    public RocksDBListState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            RocksDBKeyedStateBackend<K> backend,
            TypeSerializer<T> elementSerializer) {
        super(columnFamily, namespaceSerializer, backend);
        this.userKeySerializer = backend.getKeyTypeSerializer();
        this.elementSerializer = elementSerializer;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return userKeySerializer;
    }

    @Override
    public void update(N namespace, K userKey, List<T> values) throws IOException {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");

        clear(namespace, userKey);

        try {
            ByteArrayOutputStream serializerStream = new ByteArrayOutputStream(64);

            ByteArrayDataOutput output = ByteStreams.newDataOutput(serializerStream);
            byte[] storeKey = serializerNamespaceAndKey(namespace, userKey, output);
            byte[] valueBytes = getPreMergedValue(serializerStream, values);

            if (valueBytes != null) {
                backend.db.put(columnFamily, writeOptions, storeKey, valueBytes);
            } else {
                throw new IOException("Failed pre-merge values in update()");
            }
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while update data to RocksDB", e);
        }
    }

    @Override
    public void addAll(N namespace, K userKey, List<T> values) throws IOException {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");

        try {
            ByteArrayOutputStream serializerStream = new ByteArrayOutputStream(64);

            ByteArrayDataOutput output = ByteStreams.newDataOutput(serializerStream);
            byte[] storeKey = serializerNamespaceAndKey(namespace, userKey, output);
            byte[] mergeValues = getPreMergedValue(serializerStream, values);

            if (mergeValues != null) {
                backend.db.merge(columnFamily, writeOptions, storeKey, mergeValues);
            } else {
                throw new IOException("Failed pre-merge values in addAll()");
            }
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while updating data to RocksDB", e);
        }
    }

    @Override
    public List<T> get(N namespace, K userKey) throws IOException {
        try {
            ByteArrayOutputStream serializerStream = new ByteArrayOutputStream(64);

            ByteArrayDataOutput output = ByteStreams.newDataOutput(serializerStream);
            byte[] storeKey = serializerNamespaceAndKey(namespace, userKey, output);

            byte[] valueBytes = backend.db.get(columnFamily, storeKey);
            if (valueBytes == null) {
                return null;
            }

            List<T> result = Lists.newLinkedList();

            ByteArrayInputStream bis = new ByteArrayInputStream(valueBytes);
            DataInputStream in = new DataInputStream(bis);
            while (in.available() > 0) {
                result.add(elementSerializer.deserialize(in));

                /* 元素间有分隔符 */
                if (in.available() > 0) {
                    in.readByte();
                }
            }

            return result;
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while retrieving data from RocksDB", e);
        }
    }

    @Override
    public void add(N namespace, K userKey, T value) throws IOException {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

        try {
            ByteArrayOutputStream serializerStream = new ByteArrayOutputStream(64);

            ByteArrayDataOutput output = ByteStreams.newDataOutput(serializerStream);
            byte[] storeKey = serializerNamespaceAndKey(namespace, userKey, output);

            serializerStream.reset();

            output = ByteStreams.newDataOutput(serializerStream);
            elementSerializer.serializer(value, output);
            byte[] storeValue = serializerStream.toByteArray();

            backend.db.merge(columnFamily, writeOptions, storeKey, storeValue);
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while adding data to RocksDB", e);
        }
    }

    private byte[] getPreMergedValue(ByteArrayOutputStream serializerStream, List<T> values) throws IOException {
        serializerStream.reset();
        ByteArrayDataOutput output = ByteStreams.newDataOutput(serializerStream);

        boolean first = true;
        for (T value : values) {
            Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
            if (first) {
                first = false;
            } else {
                // 向前写分隔符
                output.write(DELIMITER);
            }
            elementSerializer.serializer(value, output);
        }

        return serializerStream.toByteArray();
    }
}
