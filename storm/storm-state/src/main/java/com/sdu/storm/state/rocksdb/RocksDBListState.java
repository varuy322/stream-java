package com.sdu.storm.state.rocksdb;

import com.google.common.collect.Lists;
import com.sdu.storm.state.InternalListState;
import com.sdu.storm.state.ListState;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.DataInputViewStreamWrapper;
import com.sdu.storm.utils.DataOutputViewStreamWrapper;
import com.sdu.storm.utils.Preconditions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the values in the list state.
 *
 *           HeapKeyedStateBackendSnapshotMigrationTest
 *
 * @author hanhan.zhang
 * */
public class RocksDBListState<K, N, V>
        extends AbstractRocksDBState<K, N, List<V>, ListState<V>>
        implements InternalListState<K, N, V> {

    /** Serializer for the values. */
    private final TypeSerializer<V> elementSerializer;

    /**
     * Separator of StringAppendTestOperator in RocksDB. See {@link #getPreMergedValue(List)}
     */
    private static final byte DELIMITER = ',';

    public RocksDBListState(ColumnFamilyHandle columnFamily,
                            TypeSerializer<N> namespaceSerializer,
                            TypeSerializer<List<V>> valueSerializer,
                            List<V> defaultValue,
                            TypeSerializer<V> elementSerializer,
                            RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
        this.elementSerializer = elementSerializer;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<List<V>> getValueSerializer() {
        return valueSerializer;
    }


    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        if (sources == null || sources.isEmpty()) {
            return;
        }

        // cache key and namespace
        final K key = backend.getCurrentKey();
        final int keyGroup = backend.getCurrentKeyGroupIndex();

        try {
            // create the target full-binary-key
            writeKeyWithGroupAndNamespace(
                    keyGroup, key, target,
                    keySerializationStream, keySerializationDataOutputView);
            final byte[] targetKey = keySerializationStream.toByteArray();

            // merge the sources to the target
            for (N source : sources) {
                if (source != null) {
                    writeKeyWithGroupAndNamespace(
                            keyGroup, key, source,
                            keySerializationStream, keySerializationDataOutputView);

                    byte[] sourceKey = keySerializationStream.toByteArray();
                    byte[] valueBytes = backend.db.get(columnFamily, sourceKey);
                    backend.db.delete(columnFamily, sourceKey);

                    if (valueBytes != null) {
                        backend.db.merge(columnFamily, writeOptions, targetKey, valueBytes);
                    }
                }
            }
        }
        catch (Exception e) {
            throw new Exception("Error while merging state in RocksDB", e);
        }
    }

    @Override
    public List<V> get() throws Exception {
        try {
            // 读取数据时, 需要初始化:
            //      KeyedStateBackend.setCurrentKey()
            //      InternalKvState.setCurrentNamespace();
            // 构建出State数据存储KEY(线程非安全)
            writeCurrentKeyWithGroupAndNamespace();
            // 读取State存储KEY
            byte[] key = keySerializationStream.toByteArray();
            byte[] valueBytes = backend.db.get(columnFamily, key);

            // VALUE序列化
            if (valueBytes == null) {
                return null;
            }

            ByteArrayInputStream bis = new ByteArrayInputStream(valueBytes);
            DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bis);

            List<V> result = Lists.newLinkedList();
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
    public void add(V value) throws Exception {
        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");

        try {
            // 读取数据时, 需要初始化:
            //      KeyedStateBackend.setCurrentKey()
            //      InternalKvState.setCurrentNamespace();
            // 构建出State数据存储KEY(线程非安全)
            writeCurrentKeyWithGroupAndNamespace();
            // 读取State存储KEY
            byte[] key = keySerializationStream.toByteArray();

            keySerializationStream.reset();
            DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(keySerializationStream);
            elementSerializer.serialize(value, out);
            backend.db.merge(columnFamily, writeOptions, key, keySerializationStream.toByteArray());
        } catch (Exception e) {
            throw new RuntimeException("Error while adding data to RocksDB", e);
        }
    }


    @Override
    public void update(List<V> values) throws Exception {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");

        clear();

        try {
            writeCurrentKeyWithGroupAndNamespace();
            byte[] key = keySerializationStream.toByteArray();
            byte[] valueBytes = getPreMergedValue(values);

            if (valueBytes != null) {
                backend.db.put(columnFamily, writeOptions, key, valueBytes);
            } else {
                throw new IOException("Failed pre-merge values in update()");
            }
        } catch (IOException | RocksDBException e) {

        }
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        Preconditions.checkNotNull(values, "List of values to add cannot be null.");

        try {
            writeCurrentKeyWithGroupAndNamespace();
            byte[] key = keySerializationStream.toByteArray();
            byte[] mergeValues = getPreMergedValue(values);

            if (mergeValues != null) {
                backend.db.merge(columnFamily, writeOptions, key, mergeValues);
            } else {
                throw new IOException("Failed pre-merge values in addAll()");
            }
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while updating data to RocksDB", e);
        }
    }

    @Override
    public void clear() {
        super.clear();
    }

    private byte[] getPreMergedValue(List<V> values) throws IOException {
        DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(keySerializationStream);

        keySerializationStream.reset();

        boolean first = true;
        for (V value : values) {
            Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
            if (first) {
                first = false;
            } else {
                // 向前写分隔符
                out.write(DELIMITER);
            }
            elementSerializer.serialize(value, out);
        }

        return keySerializationStream.toByteArray();
    }
}
