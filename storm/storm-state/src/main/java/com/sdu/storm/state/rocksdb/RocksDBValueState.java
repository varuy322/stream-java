package com.sdu.storm.state.rocksdb;

import com.sdu.storm.state.InternalValueState;
import com.sdu.storm.state.ValueState;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.DataInputViewStreamWrapper;
import com.sdu.storm.utils.DataOutputViewStreamWrapper;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of value that the state state stores.
 *
 * @author hanhan.zhang
 * */
public class RocksDBValueState<K, N, V>
        extends AbstractRocksDBState<K, N, V, ValueState<V>>
        implements InternalValueState<K, N, V> {

    /**
     * @param columnFamily 数据存储句柄
     * @param namespaceSerializer 命名空间序列化器(RocksDB存储KEY的一部分)
     * @param valueSerializer 存储数据序列化器
     * @param backend The backend for which this state is bind to
     * */
    public RocksDBValueState(ColumnFamilyHandle columnFamily,
                             TypeSerializer<N> namespaceSerializer,
                             TypeSerializer<V> valueSerializer,
                             V defaultValue,
                             RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);
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
    public TypeSerializer<V> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public V value() throws IOException {
        // 读取数据时需先设置:
        //  RocksDBKeyedStateBackend.setCurrentKey()[RocksDB存储KEY的一部分]
        //  AbstractRocksDBState.setCurrentNamespace()[RocksDB存储KEY的一部分]
        // AbstractRocksDBState.writeCurrentKeyWithGroupAndNamespace()构建RockDB存储KEY
        try {
            writeCurrentKeyWithGroupAndNamespace();

            byte[] key = keySerializationStream.toByteArray();
            byte[] valueBytes = backend.db.get(columnFamily, key);

            if (valueBytes == null || valueBytes.length == 0) {
                return getDefaultValue();
            }

            return valueSerializer.deserialize(new DataInputViewStreamWrapper(new ByteArrayInputStream(valueBytes)));
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while retrieving data from RocksDB.", e);
        }

    }

    @Override
    public void update(V value) throws IOException {
        if (value == null) {
            clear();
            return;
        }

        // 更新数据时需先设置:
        //   1: RocksDBKeyedStateBackend.setCurrentKey()[RocksDB存储KEY的一部分]
        //   2: AbstractRocksDBState.setCurrentNamespace()[RocksDB存储KEY的一部分]
        //   3: AbstractRocksDBState.writeCurrentKeyWithGroupAndNamespace()构建RockDB存储KEY
        DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(keySerializationStream);
        try {
            writeCurrentKeyWithGroupAndNamespace();
            // 构建存储KEY
            byte[] key = keySerializationStream.toByteArray();
            // 数据流重置, 序列化Value
            keySerializationStream.reset();
            valueSerializer.serialize(value, output);
            byte[] valueBytes = keySerializationStream.toByteArray();
            backend.db.put(columnFamily, writeOptions, key, valueBytes);
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while adding data to RocksDB", e);
        }
    }
}
