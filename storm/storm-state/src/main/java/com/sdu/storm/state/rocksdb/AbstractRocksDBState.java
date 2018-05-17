package com.sdu.storm.state.rocksdb;

import com.sdu.storm.state.InternalKvState;
import com.sdu.storm.state.State;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.ByteArrayOutputStreamWithPos;
import com.sdu.storm.utils.DataOutputView;
import com.sdu.storm.utils.DataOutputViewStreamWrapper;
import com.sdu.storm.utils.Preconditions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;

public abstract class AbstractRocksDBState<K, N, V, S extends State> implements InternalKvState<K, N, V>, State {

    /** Serializer for the namespace. */
    final TypeSerializer<N> namespaceSerializer;

    /** Serializer for the state values. */
    final TypeSerializer<V> valueSerializer;

    /** The current namespace, which the next value methods will refer to. */
    private N currentNamespace;

    /** Backend that holds the actual RocksDB instance where we store state. */
    protected RocksDBKeyedStateBackend<K> backend;

    /** The column family of this particular instance of state. */
    protected ColumnFamilyHandle columnFamily;

    protected final V defaultValue;

    protected final WriteOptions writeOptions;

    /** The key bytes holder */
    protected final ByteArrayOutputStreamWithPos keySerializationStream;

    protected final DataOutputView keySerializationDataOutputView;

    /** If true, the key length not fixed */
    private final boolean ambiguousKeyPossible;

    protected AbstractRocksDBState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            RocksDBKeyedStateBackend<K> backend) {
        this.namespaceSerializer = namespaceSerializer;
        this.backend = backend;

        this.columnFamily = columnFamily;

        this.writeOptions = backend.getWriteOptions();
        this.valueSerializer = Preconditions.checkNotNull(valueSerializer, "State value serializer");
        this.defaultValue = defaultValue;

        this.keySerializationStream = new ByteArrayOutputStreamWithPos(128);
        this.keySerializationDataOutputView = new DataOutputViewStreamWrapper(keySerializationStream);
        this.ambiguousKeyPossible = RocksDBKeySerializationUtils.isAmbiguousKeyPossible(backend.getKeySerializer(), namespaceSerializer);
    }

    @Override
    public void setCurrentNamespace(N namespace) {
        this.currentNamespace = Preconditions.checkNotNull(namespace, "Namespace");
    }

    @Override
    public byte[] getSerializedValue(byte[] serializedKeyAndNamespace,
                                     TypeSerializer<K> safeKeySerializer,
                                     TypeSerializer<N> safeNamespaceSerializer,
                                     TypeSerializer<V> safeValueSerializer) throws Exception {
        return new byte[0];
    }

    @Override
    public void clear() {
        try {
            // 生成存储KEY
            writeCurrentKeyWithGroupAndNamespace();
            byte[] key = keySerializationStream.toByteArray();

            // 删除存储KEY存储的数据
            backend.db.delete(columnFamily, writeOptions, key);
        } catch (IOException | RocksDBException e) {
            throw new RuntimeException("Error while removing entry from RocksDB", e);
        }
    }

    /** 该方法目的在于生成存储KEY, 存储KEY输出到({@link #keySerializationStream}) */
    protected void writeCurrentKeyWithGroupAndNamespace() throws IOException {
        writeKeyWithGroupAndNamespace(
                backend.getCurrentKeyGroupIndex(),
                backend.getCurrentKey(),
                currentNamespace,
                keySerializationStream,
                keySerializationDataOutputView
        );
    }

    protected void writeKeyWithGroupAndNamespace(int keyGroup,
                                                 K key,
                                                 N namespace,
                                                 ByteArrayOutputStreamWithPos keySerializationStream,
                                                 DataOutputView keySerializationDataOutputView) throws IOException {
        writeKeyWithGroupAndNamespace(
                keyGroup,
                key,
                backend.getKeySerializer(),
                namespace,
                namespaceSerializer,
                keySerializationStream,
                keySerializationDataOutputView);
    }

    protected void writeKeyWithGroupAndNamespace(
            final int keyGroup,
            final K key,
            final TypeSerializer<K> keySerializer,
            final N namespace,
            final TypeSerializer<N> namespaceSerializer,
            final ByteArrayOutputStreamWithPos keySerializationStream,
            final DataOutputView keySerializationDataOutputView) throws IOException {
        // 读取数据时, 需要初始化:
        //      KeyedStateBackend.setCurrentKey()
        //      InternalKvState.setCurrentNamespace();
        Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");
        Preconditions.checkNotNull(keySerializer);
        Preconditions.checkNotNull(namespaceSerializer);

        keySerializationStream.reset();
        RocksDBKeySerializationUtils.writeKeyGroup(keyGroup, backend.getKeyGroupPrefixBytes(), keySerializationDataOutputView);
        RocksDBKeySerializationUtils.writeKey(key, keySerializer, keySerializationStream, keySerializationDataOutputView, ambiguousKeyPossible);
        RocksDBKeySerializationUtils.writeNameSpace(namespace, namespaceSerializer, keySerializationStream, keySerializationDataOutputView, ambiguousKeyPossible);
    }

    protected V getDefaultValue() {
        if (defaultValue != null) {
            return valueSerializer.copy(defaultValue);
        } else {
            return null;
        }
    }
}
