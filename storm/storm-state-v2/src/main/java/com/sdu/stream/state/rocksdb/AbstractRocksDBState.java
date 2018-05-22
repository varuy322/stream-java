package com.sdu.stream.state.rocksdb;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.sdu.stream.state.InternalKvState;
import com.sdu.stream.state.seralizer.TypeSerializer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.List;

public abstract class AbstractRocksDBState<N, K, V> implements InternalKvState<N, K, V> {

    protected RocksDBKeyedStateBackend<K> backend;

    final TypeSerializer<N> namespaceSerializer;

    /** The column family of this particular instance of state. */
    protected ColumnFamilyHandle columnFamily;

    protected final WriteOptions writeOptions;

    protected AbstractRocksDBState(
            ColumnFamilyHandle columnFamily,
            TypeSerializer<N> namespaceSerializer,
            RocksDBKeyedStateBackend<K> backend) {
        this.namespaceSerializer = namespaceSerializer;
        this.columnFamily = columnFamily;
        this.backend = backend;
        this.writeOptions = this.backend.getWriteOptions();
    }

    protected byte[] serializerNamespaceAndKey(N namespace, K userKey, ByteArrayDataOutput output) throws IOException {
        // 存储KEY构成: Namespace + Key
        TypeSerializer<N> namespaceSerializer = getNamespaceSerializer();
        namespaceSerializer.serializer(namespace, output);
        TypeSerializer<K> userKeySerializer = getKeySerializer();
        userKeySerializer.serializer(userKey, output);

        return output.toByteArray();
    }

    private byte[] serializerNamespaceAndKey(N namespace, K userKey) throws IOException {
        ByteArrayDataOutput output = ByteStreams.newDataOutput(64);
        return serializerNamespaceAndKey(namespace, userKey, output);
    }

    @Override
    public void clear(N namespace, K userKey) {
        try {
            byte[] storeKey = serializerNamespaceAndKey(namespace, userKey);
            backend.db.delete(columnFamily, writeOptions, storeKey);
        } catch (RocksDBException | IOException e) {
            throw new RuntimeException("Error while removing entry from RocksDB", e);
        }
    }
}
