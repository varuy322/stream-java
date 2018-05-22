package com.sdu.stream.state.rocksdb;

import com.sdu.stream.state.InternalValueState;
import com.sdu.stream.state.seralizer.TypeSerializer;
import org.rocksdb.ColumnFamilyHandle;

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
        return null;
    }

    @Override
    public void update(N namespace, K userKey, T value) throws IOException {

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
