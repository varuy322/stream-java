package com.sdu.stream.state.rocksdb;

import com.sdu.stream.state.InternalMapState;
import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.utils.Preconditions;
import org.rocksdb.ColumnFamilyHandle;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class RocksDBMapState<N, K, UK, UV>
        extends AbstractRocksDBState<N, K, Map<UK, UV>>
        implements InternalMapState<N, K, UK, UV> {

    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;

    public RocksDBMapState(ColumnFamilyHandle columnFamily,
                           TypeSerializer<N> namespaceSerializer,
                           TypeSerializer<UK> userKeySerializer,
                           TypeSerializer<UV> userValueSerializer,
                           RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, backend);

        this.userKeySerializer = Preconditions.checkNotNull(userKeySerializer);
        this.userValueSerializer = Preconditions.checkNotNull(userValueSerializer);
    }

    @Override
    public UV get(N namespace, K userKey, UK elementKey) throws IOException {
        return null;
    }

    @Override
    public void put(N namespace, K userKey, UK elementKey, UV elementValue) throws IOException {

    }

    @Override
    public void putAll(N namespace, K userKey, Map<UK, UV> elements) throws IOException {

    }

    @Override
    public void remove(N namespace, K userKey, UK elementKey) throws IOException {

    }

    @Override
    public boolean contains(N namespace, K userKey, UK elementKey) throws IOException {
        return false;
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries(N namespace, K userKey) throws IOException {
        return null;
    }

    @Override
    public Iterable<UK> keys(N namespace, K userKey) throws IOException {
        return null;
    }

    @Override
    public Iterable<UV> values(N namespace, K userKey) throws IOException {
        return null;
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator(N namespace, K userKey) throws IOException {
        return null;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return null;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return null;
    }
}
