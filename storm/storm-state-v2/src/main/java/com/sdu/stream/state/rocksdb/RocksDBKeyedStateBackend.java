package com.sdu.stream.state.rocksdb;

import com.sdu.stream.state.InternalListState;
import com.sdu.stream.state.KeyedStateBackend;
import com.sdu.stream.state.ListStateDescriptor;
import com.sdu.stream.state.seralizer.TypeSerializer;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.stream.Stream;

public class RocksDBKeyedStateBackend<KEY> implements KeyedStateBackend<KEY> {

    /** RocksDB instance */
    protected RocksDB db;

    @Override
    public <N> Stream<KEY> getKeys(String state, N namespace) {
        return null;
    }

    @Override
    public <N, T> InternalListState<N, KEY, T> createListState(TypeSerializer<N> namespaceSerializer,
                                             ListStateDescriptor<T> stateDesc) throws IOException {
        return null;
    }

    @Override
    public TypeSerializer<KEY> getKeyTypeSerializer() {
        return null;
    }

    public WriteOptions getWriteOptions() {
        throw new RuntimeException("");
    }

}
