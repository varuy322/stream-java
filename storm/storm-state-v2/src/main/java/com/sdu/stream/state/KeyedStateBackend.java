package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.IOException;
import java.util.stream.Stream;

public interface KeyedStateBackend<K> {

    <N> Stream<K> getKeys(String state, N namespace);

    <N, T> InternalListState<N, K, T> createListState(TypeSerializer<N> namespaceSerializer,
                                                      ListStateDescriptor<T> stateDesc) throws IOException;

    TypeSerializer<K> getKeyTypeSerializer();
}
