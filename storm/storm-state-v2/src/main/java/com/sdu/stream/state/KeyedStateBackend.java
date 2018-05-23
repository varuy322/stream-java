package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.utils.Disposable;

import java.io.IOException;
import java.util.stream.Stream;

public interface KeyedStateBackend<K> extends Disposable {

    <N> Stream<K> getKeys(String state, N namespace);

    <N, T> InternalValueState<N,K, T> createValueState(TypeSerializer<N> namespaceSerializer,
                                                        ValueStateDescriptor<T> stateDesc) throws IOException;

    <N, T> InternalListState<N, K, T> createListState(TypeSerializer<N> namespaceSerializer,
                                                      ListStateDescriptor<T> stateDesc) throws IOException;

    <N, UK, UV> InternalMapState<N, K, UK, UV> createMapState(TypeSerializer<N> namespaceSerializer,
                                                              MapStateDescriptor<UK, UV> stateDesc) throws IOException;


    TypeSerializer<K> getKeyTypeSerializer();
}
