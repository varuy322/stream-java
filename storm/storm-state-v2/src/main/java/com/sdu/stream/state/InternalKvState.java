package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;

public interface InternalKvState<N, K, V> extends State<N, K> {

    TypeSerializer<N> getNamespaceSerializer();

    TypeSerializer<K> getKeySerializer();

}
