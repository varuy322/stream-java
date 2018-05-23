package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;

/**
 * @param <N> state namespace
 * @param <K> state key
 * @param <V> state value
 * @author hanhan.zhang
 * */
public interface InternalKvState<N, K, V> extends State<N, K> {

    TypeSerializer<N> getNamespaceSerializer();

    TypeSerializer<K> getKeySerializer();

}
