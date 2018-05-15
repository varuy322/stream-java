package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;

public interface InternalKvState<K, N, V> extends State {

    TypeSerializer<K> getKeySerializer();

    TypeSerializer<N> getNamespaceSerializer();

    TypeSerializer<V> getValueSerializer();

    void setCurrentNamespace(N namespace);

    byte[] getSerializedValue(
            final byte[] serializedKeyAndNamespace,
            final TypeSerializer<K> safeKeySerializer,
            final TypeSerializer<N> safeNamespaceSerializer,
            final TypeSerializer<V> safeValueSerializer) throws Exception;
}
