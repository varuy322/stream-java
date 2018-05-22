package com.sdu.stream.state;

import java.io.IOException;

public interface InternalValueState<N, K, V> extends InternalKvState<N, K, V> {

    V value(N namespace, K key) throws IOException;

    void update(N namespace, K key) throws IOException;

}
