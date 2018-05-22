package com.sdu.stream.state;

import java.io.IOException;

public interface InternalValueState<N, K, V> extends InternalKvState<N, K, V> {

    V value(N namespace, K userKey) throws IOException;

    void update(N namespace, K userKey, V value) throws IOException;

}
