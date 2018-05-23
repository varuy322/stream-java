package com.sdu.stream.state;

import java.io.IOException;

public interface InternalAppendingState<N, K, IN, OUT> extends InternalKvState<N, K, IN> {

    OUT get(N namespace, K userKey) throws IOException;

    void add(N namespace, K userKey, IN value) throws IOException;

}
