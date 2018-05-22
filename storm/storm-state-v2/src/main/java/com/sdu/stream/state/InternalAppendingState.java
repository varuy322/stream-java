package com.sdu.stream.state;

import java.io.IOException;

public interface InternalAppendingState<N, K, IN, SV, OUT> extends InternalKvState<N, K, SV> {

    OUT get(N namespace, K userKey) throws IOException;

    void add(N namespace, K userKey, IN value) throws IOException;

}
