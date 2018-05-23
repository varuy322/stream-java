package com.sdu.stream.state;

import java.io.IOException;
import java.util.List;

public interface InternalListState<N, K, T>
        extends InternalAppendingState<N, K, T, List<T>> {

    void update(N namespace, K userKey, List<T> values) throws IOException;

    void addAll(N namespace, K userKey, List<T> values) throws IOException;

}
