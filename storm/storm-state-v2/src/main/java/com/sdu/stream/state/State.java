package com.sdu.stream.state;

public interface State<N, K> {

    void clear(N namespace, K userKey);

}
