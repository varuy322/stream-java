package com.sdu.stream.state;

/**
 * @param <N> State namespace
 * @param <K> State key
 * @author hanhan.zhang
 * */
public interface State<N, K> {

    void clear(N namespace, K userKey);

}
