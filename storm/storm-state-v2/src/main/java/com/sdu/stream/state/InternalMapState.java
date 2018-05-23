package com.sdu.stream.state;

import java.util.Map;

public interface InternalMapState<N, K, UK, UV> extends InternalKvState<N, K, Map<UK, UV>> {

    UV get(N namespace, K userKey, UK field) throws Exception;

    void put(N namespace, K userKey, UK field, UV value) throws Exception;

    void putAll(N namespace, K userKey, Map<UK, UV> values) throws Exception;

    void remove(N namespace, K userKey, UK field) throws Exception;

    boolean contains(N namespace, K userKey, UK field) throws Exception;

    Iterable<UK> keys(N namespace, K userKey) throws Exception;
}
