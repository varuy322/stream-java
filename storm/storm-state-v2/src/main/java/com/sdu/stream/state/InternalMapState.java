package com.sdu.stream.state;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public interface InternalMapState<N, K, UK, UV> extends InternalKvState<N, K, Map<UK, UV>> {

    UV get(N namespace, K userKey, UK elementKey) throws IOException;

    void put(N namespace, K userKey, UK elementKey, UV elementValue) throws IOException;

    void putAll(N namespace, K userKey, Map<UK, UV> elements) throws IOException;

    void remove(N namespace, K userKey, UK elementKey) throws IOException;

    boolean contains(N namespace, K userKey, UK elementKey) throws IOException;

    Iterable<Map.Entry<UK, UV>> entries(N namespace, K userKey) throws IOException;

    Iterable<UK> keys(N namespace, K userKey) throws IOException;

    Iterable<UV> values(N namespace, K userKey) throws IOException;

    Iterator<Map.Entry<UK, UV>> iterator(N namespace, K userKey) throws IOException;
}
