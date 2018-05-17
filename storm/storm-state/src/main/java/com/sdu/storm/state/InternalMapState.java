package com.sdu.storm.state;

import java.util.Map;

/**
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <UK> Type of the values folded into the state
 * @param <UV> Type of the value in the state
 * */
public interface InternalMapState<K, N, UK, UV> extends InternalKvState<K, N, Map<UK, UV>>, MapState<UK, UV>{
}
