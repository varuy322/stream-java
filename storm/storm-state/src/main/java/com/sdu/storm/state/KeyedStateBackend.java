package com.sdu.storm.state;

import com.sdu.storm.state.heap.InternalKeyContext;
import com.sdu.storm.utils.Disposable;

import java.util.stream.Stream;

/**
 * 数据存储
 *
 * @author hanhan.zhang
 * */
public interface KeyedStateBackend<K> extends InternalKeyContext<K>, Disposable {

    /**
     * Sets the current key that is used for partitioned state.
     * @param newKey The new current key.
     */
    void setCurrentKey(K newKey);

    /**
     * @return A stream of all keys for the given state and namespace. Modifications to the state during iterating
     * 		   over it keys are not supported.
     * @param state State variable for which existing keys will be returned.
     * @param namespace Namespace for which existing keys will be returned.
     */
    <N> Stream<K> getKeys(String state, N namespace);
}
