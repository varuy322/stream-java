package com.sdu.storm.state;

import com.sdu.storm.state.heap.InternalKeyContext;
import com.sdu.storm.utils.Disposable;

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

}
