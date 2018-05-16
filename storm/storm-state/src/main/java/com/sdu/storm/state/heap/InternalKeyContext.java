package com.sdu.storm.state.heap;

import com.sdu.storm.state.KeyGroupRange;
import com.sdu.storm.state.typeutils.TypeSerializer;

public interface InternalKeyContext<K> {

    /**
     * Used by states to access the current key.
     */
    K getCurrentKey();

    /**
     * Returns the key-group to which the current key belongs.
     */
    int getCurrentKeyGroupIndex();

    /**
     * Returns the number of key-groups aka max parallelism.
     */
    int getNumberOfKeyGroups();

    /**
     * Returns the key groups for this backend.
     */
    KeyGroupRange getKeyGroupRange();

    /**
     * {@link TypeSerializer} for the state backend key type.
     */
    TypeSerializer<K> getKeySerializer();

}
