package com.sdu.storm.state;


import com.sdu.storm.utils.MathUtils;

public final class KeyGroupRangeAssignment {

    /**
     * Assigns the given key to a key-group index.
     *
     * @param key the key to assign
     * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
     * @return the key-group to which the given key is assigned
     */
    public static int assignToKeyGroup(Object key, int maxParallelism) {
        return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
    }

    /**
     * Assigns the given key to a key-group index.
     *
     * @param keyHash the hash of the key to assign
     * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
     * @return the key-group to which the given key is assigned
     */
    public static int computeKeyGroupForKeyHash(int keyHash, int maxParallelism) {
        return MathUtils.murmurHash(keyHash) % maxParallelism;
    }
}
