package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.Preconditions;

public abstract class AbstractKeyedStateBackend<K> implements KeyedStateBackend<K> {

    /** {@link TypeSerializer} for our key. */
    protected final TypeSerializer<K> keySerializer;

    /** The currently active key. */
    protected K currentKey;

    /** The key group of the currently active key */
    private int currentKeyGroup;

    /** The number of key-groups aka max parallelism */
    protected final int numberOfKeyGroups;

    /** Range of key-groups for which this backend is responsible */
    protected final KeyGroupRange keyGroupRange;



    public AbstractKeyedStateBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange) {
        this.keySerializer = Preconditions.checkNotNull(keySerializer);
        this.numberOfKeyGroups = Preconditions.checkNotNull(numberOfKeyGroups);
        this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
    }

    /**
     * Creates and returns a new {@link ListState}.
     *
     * @param namespaceSerializer TypeSerializer for the state namespace.
     * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
     *
     * @param <N> The type of the namespace.
     * @param <T> The type of the values that the {@code ListState} can store.
     */
    protected abstract <N, T> InternalListState<K, N, T> createListState(
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<T> stateDesc) throws Exception;

    @Override
    public void setCurrentKey(K newKey) {
        this.currentKey = newKey;
        this.currentKeyGroup = KeyGroupRangeAssignment.assignToKeyGroup(newKey, numberOfKeyGroups);
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public K getCurrentKey() {
        return currentKey;
    }

    @Override
    public int getCurrentKeyGroupIndex() {
        return currentKeyGroup;
    }

    @Override
    public int getNumberOfKeyGroups() {
        return numberOfKeyGroups;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }
}
