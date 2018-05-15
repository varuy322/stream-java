package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;

public abstract class AbstractStateBackend implements StateBackend {

    @Override
    public abstract  <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange);
}
