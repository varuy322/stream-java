package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;

import java.io.Serializable;

/**
 *
 * @author hanhan.zhang
 * */
public interface StateBackend extends Serializable {

    <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange);

}
