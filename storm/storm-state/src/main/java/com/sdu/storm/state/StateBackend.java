package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 *
 * @author hanhan.zhang
 * */
public interface StateBackend extends Serializable {

    <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Map stormConf,
            String component,
            int taskId,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange) throws IOException;

}
