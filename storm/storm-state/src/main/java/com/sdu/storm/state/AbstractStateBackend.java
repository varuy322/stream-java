package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;

import java.io.IOException;
import java.util.Map;

public abstract class AbstractStateBackend implements StateBackend {

    @Override
    public abstract  <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Map stormConf,
            String component,
            int taskId,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange) throws IOException ;
}
