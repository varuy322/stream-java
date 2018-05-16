package com.sdu.stream.storm.state;

import com.sdu.storm.state.AbstractKeyedStateBackend;
import com.sdu.storm.state.AbstractStateBackend;
import com.sdu.storm.state.KeyGroupRange;
import com.sdu.storm.state.typeutils.TypeSerializer;

import java.io.IOException;
import java.util.Map;

public abstract class StateBackendTestBase<B extends AbstractStateBackend> {

    // Flink StateBackend存储两类State:
    // 1: KeyStream State
    // 2: Operation State
    public abstract B getStateBackend() throws IOException;

    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Map stormConf,
                                                                    String component,
                                                                    int taskId,
                                                                    TypeSerializer<K> typeSerializer) throws IOException {
        return getStateBackend().createKeyedStateBackend(
                stormConf,
                component,
                taskId,
                typeSerializer,
                10,
                new KeyGroupRange(0, 9));
    }

}
