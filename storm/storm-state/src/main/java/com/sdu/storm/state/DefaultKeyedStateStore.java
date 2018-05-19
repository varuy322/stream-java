package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.base.VoidSerializer;

import java.io.IOException;

public class DefaultKeyedStateStore implements KeyedStateStore {

    private AbstractKeyedStateBackend<?> keyedStateBackend;

    public DefaultKeyedStateStore(AbstractKeyedStateBackend<?> keyedStateBackend) {
        this.keyedStateBackend = keyedStateBackend;
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        try {
            return keyedStateBackend.createValueState(
                    VoidSerializer.INSTANCE,
                    stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        try {
            return keyedStateBackend.createListState(
                    VoidSerializer.INSTANCE,
                    stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        try {
            return keyedStateBackend.createMapState(
                    VoidSerializer.INSTANCE,
                    stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

}
