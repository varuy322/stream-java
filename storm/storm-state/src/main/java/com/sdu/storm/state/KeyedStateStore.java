package com.sdu.storm.state;

public interface KeyedStateStore {

    <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties);

    <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties);

    <UK, UV> MapState<UK,UV> getMapState(MapStateDescriptor<UK, UV> stateProperties);
}
