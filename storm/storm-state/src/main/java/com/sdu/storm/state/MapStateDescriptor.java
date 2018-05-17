package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.state.typeutils.base.MapSerializer;

import java.util.Map;

public class MapStateDescriptor<UK, UV> extends StateDescriptor<MapState<UK, UV>, Map<UK, UV>>{

    public MapStateDescriptor(String name,
                              TypeSerializer<UK> keySerializer,
                              TypeSerializer<UV> valueSerializer,
                              Map<UK, UV> defaultValue) {
        super(name, new MapSerializer<>(keySerializer, valueSerializer), defaultValue);


    }

    @Override
    public Type getType() {
        return Type.MAP;
    }
}
