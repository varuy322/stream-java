package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.seralizer.base.MapSerializer;

import java.util.Map;

public class MapStateDescriptor<UK, UV> extends StateDescriptor<Map<UK, UV>> {

    public MapStateDescriptor(
            String name,
            TypeSerializer<UK> userKeySerializer,
            TypeSerializer<UV> userValueSerializer,
            Map<UK, UV> defaultValue) {
        super(name, new MapSerializer<>(userKeySerializer, userValueSerializer), defaultValue);
    }

    public TypeSerializer<UK> getUserKeySerializer() {
        MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) getSerializer();
        return serializer.getKeySerializer();
    }

    public TypeSerializer<UV> getUserValueSerializer() {
        MapSerializer<UK, UV> serializer = (MapSerializer<UK, UV>) getSerializer();
        return serializer.getValueSerializer();
    }
}
