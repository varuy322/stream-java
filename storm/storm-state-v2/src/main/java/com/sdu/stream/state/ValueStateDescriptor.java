package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;

public class ValueStateDescriptor<T> extends StateDescriptor<T> {

    public ValueStateDescriptor(String name, TypeSerializer<T> serializer, T defaultValue) {
        super(name, serializer, defaultValue);
    }
}
