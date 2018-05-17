package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;

import static com.sdu.storm.state.StateDescriptor.Type.VALUE;

public class ValueStateDescriptor<T> extends StateDescriptor<ValueState<T>, T> {

    public ValueStateDescriptor(String name, TypeSerializer<T> serializer, T defaultValue) {
        super(name, serializer, defaultValue);
    }

    @Override
    public Type getType() {
        return VALUE;
    }
}
