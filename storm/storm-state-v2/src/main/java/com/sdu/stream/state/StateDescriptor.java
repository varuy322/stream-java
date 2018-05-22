package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.Serializable;

public abstract class StateDescriptor<T> implements Serializable {

    protected String name;

    protected TypeSerializer<T> serializer;

    protected transient T defaultValue;

    public StateDescriptor(String name, TypeSerializer<T> serializer, T defaultValue) {
        this.name = name;
        this.serializer = serializer;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public TypeSerializer<T> getSerializer() {
        return serializer;
    }

    public T getDefaultValue() {
        return defaultValue;
    }
}
