package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.state.typeutils.base.ListSerializer;

import java.util.List;

public class ListStateDescriptor<T> extends StateDescriptor<ListState<T>, List<T>>{

    public ListStateDescriptor(String name, TypeSerializer<List<T>> serializer, List<T> defaultValue) {
        super(name, serializer, defaultValue);
    }

    public TypeSerializer<T> getElementSerializer() {
        // call getSerializer() here to get the initialization check and proper error message
        final TypeSerializer<List<T>> rawSerializer = getSerializer();
        if (!(rawSerializer instanceof ListSerializer)) {
            throw new IllegalStateException();
        }

        return ((ListSerializer<T>) rawSerializer).getElementSerializer();
    }

    @Override
    public Type getType() {
        return Type.LIST;
    }
}
