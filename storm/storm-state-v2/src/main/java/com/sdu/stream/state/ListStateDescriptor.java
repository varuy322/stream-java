package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.util.List;

public class ListStateDescriptor<T> extends StateDescriptor<List<T>> {

    public ListStateDescriptor(String name, TypeSerializer<List<T>> serializer, List<T> defaultValue) {
        super(name, serializer, defaultValue);
    }
}
