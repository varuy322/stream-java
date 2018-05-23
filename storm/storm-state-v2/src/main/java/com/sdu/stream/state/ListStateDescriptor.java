package com.sdu.stream.state;

import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.seralizer.base.ListSerializer;

import java.util.List;

public class ListStateDescriptor<T> extends StateDescriptor<List<T>>  {

    public ListStateDescriptor(String name,
                               TypeSerializer<T> elementSerializer,
                               List<T> defaultValue) {
        super(name, new ListSerializer<>(elementSerializer), defaultValue);
    }

    public TypeSerializer<T> getElementSerializer() {
        ListSerializer<T> serializer = (ListSerializer<T>) getSerializer();
        return serializer.getElementSerializer();
    }

}
