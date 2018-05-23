package com.sdu.stream.state.seralizer.base;

import com.google.common.collect.Lists;
import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.utils.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class ListSerializer<T> extends TypeSerializer<List<T>> {

    /** The serializer for the elements of the list */
    private final TypeSerializer<T> elementSerializer;

    public ListSerializer(TypeSerializer<T> elementSerializer) {
        this.elementSerializer = elementSerializer;
    }

    @Override
    public void serializer(List<T> record, DataOutput target) throws IOException {
        Preconditions.checkNotNull(record);
        target.writeInt(record.size());
        for (T element : record) {
            elementSerializer.serializer(element, target);
        }
    }

    @Override
    public List<T> deserialize(DataInput source) throws IOException {
        int size = source.readInt();
        List<T> records = Lists.newLinkedList();
        for (int i = 0; i < size; ++i) {
            records.add(elementSerializer.deserialize(source));
        }
        return records;
    }

    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

}
