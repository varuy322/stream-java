package com.sdu.storm.state.typeutils.base;

import com.google.common.collect.Lists;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;
import com.sdu.storm.utils.Preconditions;

import java.io.IOException;
import java.util.List;

public class ListSerializer<T> extends TypeSerializer<List<T>> {

    /** The serializer for the elements of the list */
    private final TypeSerializer<T> elementSerializer;

    public ListSerializer(TypeSerializer<T> elementSerializer) {
        this.elementSerializer = elementSerializer;
    }

    @Override
    public List<T> createInstance() {
        return Lists.newLinkedList();
    }

    @Override
    public int getLength() {
        // List序列化长度非固定, 则返回-1
        return -1;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public void serialize(List<T> record, DataOutputView target) throws IOException {
        Preconditions.checkNotNull(record);
        target.writeInt(record.size());
        for (T element : record) {
            elementSerializer.serialize(element, target);
        }
    }

    @Override
    public List<T> deserialize(DataInputView source) throws IOException {
        int size = source.readInt();
        List<T> records = Lists.newLinkedList();
        for (int i = 0; i < size; ++i) {
            records.add(elementSerializer.deserialize(source));
        }
        return records;
    }

    @Override
    public TypeSerializer<List<T>> duplicate() {
        TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
        return duplicateElement == elementSerializer ? this : new ListSerializer<>(duplicateElement);
    }

    @Override
    public List<T> copy(List<T> from) {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
                (obj != null && obj.getClass() == getClass() &&
                        elementSerializer.equals(((ListSerializer<?>) obj).elementSerializer));
    }

    @Override
    public boolean canEqual(Object obj) {
        return true;
    }

    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }

    /**
     * Gets the serializer for the elements of the list.
     * @return The serializer for the elements of the list
     */
    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

}
