package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ShortSerializer extends TypeSerializer<Short> {

    public static final ShortSerializer INSTANCE = new ShortSerializer();

    private ShortSerializer() { }

    @Override
    public void serializer(Short record, DataOutput target) throws IOException {
        target.writeShort(record);
    }

    @Override
    public Short deserialize(DataInput source) throws IOException {
        return source.readShort();
    }
}
