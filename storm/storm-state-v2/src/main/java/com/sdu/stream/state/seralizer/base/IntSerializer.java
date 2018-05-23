package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntSerializer extends TypeSerializer<Integer> {

    public static final IntSerializer INSTANCE = new IntSerializer();

    private IntSerializer() {}

    @Override
    public void serializer(Integer record, DataOutput target) throws IOException {
        target.writeInt(record);
    }

    @Override
    public Integer deserialize(DataInput source) throws IOException {
        return source.readInt();
    }
}
