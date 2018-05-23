package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FloatSerializer extends TypeSerializer<Float> {

    public static final FloatSerializer INSTANCE = new FloatSerializer();

    private FloatSerializer() {}

    @Override
    public void serializer(Float record, DataOutput target) throws IOException {
        target.writeFloat(record);
    }

    @Override
    public Float deserialize(DataInput source) throws IOException {
        return source.readFloat();
    }
}
