package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleSerializer extends TypeSerializer<Double> {

    public static final DoubleSerializer INSTANCE = new DoubleSerializer();


    @Override
    public void serializer(Double record, DataOutput target) throws IOException {
        target.writeDouble(record);
    }

    @Override
    public Double deserialize(DataInput source) throws IOException {
        return source.readDouble();
    }
}
