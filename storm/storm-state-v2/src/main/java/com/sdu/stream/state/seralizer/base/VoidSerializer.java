package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VoidSerializer extends TypeSerializer<Void> {

    public static final VoidSerializer INSTANCE = new VoidSerializer();

    private VoidSerializer() {}

    @Override
    public void serializer(Void record, DataOutput target) throws IOException {
        target.write(0);
    }

    @Override
    public Void deserialize(DataInput source) throws IOException {
        source.readByte();
        return null;
    }
}
