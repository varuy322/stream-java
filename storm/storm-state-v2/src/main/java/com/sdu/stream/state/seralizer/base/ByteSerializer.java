package com.sdu.stream.state.seralizer.base;


import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ByteSerializer extends TypeSerializer<Byte> {

    public static final ByteSerializer INSTANCE = new ByteSerializer();

    private ByteSerializer() {}

    @Override
    public void serializer(Byte record, DataOutput target) throws IOException {
        target.writeByte(record);
    }

    @Override
    public Byte deserialize(DataInput source) throws IOException {
        return source.readByte();
    }
}
