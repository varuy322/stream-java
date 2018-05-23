package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BooleanSerializer extends TypeSerializer<Boolean> {

    public static final BooleanSerializer INSTANCE = new BooleanSerializer();

    private BooleanSerializer() {}

    @Override
    public void serializer(Boolean record, DataOutput target) throws IOException {
        target.writeBoolean(record);
    }

    @Override
    public Boolean deserialize(DataInput source) throws IOException {
        return source.readBoolean();
    }

}
