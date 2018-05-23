package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongSerializer extends TypeSerializer<Long> {

    public static final LongSerializer INSTANCE = new LongSerializer();

    private LongSerializer() {}

    @Override
    public void serializer(Long record, DataOutput target) throws IOException {
        target.writeLong(record);
    }

    @Override
    public Long deserialize(DataInput source) throws IOException {
        return source.readLong();
    }
}
