package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CharSerializer extends TypeSerializer<Character> {

    public static final CharSerializer INSTANCE = new CharSerializer();

    private CharSerializer() {}

    @Override
    public void serializer(Character record, DataOutput target) throws IOException {
        target.writeChar(record);
    }

    @Override
    public Character deserialize(DataInput source) throws IOException {
        return source.readChar();
    }
}
