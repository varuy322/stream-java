package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class CharSerializer extends TypeSerializerSingleton<Character> {

    public static final CharSerializer INSTANCE = new CharSerializer();

    private static final Character ZERO = '0';

    private CharSerializer() {}

    @Override
    public Character createInstance() {
        return ZERO;
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public void serialize(Character record, DataOutputView target) throws IOException {
        target.writeChar(record);
    }

    @Override
    public Character deserialize(DataInputView source) throws IOException {
        return source.readChar();
    }

    @Override
    public Character copy(Character from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CharSerializer;
    }
}
