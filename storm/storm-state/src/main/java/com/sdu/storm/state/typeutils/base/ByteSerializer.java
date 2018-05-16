package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class ByteSerializer extends TypeSerializerSingleton<Byte> {

    public static final ByteSerializer INSTANCE = new ByteSerializer();

    private static final Byte ZERO = 0;

    private ByteSerializer() {}

    @Override
    public Byte createInstance() {
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
    public void serialize(Byte record, DataOutputView target) throws IOException {
        target.writeByte(record);
    }

    @Override
    public Byte deserialize(DataInputView source) throws IOException {
        return source.readByte();
    }

    @Override
    public Byte copy(Byte from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof ByteSerializer;
    }
}
