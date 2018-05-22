package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class ShortSerializer extends TypeSerializerSingleton<Short> {

    public static final ShortSerializer INSTANCE = new ShortSerializer();

    private static final Short ZERO = 0;

    private ShortSerializer() { }

    @Override
    public Short createInstance() {
        return ZERO;
    }

    @Override
    public int getLength() {
        return 4;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public void serialize(Short record, DataOutputView target) throws IOException {
        target.writeShort(record);
    }

    @Override
    public Short deserialize(DataInputView source) throws IOException {
        return source.readShort();
    }

    @Override
    public Short copy(Short from) {
        return from;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof ShortSerializer;
    }
}
