package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class IntSerializer extends TypeSerializerSingleton<Integer> {

    public static final IntSerializer INSTANCE = new IntSerializer();

    private static final Integer ZERO = 0;

    private IntSerializer() {}

    @Override
    public Integer createInstance() {
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
    public void serialize(Integer record, DataOutputView target) throws IOException {
        target.writeInt(record);
    }

    @Override
    public Integer deserialize(DataInputView source) throws IOException {
        return source.readInt();
    }

    @Override
    public Integer copy(Integer from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof IntSerializer;
    }
}
