package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class FloatSerializer extends TypeSerializerSingleton<Float> {

    public static final FloatSerializer INSTANCE = new FloatSerializer();

    private static final Float ZERO = 0f;

    private FloatSerializer() {}

    @Override
    public Float createInstance() {
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
    public void serialize(Float record, DataOutputView target) throws IOException {
        target.writeFloat(record);
    }

    @Override
    public Float deserialize(DataInputView source) throws IOException {
        return source.readFloat();
    }

    @Override
    public Float copy(Float from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof FloatSerializer;
    }
}
