package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class DoubleSerializer extends TypeSerializerSingleton<Double> {

    public static final DoubleSerializer INSTANCE = new DoubleSerializer();

    private static final Double ZERO = 0.0d;

    @Override
    public Double createInstance() {
        return ZERO;
    }

    @Override
    public int getLength() {
        return 8;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public void serialize(Double record, DataOutputView target) throws IOException {
        target.writeDouble(record);
    }

    @Override
    public Double deserialize(DataInputView source) throws IOException {
        return source.readDouble();
    }

    @Override
    public Double copy(Double from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof DoubleSerializer;
    }
}
