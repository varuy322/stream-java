package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class BooleanSerializer extends TypeSerializerSingleton<Boolean> {

    public static final BooleanSerializer INSTANCE = new BooleanSerializer();

    private static final Boolean FALSE = Boolean.FALSE;

    @Override
    public Boolean createInstance() {
        return FALSE;
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
    public void serialize(Boolean record, DataOutputView target) throws IOException {
        target.writeBoolean(record);
    }

    @Override
    public Boolean deserialize(DataInputView source) throws IOException {
        return source.readBoolean();
    }

    @Override
    public Boolean copy(Boolean from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof BooleanSerializer;
    }
}
