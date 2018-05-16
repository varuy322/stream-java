package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class VoidSerializer extends TypeSerializerSingleton<Void> {

    public static final VoidSerializer INSTANCE = new VoidSerializer();

    private VoidSerializer() {}

    @Override
    public Void createInstance() {
        return null;
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
    public void serialize(Void record, DataOutputView target) throws IOException {
        target.write(0);
    }

    @Override
    public Void deserialize(DataInputView source) throws IOException {
        source.readByte();
        return null;
    }

    @Override
    public Void copy(Void from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof VoidSerializer;
    }
}
