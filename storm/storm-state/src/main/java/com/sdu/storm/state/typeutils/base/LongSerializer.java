package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class LongSerializer extends TypeSerializerSingleton<Long> {

    public static final LongSerializer INSTANCE = new LongSerializer();

    private static final Long ZERO = 0L;

    private LongSerializer() {}

    @Override
    public Long createInstance() {
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
    public void serialize(Long record, DataOutputView target) throws IOException {
        target.writeLong(record);
    }

    @Override
    public Long deserialize(DataInputView source) throws IOException {
        return source.readLong();
    }

    @Override
    public Long copy(Long from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof LongSerializer;
    }
}
