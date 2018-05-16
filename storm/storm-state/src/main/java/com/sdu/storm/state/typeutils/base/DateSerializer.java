package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;
import java.util.Date;

public class DateSerializer extends TypeSerializerSingleton<Date> {

    public static final DateSerializer INSTANCE = new DateSerializer();

    private DateSerializer() {};

    @Override
    public Date createInstance() {
        return new Date();
    }

    @Override
    public int getLength() {
        return 8;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public void serialize(Date record, DataOutputView target) throws IOException {
        if (record == null) {
            target.writeLong(Long.MIN_VALUE);
        } else {
            target.writeLong(record.getTime());
        }
    }

    @Override
    public Date deserialize(DataInputView source) throws IOException {
        long v = source.readLong();
        if (v == Long.MIN_VALUE) {
            return null;
        }
        return new Date(v);
    }

    @Override
    public Date copy(Date from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof DateSerializer;
    }
}
