package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class DateSerializer extends TypeSerializer<Date> {

    public static final DateSerializer INSTANCE = new DateSerializer();

    private DateSerializer() {}

    @Override
    public void serializer(Date record, DataOutput target) throws IOException {
        if (record == null) {
            target.writeLong(Long.MIN_VALUE);
        } else {
            target.writeLong(record.getTime());
        }
    }

    @Override
    public Date deserialize(DataInput source) throws IOException {
        long v = source.readLong();
        if (v == Long.MIN_VALUE) {
            return null;
        }
        return new Date(v);
    }
}
