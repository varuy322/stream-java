package com.sdu.storm.topology.types.base;

import com.sdu.storm.topology.types.TupleObject;

import static com.sdu.storm.topology.types.TupleObject.TupleType.TUPLE_LONG;

public class TupleLong extends TupleObject<Long> {

    private TupleLong(Long value) {
        super(value);
    }

    @Override
    public TupleType tupleType() {
        return TUPLE_LONG;
    }

    public static TupleLong of(String value) {
        return new TupleLong(Long.valueOf(value));
    }

    public static TupleLong of(long value) {
        return new TupleLong(value);
    }

}
