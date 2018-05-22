package com.sdu.storm.topology.types.base;

import com.sdu.storm.topology.types.TupleObject;

public class TupleBoolean extends TupleObject<Boolean> {

    private TupleBoolean(Boolean value) {
        super(value);
    }

    @Override
    public TupleType tupleType() {
        return TupleType.TUPLE_BOOLEAN;
    }

    public static TupleBoolean of(String value) {
        return new TupleBoolean(Boolean.valueOf(value));
    }

    public static TupleBoolean of(boolean value) {
        return new TupleBoolean(value);
    }
}
