package com.sdu.storm.topology.types.base;

import com.sdu.storm.topology.types.TupleObject;

public class TupleShort extends TupleObject<Short> {

    private TupleShort(Short value) {
        super(value);
    }

    @Override
    public TupleType tupleType() {
        return TupleType.TUPLE_SHORT;
    }

    public static TupleShort of(String value) {
        return new TupleShort(Short.valueOf(value));
    }

    public static TupleShort of(short value) {
        return new TupleShort(value);
    }
}
