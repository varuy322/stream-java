package com.sdu.storm.topology.types.base;

import com.sdu.storm.topology.types.TupleObject;

public class TupleString extends TupleObject<String> {

    private TupleString(String value) {
        super(value);
    }

    @Override
    public TupleType tupleType() {
        return TupleType.TUPLE_STRING;
    }

    public static TupleString of(String value) {
        return new TupleString(value);
    }
}
