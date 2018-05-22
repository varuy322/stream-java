package com.sdu.storm.topology.types.base;

import com.sdu.storm.topology.types.TupleObject;

public class TupleInt extends TupleObject<Integer> {

    private TupleInt(Integer value) {
        super(value);
    }

    @Override
    public TupleType tupleType() {
        return TupleType.TUPLE_INT;
    }

    public static TupleInt of(String value) {
        return new TupleInt(Integer.parseInt(value));
    }

    public static TupleInt of(int value) {
        return new TupleInt(value);
    }
}
