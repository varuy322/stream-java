package com.sdu.storm.topology.types.base;

import com.sdu.storm.topology.types.TupleObject;

public class TupleFloat extends TupleObject<Float> {

    private TupleFloat(Float value) {
        super(value);
    }

    @Override
    public TupleType tupleType() {
        return TupleType.TUPLE_FLOAT;
    }

    public static TupleFloat of(String value) {
        return new TupleFloat(Float.valueOf(value));
    }

    public static TupleFloat of(float value) {
        return new TupleFloat(value);
    }
}
