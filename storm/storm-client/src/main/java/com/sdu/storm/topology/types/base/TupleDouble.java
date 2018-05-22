package com.sdu.storm.topology.types.base;

import com.sdu.storm.topology.types.TupleObject;

public class TupleDouble extends TupleObject<Double> {

    private TupleDouble(Double value) {
        super(value);
    }

    @Override
    public TupleType tupleType() {
        return TupleType.TUPLE_DOUBLE;
    }

    public static TupleDouble of(String value) {
        return new TupleDouble(Double.valueOf(value));
    }

    public static TupleDouble of(double value) {
        return new TupleDouble(value);
    }
}
