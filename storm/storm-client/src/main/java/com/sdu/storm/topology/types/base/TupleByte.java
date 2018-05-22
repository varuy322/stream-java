package com.sdu.storm.topology.types.base;

import com.sdu.storm.topology.types.TupleObject;

public class TupleByte extends TupleObject<Byte> {

    private TupleByte(Byte value) {
        super(value);
    }

    @Override
    public TupleType tupleType() {
        return TupleType.TUPLE_BYTE;
    }

    public static TupleByte of(String value) {
        return new TupleByte(Byte.valueOf(value));
    }

    public static TupleByte of(Byte value) {
        return new TupleByte(value);
    }
}
