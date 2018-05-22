package com.sdu.storm.topology.types;

import java.io.Serializable;

public abstract class TupleObject<V> implements Serializable {

    private V value;

    public TupleObject(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    public abstract TupleType tupleType();

    public enum TupleType {
        TUPLE_STRING,
        TUPLE_INT,
        TUPLE_SHORT,
        TUPLE_BYTE,
        TUPLE_LONG,
        TUPLE_FLOAT,
        TUPLE_DOUBLE,
        TUPLE_BOOLEAN
    }
}

