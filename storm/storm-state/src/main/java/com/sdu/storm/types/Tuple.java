package com.sdu.storm.types;

import java.io.Serializable;

public abstract class Tuple implements Serializable {

    public static final int MAX_ARITY = 25;

    private static final Class<?>[] CLASSES = new Class[] {
            Tuple2.class
    };

    public abstract <T> T getField(int pos);

    public <T> T getFieldNotNull(int pos){
        T field = getField(pos);
        if (field != null) {
            return field;
        } else {
            throw new NullFieldException(pos);
        }
    }

    public abstract <T> void setField(T value, int pos);

    public abstract int getArity();

    public abstract <T extends Tuple> T copy();

//    public abstract int

    @SuppressWarnings("unchecked")
    public static Class<? extends Tuple> getTupleClass(int arity) {
        if (arity < 0 || arity > MAX_ARITY) {
            throw new IllegalArgumentException("The tuple arity must be in [0, " + MAX_ARITY + "].");
        }
        return (Class<? extends Tuple>) CLASSES[arity];
    }

}
