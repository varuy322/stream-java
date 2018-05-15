package com.sdu.storm.types;

import com.sdu.storm.utils.StringUtils;

public class Tuple2<T0, T1> extends Tuple {

    private T0 f0;
    private T1 f1;

    public Tuple2() { }

    public Tuple2(T0 f0, T1 f1) {
        this.f0 = f0;
        this.f1 = f1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getField(int pos) {
        switch(pos) {
            case 0: return (T) this.f0;
            case 1: return (T) this.f1;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void setField(T value, int pos) {
        switch(pos) {
            case 0:
                this.f0 = (T0) value;
                break;
            case 1:
                this.f1 = (T1) value;
                break;
            default: throw new IndexOutOfBoundsException(String.valueOf(pos));
        }
    }

    @Override
    public int getArity() {
        return 2;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple2<T0, T1> copy() {
        return new Tuple2<>(f0, f1);
    }

    @Override
    public String toString() {
        return "(" + StringUtils.arrayAwareToString(this.f0)
                + "," + StringUtils.arrayAwareToString(this.f1)
                + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Tuple2)) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        Tuple2 tuple = (Tuple2) o;
        if (f0 != null ? !f0.equals(tuple.f0) : tuple.f0 != null) {
            return false;
        }
        if (f1 != null ? !f1.equals(tuple.f1) : tuple.f1 != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = f0 != null ? f0.hashCode() : 0;
        result = 31 * result + (f1 != null ? f1.hashCode() : 0);
        return result;
    }

    public static <T0, T1> Tuple2<T0, T1> of(T0 value0, T1 value1) {
        return new Tuple2<>(value0, value1);
    }
}
