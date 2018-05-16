package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.state.typeutils.TypeSerializer;

public abstract class TypeSerializerSingleton<T> extends TypeSerializer<T> {

    @Override
    public TypeSerializerSingleton<T> duplicate() {
        return this;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TypeSerializerSingleton) {
            TypeSerializerSingleton<?> other = (TypeSerializerSingleton<?>) obj;

            return other.canEqual(this);
        } else {
            return false;
        }
    }

}
