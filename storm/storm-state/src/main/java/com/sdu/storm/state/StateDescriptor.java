package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * <p>Subclasses must correctly implement {@link #equals(Object)} and {@link #hashCode()}.
 *
 * @param <S> The type of the State objects created from this {@code StateDescriptor}.
 * @param <T> The type of the value of the state object described by this state descriptor.
 *
 * @author hanhan.zhang
 * */
public abstract class StateDescriptor<S extends State, T> implements Serializable {

    public enum Type {
        LIST,
        MAP
    }

    /** Name that uniquely identifies state created from this StateDescriptor. */
    protected final String name;

    @Nullable
    protected TypeSerializer<T> serializer;

    /** The default value returned by the state when no other value is bound to a key. */
    @Nullable
    protected transient T defaultValue;

    protected StateDescriptor(String name, TypeSerializer<T> serializer, T defaultValue) {
        this.name = name;
        this.serializer = serializer;
        this.defaultValue = defaultValue;
    }

    /**
     * Returns the name of this {@code StateDescriptor}.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the {@link TypeSerializer} that can be used to serialize the value in the state.
     */
    public TypeSerializer<T> getSerializer() {
        if (serializer != null) {
            return serializer.duplicate();
        } else {
            throw new IllegalStateException("Serializer not yet initialized.");
        }
    }

    /**
     * Returns the default value.
     */
    public T getDefaultValue() {
        if (defaultValue != null) {
            if (serializer != null) {
                return serializer.copy(defaultValue);
            } else {
                throw new IllegalStateException("Serializer not yet initialized.");
            }
        } else {
            return null;
        }
    }

    public abstract Type getType();
}
