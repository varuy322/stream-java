package com.sdu.storm.state;

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

}
