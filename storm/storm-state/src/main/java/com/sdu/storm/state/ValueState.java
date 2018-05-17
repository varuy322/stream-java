package com.sdu.storm.state;

import java.io.IOException;

/**
 * {@link State} interface for partitioned single-value state. The value can be retrieved or
 * updated.
 *
 * @param <T> Type of the value in the state.
 *
 * @author hanhan.zhang
 * */
public interface ValueState<T> extends State {

    /**
     * Returns the current value for the state. When the state is not
     * partitioned the returned value is the same for all inputs in a given
     * operator instance. If state partitioning is applied, the value returned
     * depends on the current operator input, as the operator maintains an
     * independent state for each partition.
     *
     * <p>If you didn't specify a default value when creating the {@link ValueStateDescriptor}
     * this will return {@code null} when to value was previously set using {@link #update(Object)}.
     *
     * @return The state value corresponding to the current input.
     *
     * @throws IOException Thrown if the system cannot access the state.
     */
    T value() throws IOException;

    /**
     * Updates the operator state accessible by {@link #value()} to the given
     * value. The next time {@link #value()} is called (for the same state
     * partition) the returned state will represent the updated value. When a
     * partitioned state is updated with null, the state for the current key
     * will be removed and the default value is returned on the next access.
     *
     * @param value The new value for the state.
     *
     * @throws IOException Thrown if the system cannot access the state.
     */
    void update(T value) throws IOException;
}

