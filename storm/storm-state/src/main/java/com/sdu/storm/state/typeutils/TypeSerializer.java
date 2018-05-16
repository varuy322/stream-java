package com.sdu.storm.state.typeutils;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

public abstract class TypeSerializer<T> implements Serializable {

    /**
     * Creates a new instance of the data type.
     *
     * @return A new instance of the data type.
     */
    public abstract T createInstance();

    /**
     * Gets the length of the data type, if it is a fix length data type.
     *
     * @return The length of the data type, or <code>-1</code> for variable length data types.
     */
    public abstract int getLength();

    /**
     * Gets whether the type is an immutable type.
     *
     * @return True, if the type is immutable.
     */
    public abstract boolean isImmutableType();

    /**
     * Serializes the given record to the given target output view.
     *
     * @param record The record to serialize.
     * @param target The output view to write the serialized data to.
     *
     * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically raised by the
     *                     output view, which may have an underlying I/O channel to which it delegates.
     */
    public abstract void serialize(T record, DataOutputView target) throws IOException;

    /**
     * De-serializes a record from the given source input view.
     *
     * @param source The input view from which to read the data.
     * @return The deserialized element.
     *
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
     *                     input view, which may have an underlying I/O channel from which it reads.
     */
    public abstract T deserialize(DataInputView source) throws IOException;

    /**
     * Creates a deep copy of this serializer if it is necessary, i.e. if it is stateful. This
     * can return itself if the serializer is not stateful.
     *
     * We need this because Serializers might be used in several threads. Stateless serializers
     * are inherently thread-safe while stateful serializers might not be thread-safe.
     */
    public abstract TypeSerializer<T> duplicate();

    /**
     * Creates a deep copy of the given element in a new element.
     *
     * @param from The element reuse be copied.
     * @return A deep copy of the element.
     */
    public abstract T copy(T from);

    public abstract boolean equals(Object obj);

    /**
     * Returns true if the given object can be equaled with this object. If not, it returns false.
     *
     * @param obj Object which wants to take part in the equality relation
     * @return true if obj can be equaled with this, otherwise false
     */
    public abstract boolean canEqual(Object obj);

    public abstract int hashCode();
}
