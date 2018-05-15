package com.sdu.storm.state.typeutils;

import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

public abstract class TypeSerializer<T> implements Serializable {

    /**
     * Gets the length of the data type, if it is a fix length data type.
     *
     * @return The length of the data type, or <code>-1</code> for variable length data types.
     */
    public abstract int getLength();

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
}
