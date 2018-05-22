package com.sdu.stream.state.seralizer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public abstract class TypeSerializer<T> implements Serializable {

    public abstract void serializer(T element, DataOutput output) throws IOException;

    public abstract T deserialize(DataInput input) throws IOException;

}
