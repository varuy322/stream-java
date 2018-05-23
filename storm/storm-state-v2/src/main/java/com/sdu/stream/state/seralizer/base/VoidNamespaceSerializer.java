package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.seralizer.VoidNamespace;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VoidNamespaceSerializer extends TypeSerializer<VoidNamespace> {

    public static final VoidNamespaceSerializer INSTANCE = new VoidNamespaceSerializer();

    private VoidNamespaceSerializer() {}

    @Override
    public void serializer(VoidNamespace record, DataOutput target) throws IOException {
        // Make progress in the stream, write one byte.
        //
        // We could just skip writing anything here, because of the way this is
        // used with the state backends, but if it is ever used somewhere else
        // (even though it is unlikely to happen), it would be a problem.
        target.write(0);
    }

    @Override
    public VoidNamespace deserialize(DataInput source) throws IOException {
        source.readByte();
        return VoidNamespace.get();
    }
}
