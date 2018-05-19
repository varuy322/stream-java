package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.state.typeutils.VoidNamespace;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class VoidNamespaceSerializer extends TypeSerializerSingleton<VoidNamespace> {

    public static final VoidNamespaceSerializer INSTANCE = new VoidNamespaceSerializer();

    @Override
    public VoidNamespace createInstance() {
        return VoidNamespace.get();
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public void serialize(VoidNamespace record, DataOutputView target) throws IOException {
        // Make progress in the stream, write one byte.
        //
        // We could just skip writing anything here, because of the way this is
        // used with the state backends, but if it is ever used somewhere else
        // (even though it is unlikely to happen), it would be a problem.
        target.write(0);
    }

    @Override
    public VoidNamespace deserialize(DataInputView source) throws IOException {
        source.readByte();
        return VoidNamespace.get();
    }

    @Override
    public VoidNamespace copy(VoidNamespace from) {
        return from;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof VoidSerializer;
    }
}
