package com.sdu.storm.state.typeutils.base;

import com.sdu.storm.configuration.ConfigConstants;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class StringSerializer extends TypeSerializerSingleton<String> {

    public static final StringSerializer INSTANCE = new StringSerializer();

    private static final String EMPTY = "";

    private StringSerializer() {}

    @Override
    public String createInstance() {
        return EMPTY;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public void serialize(String record, DataOutputView target) throws IOException {
        byte[] bytes = record.getBytes(ConfigConstants.DEFAULT_CHARSET);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public String deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte []bytes = new byte[length];
        source.read(bytes);
        return new String(bytes, ConfigConstants.DEFAULT_CHARSET);
    }

    @Override
    public String copy(String from) {
        return null;
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof StringSerializer;
    }
}
