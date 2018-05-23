package com.sdu.stream.state.seralizer.base;

import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.utils.ConfigConstants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringSerializer extends TypeSerializer<String> {

    public static final StringSerializer INSTANCE = new StringSerializer();

    private StringSerializer() {}

    @Override
    public void serializer(String record, DataOutput target) throws IOException {
        byte[] bytes = record.getBytes(ConfigConstants.DEFAULT_CHARSET);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public String deserialize(DataInput source) throws IOException {
        int length = source.readInt();
        byte []bytes = new byte[length];
        source.readFully(bytes);
        return new String(bytes, ConfigConstants.DEFAULT_CHARSET);
    }
}
