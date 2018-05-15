package com.sdu.storm.utils;

import java.io.DataOutput;
import java.io.IOException;

public interface DataOutputView extends DataOutput {

    void skipBytesToWrite(int numBytes) throws IOException;

    /**
     * @param source The source to copy the bytes from
     * @param numBytes The number of bytes to copy
     * */
    void write(DataInputView source, int numBytes) throws IOException;

}
