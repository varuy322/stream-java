package com.sdu.storm.utils;

import com.sun.istack.internal.NotNull;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class DataInputViewStreamWrapper extends DataInputStream implements DataInputView {

    public DataInputViewStreamWrapper(@NotNull InputStream in) {
        super(in);
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        if (skipBytes(numBytes) != numBytes){
            throw new EOFException("Could not skip " + numBytes + " bytes.");
        }
    }
}
