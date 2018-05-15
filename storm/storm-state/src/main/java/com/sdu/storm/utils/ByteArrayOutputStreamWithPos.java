package com.sdu.storm.utils;

import com.google.common.base.Preconditions;
import com.sun.istack.internal.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class ByteArrayOutputStreamWithPos extends OutputStream {

    private byte[] buffer;
    private int count;

    public ByteArrayOutputStreamWithPos() {
        this(64);
    }

    public ByteArrayOutputStreamWithPos(int size) {
        Preconditions.checkArgument(size >= 0);
        buffer = new byte[size];
    }

    private void ensureCapacity(int requiredCapacity) {
        if (requiredCapacity - buffer.length > 0) {
            increaseCapacity(requiredCapacity);
        }
    }

    private void increaseCapacity(int requiredCapacity) {
        int oldCapacity = buffer.length;
        int newCapacity = oldCapacity << 1;
        if (newCapacity - requiredCapacity < 0) {
            newCapacity = requiredCapacity;
        }
        if (newCapacity < 0) {
            if (requiredCapacity < 0) {
                throw new OutOfMemoryError();
            }
            newCapacity = Integer.MAX_VALUE;
        }
        buffer = Arrays.copyOf(buffer, newCapacity);
    }

    @Override
    public void write(int b) throws IOException {
        ensureCapacity(count + 1);
        buffer[count] = (byte) b;
        ++count;
    }

    @Override
    public void write(@NotNull byte[] b, int off, int len) throws IOException {
        if ((off < 0) || (len < 0) || (off > b.length) ||
                ((off + len) - b.length > 0)) {
            throw new IndexOutOfBoundsException();
        }

        ensureCapacity(count + len);

        System.arraycopy(b, off, buffer, count, len);
        count += len;
    }

    public void reset() {
        count = 0;
    }

    public byte toByteArray()[] {
        return Arrays.copyOf(buffer, count);
    }

    public int size() {
        return count;
    }

    public String toString() {
        return new String(buffer, 0, count, StandardCharsets.UTF_8);
    }

    public int getPosition() {
        return count;
    }

    public void setPosition(int position) {
        Preconditions.checkArgument(position >= 0, "Position out of bounds.");
        ensureCapacity(position + 1);
        count = position;
    }

    @Override
    public void close() throws IOException {
    }

    public byte[] getBuf() {
        return buffer;
    }
}
