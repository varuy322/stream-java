package com.sdu.storm.utils;

import java.io.IOException;
import java.io.InputStream;

/**
 * 读取有限长度字节流
 *
 * @author hanhan.zhang
 * */
public class ByteArrayInputStreamWithPos extends InputStream {

    protected byte[] buffer;
    // 读取位置
    protected int position;
    // 读取字节数
    protected int count;
    // 标记位置
    protected int mark = 0;

    public ByteArrayInputStreamWithPos(byte[] buffer) {
        this(buffer, 0, buffer.length);
    }

    public ByteArrayInputStreamWithPos(byte[] buffer, int offset, int length) {
        this.position = offset;
        this.buffer = buffer;
        this.mark = offset;
        this.count = Math.min(buffer.length, offset + length);
    }

    @Override
    public int read() throws IOException {
        return position < count ? 0xFF & (buffer[position++]) : -1 ;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        Preconditions.checkNotNull(b);

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }

        if (position >= count) {
            return -1; // signal EOF
        }

        int available = count - position;

        if (len > available) {
            len = available;
        }

        if (len <= 0) {
            return 0;
        }

        System.arraycopy(buffer, position, b, off, len);
        position += len;
        return len;
    }

    @Override
    public long skip(long toSkip) {
        long remain = count - position;

        if (toSkip < remain) {
            remain = toSkip < 0 ? 0 : toSkip;
        }

        position += remain;
        return remain;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readAheadLimit) {
        mark = position;
    }

    @Override
    public void reset() {
        position = mark;
    }

    @Override
    public int available() {
        return count - position;
    }

    @Override
    public void close() throws IOException {
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int pos) {
        Preconditions.checkArgument(pos >= 0 && pos <= count, "Position out of bounds.");
        this.position = pos;
    }
}
