package com.sdu.storm.utils;

import com.sdu.storm.configuration.ConfigConstants;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;

import static com.sdu.storm.utils.Preconditions.checkNotNull;

public class StringUtils {

    private static final char[] HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    public static void writeNullableString(@Nullable String str, DataOutputView out) throws IOException {
        if (str != null) {
            out.writeBoolean(true);
            writeString(str, out);
        } else {
            out.writeBoolean(false);
        }
    }

    private static void writeString(String text, DataOutputView out)  throws IOException {
        checkNotNull(text);
        // 空串处理
        byte[] bytes = text.getBytes(ConfigConstants.DEFAULT_CHARSET);
        out.writeInt(bytes.length);
        if (bytes.length != 0) {
            out.write(bytes);
        }
    }

    public static @Nullable String readNullableString(DataInputView in) throws IOException {
        if (in.readBoolean()) {
            return readString(in);
        } else {
            return null;
        }
    }

    private static String readString(DataInputView in) throws IOException {
        int length = in.readInt();
        if (length == 0) {
            return new String("".getBytes(),  ConfigConstants.DEFAULT_CHARSET);
        }

        byte[] bytes = new byte[length];
        in.read(bytes);

        return new String(bytes, ConfigConstants.DEFAULT_CHARSET);
    }

    public static String arrayAwareToString(Object o) {
        if (o == null) {
            return "null";
        }
        if (o.getClass().isArray()) {
            return arrayToString(o);
        }

        return o.toString();
    }

    private static String arrayToString(Object array) {
        if (array == null) {
            throw new NullPointerException();
        }

        if (array instanceof int[]) {
            return Arrays.toString((int[]) array);
        }
        if (array instanceof long[]) {
            return Arrays.toString((long[]) array);
        }
        if (array instanceof Object[]) {
            return Arrays.toString((Object[]) array);
        }
        if (array instanceof byte[]) {
            return Arrays.toString((byte[]) array);
        }
        if (array instanceof double[]) {
            return Arrays.toString((double[]) array);
        }
        if (array instanceof float[]) {
            return Arrays.toString((float[]) array);
        }
        if (array instanceof boolean[]) {
            return Arrays.toString((boolean[]) array);
        }
        if (array instanceof char[]) {
            return Arrays.toString((char[]) array);
        }
        if (array instanceof short[]) {
            return Arrays.toString((short[]) array);
        }

        if (array.getClass().isArray()) {
            return "<unknown array type>";
        } else {
            throw new IllegalArgumentException("The given argument is no array.");
        }
    }

    public static String byteToHexString(final byte[] bytes) {
        return byteToHexString(bytes, 0, bytes.length);
    }

    public static String byteToHexString(final byte[] bytes, final int start, final int end) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes == null");
        }

        int length = end - start;
        char[] out = new char[length * 2];

        for (int i = start, j = 0; i < end; i++) {
            out[j++] = HEX_CHARS[(0xF0 & bytes[i]) >>> 4];
            out[j++] = HEX_CHARS[0x0F & bytes[i]];
        }

        return new String(out);
    }
}
