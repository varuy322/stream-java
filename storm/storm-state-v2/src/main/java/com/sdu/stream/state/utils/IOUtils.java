package com.sdu.stream.state.utils;

public class IOUtils {

    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Throwable ignored) {}
    }

}
