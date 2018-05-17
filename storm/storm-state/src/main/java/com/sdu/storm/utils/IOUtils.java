package com.sdu.storm.utils;

public class IOUtils {

    /**
     * Closes the given AutoCloseable.
     *
     * <p><b>Important:</b> This method is expected to never throw an exception.
     */
    public static void closeQuietly(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Throwable ignored) {}
    }

}
