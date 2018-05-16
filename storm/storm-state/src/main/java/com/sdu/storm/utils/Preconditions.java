package com.sdu.storm.utils;

import javax.annotation.Nullable;

import static java.lang.String.format;

public class Preconditions {

    public static void checkArgument(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean condition, @Nullable Object errorMessage) {
        if (!condition) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static void checkArgument(boolean condition,
                                     @Nullable String errorMessageTemplate,
                                     @Nullable Object... errorMessageArgs) {

        if (!condition) {
            throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static <T> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }

    public static <T> T checkNotNull(T reference, @Nullable String errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }

    public static void checkState(boolean condition, @Nullable Object errorMessage) {
        if (!condition) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

}
