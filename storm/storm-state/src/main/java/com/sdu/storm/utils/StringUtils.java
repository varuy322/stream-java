package com.sdu.storm.utils;

import java.util.Arrays;

public class StringUtils {

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

}
