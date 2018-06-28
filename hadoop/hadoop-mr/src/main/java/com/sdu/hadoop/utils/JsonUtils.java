package com.sdu.hadoop.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * @author hanhan.zhang
 * */
public class JsonUtils {

    private static final Gson GSON = new GsonBuilder().create();

    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }

}
