package com.sdu.storm.topology.utils;

import com.google.gson.Gson;

public class GsonUtils {

    private static Gson GSON = new Gson();

    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }

    public static <T> T fromJson(String json, Class<T> cls) {
        return GSON.fromJson(json, cls);
    }
}
