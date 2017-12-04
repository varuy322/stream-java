package com.sdu.stream.flink.utils;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/**
 * @author hanhan.zhang
 * */
public class GsonUtils {

    private static final Gson GSON = new GsonBuilder().create();

    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }

    public static <T> T fromJson(String json, Class<T> cls) {
        return GSON.fromJson(json, cls);
    }

    public static <T> T fromJson(String json, Type type) {
        return GSON.fromJson(json, type);
    }

    public static void main(String[] args) {
        Map<String, String> map = Maps.newHashMap();
        map.put("Test", "A");
        String json = toJson(map);

        map = fromJson(json, new TypeToken<Map<String, String>>() {}.getType());
        System.out.println("Map: " + map);
    }
}
