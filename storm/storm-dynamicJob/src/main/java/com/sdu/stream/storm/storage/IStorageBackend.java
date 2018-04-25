package com.sdu.stream.storm.storage;

import java.util.Map;
import java.util.concurrent.Future;

public interface IStorageBackend {

    void put(String key, String value);

    void get(String key, String value);

    void hput(String key, Map<String, String> fields);
    void hput(String key, String field, String value);

    Map<String, String> hget(String key);
    String hget(String key, String field);
}
