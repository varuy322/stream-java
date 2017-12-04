package com.sdu.stream.flink.storage;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface StorageBackend extends Serializable {

    void init(StorageConf conf) throws Exception;

    void put(String key, String value, int timeoutSecond) throws Exception;

    void put(String key, String value) throws Exception;

    String get(String key) throws Exception;

    void remove(String key) throws Exception;

    void dispose() throws Exception;
}
