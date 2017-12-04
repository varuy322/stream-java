package com.sdu.stream.flink.storage;

/**
 * @author hanhan.zhang
 * */
public class RedisStorageBackend implements StorageBackend {

    @Override
    public void init(StorageConf conf) throws Exception {

    }

    @Override
    public void put(String key, String value, int timeoutSecond) throws Exception {

    }

    @Override
    public void put(String key, String value) throws Exception {

    }

    @Override
    public String get(String key) throws Exception {
        return null;
    }

    @Override
    public void remove(String key) throws Exception {

    }

    @Override
    public void dispose() throws Exception {

    }

}
