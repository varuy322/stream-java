package com.sdu.stream.flink.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * @author hanhan.zhang
 * */
public class MemoryStorageBackend implements StorageBackend {

    private static final long DEFAULT_EXPIRE_TIME = 5 * 60;

    private Cache<String, String> cache;

    @Override
    public void init(StorageConf conf) throws Exception {
        long expire = conf.getStorageExpireTime(DEFAULT_EXPIRE_TIME);
        cache = CacheBuilder.newBuilder()
                            .expireAfterWrite(expire, TimeUnit.SECONDS)
                            .build();
    }

    @Override
    public void put(String key, String value, int timeoutSecond) throws Exception {
        throw new UnsupportedOperationException("memory storage unifier manage expire, please use 'put(String key, String value)'");
    }

    @Override
    public void put(String key, String value) throws Exception {
        cache.put(key, value);
    }

    @Override
    public String get(String key) throws Exception {
        return cache.getIfPresent(key);
    }

    @Override
    public void remove(String key) throws Exception {
        cache.invalidate(key);
    }

    @Override
    public void dispose() throws Exception {
        if (cache != null) {
            cache.invalidateAll();
            cache = null;
        }
    }
}
