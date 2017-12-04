package com.sdu.stream.flink.storage;

import com.google.common.collect.Maps;

import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.math.NumberUtils.toInt;
import static org.apache.commons.lang3.math.NumberUtils.toLong;

/**
 * @author hanhan.zhang
 * */
public class StorageConf {

    public static final String STORAGE_EXPIRE_TIME = "storage.expire.time";

    public static final String STORAGE_TYPE = "storage.type";
    public static final String MEMORY_STORAGE = "Memory";
    public static final String ROCKSDB_STORAGE = "RocksDB";
    public static final String SQUIRREL_STORAGE = "Squirrel";

    // RocksDB Conf
    public static final String ROCKSDB_ROOT_DIR = "storage.rocksdb.root.dir";
    public static final String ROCKSDB_DELETE_ON_EXIT = "storage.rocksdb.delete.on.exit";

    private Map<String, String> conf;

    public StorageConf() {
        this.conf = Maps.newConcurrentMap();
    }

    public StorageConf(Map<String, String> conf) {
        this.conf = Maps.newConcurrentMap();
        this.conf.putAll(conf);
    }

    public void setString(String key, String value) {
        this.conf.putIfAbsent(key, value);
    }

    public String getString(String key, String defaultValue) {
        return conf.getOrDefault(key, defaultValue);
    }

    private boolean getBoolean(String key, boolean defaultValue) {
        String value = conf.get(key);
        if (isEmpty(value)) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    public int getInt(String key, int defaultValue) {
        String value = conf.get(key);
        return toInt(value, defaultValue);
    }

    private long getLong(String key, long defaultValue) {
        String value = conf.get(key);
        return toLong(value, defaultValue);
    }

    // RocksDB Conf
    public String getRocksDBDir(String defaultValue) {
        return getString(ROCKSDB_ROOT_DIR, defaultValue);
    }

    public boolean isRocksDBDeleteOnExit(boolean defaultValue) {
        return getBoolean(ROCKSDB_DELETE_ON_EXIT, defaultValue);
    }

    public long getStorageExpireTime(long defaultValue) {
        return getLong(STORAGE_EXPIRE_TIME, defaultValue);
    }
}
