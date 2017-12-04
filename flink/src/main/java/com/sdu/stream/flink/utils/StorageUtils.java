package com.sdu.stream.flink.utils;

import com.sdu.stream.flink.storage.*;
import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.accumulators.IntCounter;

import java.util.Map;

import static com.sdu.stream.flink.storage.StorageConf.*;
import static com.sdu.stream.flink.utils.GsonUtils.fromJson;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

/**
 * @author hanhan.zhang
 * */
public class StorageUtils {

    private static volatile StorageBackend storageBackend;

    static {

    }

    public static StorageBackend getStorageBackend(GlobalJobParameters conf) {
        if (storageBackend == null) {
            synchronized (StorageUtils.class) {
                if (storageBackend == null) {
                    StorageConf storageConf = fromGlobalConf(conf);
                    String storageType = storageConf.getString(STORAGE_TYPE, "Memory");
                    switch (storageType) {
                        case MEMORY_STORAGE:
                            storageBackend = new MemoryStorageBackend();
                            break;
                        case ROCKSDB_STORAGE:
                            storageBackend = new RocksDBStorageBackend();
                            break;
                        case SQUIRREL_STORAGE:
                            storageBackend = new RedisStorageBackend();
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported storage type: " + storageType);
                    }
                    try {
                        storageBackend.init(storageConf);
                    } catch (Exception e) {
                        throw new RuntimeException("StorageBackend initializer failure", e);
                    }
                }
            }
        }
        return storageBackend;
    }

    private static StorageConf fromGlobalConf(GlobalJobParameters conf) {
        StorageConf storageConf = new StorageConf();
        Map<String, String> settings = conf.toMap();
        settings.forEach((key, value) -> {
            if (key.startsWith("storage.")) {
                storageConf.setString(key, value);
            }
        });
        return storageConf;
    }

    public static String createStorageKey(String storagePrefix, String storageKey) {
        return String.format("%s_%s", storagePrefix, storageKey);
    }

    public static  <T> T getCacheData(StorageBackend backend, IntCounter metric, String key, Class<T> cls) {
        T result = null;
        try {
            String value = backend.get(key);
            if (isNotEmpty(value)) {
                result = fromJson(value, cls);
                backend.remove(key);
            }
        } catch (Exception e) {
            metric.add(1);
        }
        return result;
    }

}
