package com.sdu.stream.storm.storage;

import org.apache.storm.Config;
import org.apache.storm.state.StateProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.sdu.stream.storm.utils.StormConf.TOPOLOGY_STORAGE_BACKEND_PROVIDER;

public class StorageBackendFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageBackendFactory.class);

    private static final String DEFAULT_PROVIDER = "com.sdu.stream.storm.storage.MemoryStorageBackedProvider";



    public static IStorageBackend getStorageBackend(String namespace, Map stormConf) {
        IStorageBackend storageBackend;

        try {
            String provider;
            if (stormConf.containsKey(TOPOLOGY_STORAGE_BACKEND_PROVIDER)) {
                provider = (String) stormConf.get(TOPOLOGY_STORAGE_BACKEND_PROVIDER);
            } else {
                provider = DEFAULT_PROVIDER;
            }
            Class<?> cls = Class.forName(provider);
            Object object = cls.newInstance();
            if (object instanceof StorageBackendProvider) {
                storageBackend = ((StorageBackendProvider) object).newStorageBackend(namespace, stormConf);
            } else {
                String msg = "Invalid state provider '" + provider +
                        "'. Should implement com.sdu.stream.storm.storage.StorageBackendProvider";
                LOGGER.error(msg);
                throw new RuntimeException(msg);
            }
        } catch (Exception e) {
            LOGGER.error("Got exception while loading the storage backend provider", e);
            throw new RuntimeException(e);
        }

        return storageBackend;
    }

}
