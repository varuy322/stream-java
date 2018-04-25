package com.sdu.stream.storm.storage;

import java.util.Map;

public class MemoryStorageBackedProvider implements StorageBackendProvider {

    @Override
    public IStorageBackend newStorageBackend(String namespace, Map stormConf) {
        return null;
    }

}
