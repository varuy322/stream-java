package com.sdu.stream.storm.storage;

import java.util.Map;

public interface StorageBackendProvider {

    IStorageBackend newStorageBackend(String namespace, Map stormConf);

}
