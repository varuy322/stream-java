package com.sdu.stream.storm.node.bolt;

import com.sdu.stream.storm.storage.IStorageBackend;
import com.sdu.stream.storm.storage.StorageBackendFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public abstract class RTDStorageBolt extends RTDBaseRichBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        String namespace = buildStorageBackendNamespace();
        IStorageBackend storageBackend = StorageBackendFactory.getStorageBackend(namespace, stormConf);
        initStorageBackend(storageBackend);
    }


    public abstract String buildStorageBackendNamespace();
    public abstract void initStorageBackend(IStorageBackend storageBackend);
}
