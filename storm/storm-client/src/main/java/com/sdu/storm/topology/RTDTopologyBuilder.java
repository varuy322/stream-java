package com.sdu.storm.topology;

import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.TopologyBuilder;

public class RTDTopologyBuilder extends TopologyBuilder {

    public BoltDeclarer setBolt(String id, IWindowedBolt bolt, WindowStateStoreType type) throws IllegalArgumentException {
        switch (type) {
            case REDIS:
                return null;
            case MEMORY:
                return super.setBolt(id, bolt);
            default:
                throw new IllegalArgumentException("Unsupported window store type: " + type);
        }
    }


    public BoltDeclarer setBolt(String id, IWindowedBolt bolt, Number parallelism_hint, WindowStateStoreType type) throws IllegalArgumentException {
        switch (type) {
            case REDIS:
                return null;
            case MEMORY:
                return super.setBolt(id, bolt, parallelism_hint);
            default:
                throw new IllegalArgumentException("Unsupported window store type: " + type);
        }
    }

    public enum WindowStateStoreType {
        REDIS, MEMORY
    }
}
