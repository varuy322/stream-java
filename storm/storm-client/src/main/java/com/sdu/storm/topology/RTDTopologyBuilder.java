package com.sdu.storm.topology;

import com.sdu.storm.topology.window.BasicWindowBolt;
import com.sdu.storm.topology.window.BasicWindowedExecutor;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class RTDTopologyBuilder extends TopologyBuilder {

    public BoltDeclarer setBolt(String id, BasicWindowBolt windowBolt, Number parallelism) {
        return setBolt(id, new BasicWindowedExecutor(windowBolt), parallelism);
    }
}
