package com.sdu.storm.topology;

import com.sdu.storm.topology.window.BasicWindowBolt;
import com.sdu.storm.topology.window.BasicWindowedBoltExecutor;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;

public class RTDTopologyBuilder extends TopologyBuilder {

    public BoltDeclarer setBolt(String id, BasicWindowBolt<Tuple> windowBolt, Number parallelism) {
        return setBolt(id, new BasicWindowedBoltExecutor(windowBolt), parallelism);
    }
}
