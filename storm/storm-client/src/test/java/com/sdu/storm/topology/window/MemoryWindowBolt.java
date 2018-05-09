package com.sdu.storm.topology.window;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;

import java.util.Map;

public class MemoryWindowBolt extends BaseWindowedBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(TupleWindow inputWindow) {

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
