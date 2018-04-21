package com.sdu.stream.storm.node.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * RTD数据流统计数据输出
 *
 * @author hanhan.zhang
 * */
public class RTDSinkBolt extends RTDBaseRichBolt {

    @Override
    public void schemaUpdate(int version, String schemaJson) {

    }

    @Override
    public void executeBySchema(Tuple tuple) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
