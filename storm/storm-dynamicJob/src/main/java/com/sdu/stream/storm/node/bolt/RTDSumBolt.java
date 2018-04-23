package com.sdu.stream.storm.node.bolt;

import org.apache.storm.redis.state.RedisKeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RTDSumBolt extends RTDBaseStatefulBolt<RedisKeyValueState<String, Integer>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RTDSumBolt.class);

    private String topic;

    public RTDSumBolt(String topic) {
        this.topic = topic;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void schemaUpdate(int version, String schemaJson) {

    }

    @Override
    public void executeBySchema(Tuple tuple) {

    }

    @Override
    public void initState(RedisKeyValueState<String, Integer> state) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this.topic, new Fields(RTD_ACTION_TOPIC, RTD_ACTION_TYPE, RTD_ACTION_RESULT));
    }
}
