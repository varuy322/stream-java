package com.sdu.stream.storm.node.bolt;

import org.apache.storm.redis.state.RedisKeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class RTDAggregateBolt extends RTDBaseStatefulBolt<RedisKeyValueState<String, Object>> {

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
    public void initState(RedisKeyValueState<String, Object> state) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
