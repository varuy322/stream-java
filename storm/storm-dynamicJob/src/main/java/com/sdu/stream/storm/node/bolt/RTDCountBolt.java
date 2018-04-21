package com.sdu.stream.storm.node.bolt;

import com.sdu.stream.storm.schema.RTDConf;
import com.sdu.stream.storm.schema.action.CountAction;
import org.apache.storm.state.InMemoryKeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 订阅"Topic"数据流并对字段"求和"
 *
 * @author hanhan.zhang
 * */
public class RTDCountBolt extends RTDBaseStatefulBolt<InMemoryKeyValueState<String, Number>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RTDCountBolt.class);

    private String topic;

    private OutputCollector collector;

    private transient int version;
    private transient CountAction action;

    public RTDCountBolt(String topic) {
        this.topic = topic;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        RTDConf conf = checkAndGetRTDConf(stormConf);

    }

    @Override
    public void initState(InMemoryKeyValueState<String, Number> state) {

    }

    @Override
    public void schemaUpdate(int version, String schemaJson) {

    }

    @Override
    public void executeBySchema(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
