package com.sdu.stream.storm.node.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * RTD Conf Schema Check(Only One Task)
 *
 * Note:
 *
 *  After all bolt component ack schema update message, Topology spout send message with new RTD conf version
 *
 * @author hanhan.zhang
 * */
public class RTDSchemaCheckSpout implements IRichSpout {

    public static final String RTD_SCHEMA_STREAM_ID = "$RTDSchemaStream";

    public static final String RTD_SCHEMA_CHECK_INTERVAL = "rtd.schema.check.interval";

    public static final String RTD_SCHEMA_VERSION = "RTDConfVersion";
    public static final String RTD_SCHEMA_CONTENT = "RTDConfContent";

    // Topology使用的Schema Version
    private int useVersion = 0;

    private long laskschemaAckTs;

    private long schemaCheckInterval;

    private SpoutOutputCollector collector;

    @SuppressWarnings("unchecked")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.schemaCheckInterval = (long) conf.getOrDefault(RTD_SCHEMA_CHECK_INTERVAL, 180000L);

        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(RTD_SCHEMA_STREAM_ID, new Fields(RTD_SCHEMA_VERSION, RTD_SCHEMA_CONTENT));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public static boolean isSchemaCheckStream(String streamId) {
        return RTD_SCHEMA_STREAM_ID.equals(streamId);
    }

}
