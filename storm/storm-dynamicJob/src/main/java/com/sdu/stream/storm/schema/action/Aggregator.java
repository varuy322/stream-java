package com.sdu.stream.storm.schema.action;

import com.sdu.stream.storm.parse.DataRow;
import com.sdu.stream.storm.utils.Quota;
import com.sdu.stream.storm.utils.RTDCalculateException;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;

import java.util.List;
import java.util.Map;

/**
 *
 * @author hanhan.zhang
 * */
public class Aggregator implements Action<String, Object> {

    private static final int DEFAULT_AGGREGATE_LENGTH = 30;

    private int length;

    private String topic;

    private List<Quota> quotas;

    public Aggregator(String topic, List<Quota> quotas) {
        this(topic, quotas, DEFAULT_AGGREGATE_LENGTH);
    }

    public Aggregator(String topic, List<Quota> quotas, int length) {
        this.length = length;
        this.topic = topic;
        this.quotas = quotas;
    }

    public int getLength() {
        return length;
    }

    public String getTopic() {
        return topic;
    }

    public List<Quota> getQuotas() {
        return quotas;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        
    }

    @Override
    public void setState(KeyValueState<String, Object> keyValueState) {

    }

    @Override
    public void execute(DataRow dataRow) throws RTDCalculateException {

    }

    @Override
    public ActionType actionType() {
        return ActionType.AGGREGATE;
    }

}
