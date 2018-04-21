package com.sdu.stream.storm.schema.action;

import com.sdu.stream.storm.utils.Quota;

import java.util.List;

/**
 * 
 *
 * @author hanhan.zhang
 * */
public class AggregateAction implements Action {

    private static final int DEFAULT_AGGREGATE_LENGTH = 30;

    private int length;

    private String topic;

    private List<Quota> quotas;

    public AggregateAction(String topic, List<Quota> quotas) {
        this(topic, quotas, DEFAULT_AGGREGATE_LENGTH);
    }

    public AggregateAction(String topic, List<Quota> quotas, int length) {
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
    public ActionType actionType() {
        return ActionType.AGGREGATE;
    }

}
