package com.sdu.stream.storm.schema.action;

import com.sdu.stream.storm.utils.Quota;

import java.util.List;

/**
 * Topic下指定字段"求和"操作
 *
 * @author hanhan.zhang
 * */
public class CountAction implements Action {

    private static final int THRESHOLD_MS = 60 * 1000;

    private String topic;

    private List<Quota> quotas;

    private int thresholdMs;

    public CountAction(String topic, List<Quota> quotas) {
        this(topic, quotas, THRESHOLD_MS);
    }

    public CountAction(String topic, List<Quota> quotas, int thresholdMs) {
        this.topic = topic;
        this.quotas = quotas;
        this.thresholdMs = thresholdMs;
    }

    public String getTopic() {
        return topic;
    }

    public List<Quota> getQuotas() {
        return quotas;
    }

    public int getThresholdMs() {
        return thresholdMs;
    }

    @Override
    public ActionType actionType() {
        return ActionType.COUNT;
    }

}
