package com.sdu.stream.storm.schema.action;

import java.util.List;

/**
 * Topic下指定字段"求和"操作
 *
 * @author hanhan.zhang
 * */
public class CountAction implements Action {

    private static final int THRESHOLD_MS = 60 * 1000;

    private String topic;

    private List<String> fields;

    private int thresholdMs;

    public CountAction(String topic, List<String> fields) {
        this(topic, fields, THRESHOLD_MS);
    }

    public CountAction(String topic, List<String> fields, int thresholdMs) {
        this.topic = topic;
        this.fields = fields;
        this.thresholdMs = thresholdMs;
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getFields() {
        return fields;
    }

    public int getThresholdMs() {
        return thresholdMs;
    }

    @Override
    public ActionType actionType() {
        return ActionType.COUNT;
    }

}
