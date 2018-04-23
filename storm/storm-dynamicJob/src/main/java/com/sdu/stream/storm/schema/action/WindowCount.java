package com.sdu.stream.storm.schema.action;

import java.util.List;

public class WindowCount implements Action {

    private String topic;
    private List<String> fields;
    private int threshold;
    private String alias;

    private WindowCount(String topic, List<String> fields, int threshold, String alias) {
        this.topic = topic;
        this.fields = fields;
        this.threshold = threshold;
        this.alias = alias;
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getFields() {
        return fields;
    }

    public int getThreshold() {
        return threshold;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public ActionType actionType() {
        return ActionType.WindowCount;
    }

    public static WindowCount of(String topic, List<String> fields, int threshold, String alias) {
        return new WindowCount(topic, fields, threshold, alias);
    }

}
