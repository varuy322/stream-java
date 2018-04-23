package com.sdu.stream.storm.schema.action;

/**
 *
 * @author hanhan.zhang
 * */
public class Aggregator implements Action {

    private String topic;
    private String field;
    private int threshold;
    private String alias;

    private Aggregator(String topic, String field, int threshold, String alias) {
        this.topic = topic;
        this.field = field;
        this.threshold = threshold;
        this.alias = alias;
    }

    public String getTopic() {
        return topic;
    }

    public String getField() {
        return field;
    }

    public int getThreshold() {
        return threshold;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public ActionType actionType() {
        return ActionType.Aggregate;
    }

    public static Aggregator of(String topic, String field, int threshold, String alias) {
        return new Aggregator(topic, field, threshold, alias);
    }

}
