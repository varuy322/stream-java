package com.sdu.stream.storm.schema.action;

import java.util.List;

public class Sum implements Action {

    private String topic;
    private List<String> fields;
    private String alias;

    private Sum(String topic, List<String> fields, String alias) {
        this.topic = topic;
        this.fields = fields;
        this.alias = alias;
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getFields() {
        return fields;
    }

    public String getAlias() {
        return alias;
    }

    @Override
    public ActionType actionType() {
        return ActionType.Sum;
    }

    public static Sum of(String topic, List<String> fields, String alias) {
        return new Sum(topic, fields, alias);
    }
}
