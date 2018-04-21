package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.schema.action.CountAction;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author hanhan.zhang
 * */
public class RTDCountActionSchema implements RTDActionSchema<CountAction> {

    private Map<String, CountAction> countActions;

    public RTDCountActionSchema(Map<String, CountAction> countActions) {
        this.countActions = countActions;
    }

    @Override
    public Set<String> actionTopics() {
        return countActions.keySet();
    }

    @Override
    public CountAction getTopicAction(String topic) {
        return countActions.get(topic);
    }
}
