package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.schema.action.AggregateAction;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public class RTDAggregateActionSchema implements RTDActionSchema<AggregateAction> {

    private Map<String, AggregateAction> aggregateActions;

    public RTDAggregateActionSchema(Map<String, AggregateAction> aggregateActions) {
        this.aggregateActions = aggregateActions;
    }

    @Override
    public Set<String> actionTopics() {
        if (aggregateActions == null) {
            return emptySet();
        }
        return aggregateActions.keySet();
    }

    @Override
    public AggregateAction getTopicAction(String topic) {
        assert aggregateActions != null;
        return aggregateActions.get(topic);
    }
}
