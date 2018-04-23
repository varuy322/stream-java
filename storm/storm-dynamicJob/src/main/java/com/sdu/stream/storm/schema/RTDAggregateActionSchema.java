package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.schema.action.Aggregator;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public class RTDAggregateActionSchema implements RTDActionSchema<Aggregator> {

    private Map<String, Aggregator> aggregateActions;

    public RTDAggregateActionSchema(Map<String, Aggregator> aggregateActions) {
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
    public Aggregator getTopicAction(String topic) {
        assert aggregateActions != null;
        return aggregateActions.get(topic);
    }
}
