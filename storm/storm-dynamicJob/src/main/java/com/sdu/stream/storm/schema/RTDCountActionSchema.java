package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.schema.action.Counter;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 *
 * @author hanhan.zhang
 * */
public class RTDCountActionSchema implements RTDActionSchema<Counter> {

    private Map<String, Counter> countActions;

    public RTDCountActionSchema(Map<String, Counter> countActions) {
        this.countActions = countActions;
    }

    @Override
    public Set<String> actionTopics() {
        if (countActions == null) {
            return emptySet();
        }
        return countActions.keySet();
    }

    @Override
    public Counter getTopicAction(String topic) {
        assert countActions != null;
        return countActions.get(topic);
    }
}
