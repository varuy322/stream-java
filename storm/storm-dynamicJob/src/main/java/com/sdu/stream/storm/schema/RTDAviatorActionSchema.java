package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.schema.action.Aviator;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public class RTDAviatorActionSchema implements RTDActionSchema<Aviator> {

    private Map<String, Aviator> aviatorActions;

    public RTDAviatorActionSchema(Map<String, Aviator> aviatorActions) {
        this.aviatorActions = aviatorActions;
    }

    @Override
    public Set<String> actionTopics() {
        if (aviatorActions == null) {
            return emptySet();
        }
        return aviatorActions.keySet();
    }

    @Override
    public Aviator getTopicAction(String topic) {
        assert aviatorActions != null;
        return aviatorActions.get(topic);
    }
}
