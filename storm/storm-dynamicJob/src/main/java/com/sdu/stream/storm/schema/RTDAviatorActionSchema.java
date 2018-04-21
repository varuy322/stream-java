package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.schema.action.AviatorAction;

import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

public class RTDAviatorActionSchema implements RTDActionSchema<AviatorAction> {

    private Map<String, AviatorAction> aviatorActions;

    public RTDAviatorActionSchema(Map<String, AviatorAction> aviatorActions) {
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
    public AviatorAction getTopicAction(String topic) {
        assert aviatorActions != null;
        return aviatorActions.get(topic);
    }
}
