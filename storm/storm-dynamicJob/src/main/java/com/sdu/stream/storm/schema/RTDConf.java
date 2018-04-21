package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.utils.RTDSchemaException;

import java.util.Map;

/**
 * RTD数据流处理配置信息
 *
 * @author hanhan.zhang
 * */
public class RTDConf {

    private int version;

    private Map<String, JSONSchema> topicJsonSchemas;

    private Map<ActionSchemaType, RTDActionSchema<?>> actionSchemas;

    public RTDConf(int version, Map<String, JSONSchema> topicJsonSchemas, Map<ActionSchemaType, RTDActionSchema<?>> actionSchemas) {
        this.version = version;
        this.topicJsonSchemas = topicJsonSchemas;
        this.actionSchemas = actionSchemas;
    }

    public int getVersion() {
        return version;
    }

    public Map<String, JSONSchema> getTopicJsonSchemas() {
        return topicJsonSchemas;
    }

    @SuppressWarnings("unchecked")
    public <T> T getRTDActionSchema(ActionSchemaType type) {
        if (actionSchemas == null || actionSchemas.isEmpty()) {
            throw new RTDSchemaException("RTD action schema empty !!!");
        }
        RTDActionSchema<?> actionSchema = actionSchemas.get(type);
        if (actionSchema == null) {
            throw new RTDSchemaException("RTD action schema empty, type: " + type);
        }
        return (T) actionSchema;
    }
}
