package com.sdu.stream.storm.schema;

import java.util.List;
import java.util.Map;

/**
 * RTD数据流处理配置信息
 *
 * @author hanhan.zhang
 * */
public class RTDConf {

    private int version;

    private Map<String, JSONSchema> topicJsonSchemas;

    private List<RTDActionSchema> actionSchemas;

    public RTDConf(int version, Map<String, JSONSchema> topicJsonSchemas, List<RTDActionSchema> actionSchemas) {
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

    public List<RTDActionSchema> getActionSchemas() {
        return actionSchemas;
    }
}
