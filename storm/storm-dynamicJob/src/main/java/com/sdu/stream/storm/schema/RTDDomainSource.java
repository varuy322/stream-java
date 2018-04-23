package com.sdu.stream.storm.schema;

import java.util.Map;

/**
 * Topic Json数据解析配置
 *
 * @author hanhan.zhang
 * */
public class RTDDomainSource {

    /**kafka topic*/
    private String topic;

    /**数据标准化*/
    private Map<String, JsonSchema> standard;

    public RTDDomainSource(String topic, Map<String, JsonSchema> standard) {
        this.topic = topic;
        this.standard = standard;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, JsonSchema> getStandard() {
        return standard;
    }

    public static class JsonSchema {
        /**Json路径*/
        private String path;
        /**Json数据类型*/
        private String type;

        public JsonSchema(String path, String type) {
            this.path = path;
            this.type = type;
        }

        public String getPath() {
            return path;
        }

        public String getType() {
            return type;
        }
    }

}
