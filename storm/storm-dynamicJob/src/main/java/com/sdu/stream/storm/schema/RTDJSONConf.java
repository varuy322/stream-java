package com.sdu.stream.storm.schema;

public class RTDJSONConf implements RTDConf<JSONSchema> {

    private JSONSchema schema;

    public RTDJSONConf(JSONSchema schema) {
        this.schema = schema;
    }

    @Override
    public JSONSchema dataSchema() {
        return schema;
    }

}
