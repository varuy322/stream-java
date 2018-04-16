package com.sdu.stream.storm.schema;

import com.google.common.collect.Maps;
import com.sdu.stream.storm.utils.ColumnType;

import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class JSONSchema implements Schema {

    private String schemaName;

    // key: 列名, value: json path
    private Map<String, String> jsonConf;

    // key: 列名, value: 类型
    private Map<String, ColumnType> fieldType;

    public JSONSchema(String schemaName, Map<String, String> jsonConf) {
        this.schemaName = schemaName;
        this.jsonConf = jsonConf;
        this.fieldType = Maps.newHashMap();
        for (Map.Entry<String, String> entry : this.jsonConf.entrySet()) {
            this.fieldType.put(entry.getKey(), ColumnType.String);
        }
    }

    public JSONSchema(String schemaName, Map<String, String> jsonConf, Map<String, ColumnType> fieldType) {
        this.schemaName = schemaName;
        this.jsonConf = jsonConf;
        this.fieldType = fieldType;
    }

    public int schemaLength() {
        return this.jsonConf.size();
    }

    public Map<String, String> schemaJSONConf() {
        return this.jsonConf;
    }

    public Map<String, ColumnType> schemaType() {
        return this.fieldType;
    }

    @Override
    public String schemaName() {
        return this.schemaName;
    }
}
