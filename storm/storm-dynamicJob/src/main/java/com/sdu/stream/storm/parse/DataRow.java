package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.utils.ColumnNotFoundException;
import com.sdu.stream.storm.utils.ColumnType;

import java.util.Map;

public class DataRow {

    private String topic;

    private Map<String, ColumnSchema> columns;

    private DataRow(String topic, Map<String, ColumnSchema> columns) {
        this.topic = topic;
        this.columns = columns;
    }

    public String getTopic() {
        return topic;
    }

    @SuppressWarnings("unchecked")
    public <T> T getColumnValue(String column) throws ColumnNotFoundException {
        assert columns != null;
        ColumnSchema schema = columns.get(column);
        if (schema == null) {
            throw new ColumnNotFoundException(column);
        }

        return (T) schema.value;
    }

    public static DataRow of(String topic, Map<String, ColumnSchema> columns) {
        return new DataRow(topic, columns);
    }

    public static class ColumnSchema {
        private String name;
        private ColumnType type;
        private Object value;

        ColumnSchema(String name, ColumnType type, Object value) {
            this.name = name;
            this.type = type;
            this.value = value;
        }
    }
}
