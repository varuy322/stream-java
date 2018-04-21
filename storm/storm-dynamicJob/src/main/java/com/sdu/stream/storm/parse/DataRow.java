package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.utils.ColumnType;

public class DataRow {

    private String topic;

    private String[] columnNames;

    private ColumnType[] columnTypes;

    private Object[] columnValues;

    private DataRow(String topic, String[] columnNames, ColumnType[] columnTypes, Object[] columnValues) {
        this.topic = topic;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnValues = columnValues;
    }

    public String getTopic() {
        return topic;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public ColumnType[] getColumnTypes() {
        return columnTypes;
    }

    public Object[] getColumnValues() {
        return columnValues;
    }

    public static DataRow of(String stream, String[] columnNames, ColumnType[] columnTypes, Object[] columnValues) {
        return new DataRow(stream, columnNames, columnTypes, columnValues);
    }
}
