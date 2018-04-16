package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.utils.ColumnType;

public class DataRow {

    private String stream;

    private String[] columnNames;

    private ColumnType[] columnTypes;

    private Object[] columnValues;

    private DataRow(String stream, String[] columnNames, ColumnType[] columnTypes, Object[] columnValues) {
        this.stream = stream;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnValues = columnValues;
    }

    public static DataRow of(String stream, String[] columnNames, ColumnType[] columnTypes, Object[] columnValues) {
        return new DataRow(stream, columnNames, columnTypes, columnValues);
    }
}
