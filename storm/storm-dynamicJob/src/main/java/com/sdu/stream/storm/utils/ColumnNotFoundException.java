package com.sdu.stream.storm.utils;

public class ColumnNotFoundException extends Exception {

    public ColumnNotFoundException(String column) {
        super("Column not found, name: " + column);
    }
}
