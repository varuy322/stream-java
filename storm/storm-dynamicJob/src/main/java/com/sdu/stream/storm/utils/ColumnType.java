package com.sdu.stream.storm.utils;

public enum ColumnType {

    String("String", java.lang.String.class);

    private String name;

    private Class<?> cls;

    ColumnType(String name, Class<?> cls) {
        this.name = name;
        this.cls = cls;
    }

    public Class<?> columnType() {
        return cls;
    }
}
