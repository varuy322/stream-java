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

    public String getName() {
        return name;
    }

    public static ColumnType getColumnType(String name) {
        ColumnType[] types = ColumnType.values();
        for (ColumnType type : types) {
            if (type.getName().equals(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Can't find column type, type: " + name);
    }
}
