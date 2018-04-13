package com.sdu.stream.storm.utils;

public enum RTDType {

    JSON("json");

    private String type;

    RTDType(String type) {
        this.type = type;
    }

    public String dataType() {
        return type;
    }
}
