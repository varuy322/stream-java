package com.sdu.stream.storm.utils;

import java.util.List;

public class Quota {

    private List<String> fields;

    private String alias;

    public Quota(List<String> fields, String alias) {
        this.fields = fields;
        this.alias = alias;
    }

    public List<String> getFields() {
        return fields;
    }

    public String getAlias() {
        return alias;
    }
}
