package com.sdu.stream.storm.utils;

import java.util.List;

public class Quota {

    private String id;

    private List<String> fields;

    private String alias;

    public Quota(String id, List<String> fields, String alias) {
        this.id = id;
        this.fields = fields;
        this.alias = alias;
    }

    public String getId() {
        return id;
    }

    public List<String> getFields() {
        return fields;
    }

    public String getAlias() {
        return alias;
    }
}
