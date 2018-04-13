package com.sdu.stream.storm.node.spout;

/**
 * @author hanhan.zhang
 * */
public class RTDSchema {

    private int version;

    private String schema;

    public RTDSchema(int version, String schema) {
        this.version = version;
        this.schema = schema;
    }

    public int getVersion() {
        return version;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RTDSchema rtdSchema = (RTDSchema) o;

        return version == rtdSchema.version;
    }

    @Override
    public int hashCode() {
        return version;
    }
}
