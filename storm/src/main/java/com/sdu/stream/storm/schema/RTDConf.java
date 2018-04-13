package com.sdu.stream.storm.schema;

public interface RTDConf<T extends Schema> {

    T dataSchema();

}
