package com.sdu.stream.storm.utils;

import org.apache.storm.tuple.Fields;

public class RTDConf {
    // 实时数据Schema
    public static final String RTD_SCHEMA_CONF = "RTD_SCHEMA_CONF";

    // 标准化流
    public static final String RTD_STANDARD_STREAM = "RTD_STANDARD_STREAM";
    // 标准化流输出
    public static final Fields RTD_STANDARD_OUTPUT_FIELDS = new Fields("topic", "fields");


}
