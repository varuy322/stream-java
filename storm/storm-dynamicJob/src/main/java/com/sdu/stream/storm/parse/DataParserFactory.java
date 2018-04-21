package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.schema.JSONSchema;
import com.sdu.stream.storm.schema.Schema;
import com.sdu.stream.storm.utils.RTDType;

public class DataParserFactory {

    @SuppressWarnings("unchecked")
    public static <T> DataParser<T> createDataParser(String topic, Schema schema) {
        if (schema instanceof JSONSchema) {
            return (DataParser<T>) new JSONDataParser(topic, (JSONSchema) schema);
        }
        throw new IllegalArgumentException("Parameter schema should be JSONSchema !!!");
    }

}
