package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.schema.JSONSchema;
import com.sdu.stream.storm.schema.Schema;
import com.sdu.stream.storm.utils.RTDType;

public class DataParserFactory {

    @SuppressWarnings("unchecked")
    public static <T> DataParser<T> createDataParser(RTDType type, String stream, Schema schema) {
        switch (type) {
            case JSON:
                if (schema instanceof JSONSchema) {
                    return (DataParser<T>) new JSONDataParser(stream, (JSONSchema) schema);
                }
                throw new IllegalArgumentException("Parameter schema should be JSONSchema !!!");
            default:
                throw new IllegalArgumentException("");
        }
    }

}
