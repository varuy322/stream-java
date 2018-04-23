package com.sdu.stream.storm.utils;

public class QuotaResult<T> {

    private String quotaName;

    private String key;

    private T value;

    private QuotaResult(String quotaName, String key, T value) {
        this.quotaName = quotaName;
        this.key = key;
        this.value = value;
    }

    public String getQuotaName() {
        return quotaName;
    }

    public String getKey() {
        return key;
    }

    public T getValue() {
        return value;
    }

    public static <T> QuotaResult of(String name, String key, T value) {
        return new QuotaResult<>(name, key, value);
    }
}
