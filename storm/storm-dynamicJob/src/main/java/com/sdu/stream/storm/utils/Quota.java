package com.sdu.stream.storm.utils;

import java.util.Map;

public class Quota {

    private String topic;

    // key: 存储类型, value: 命名空间
    private Map<StorageType, String> quotaStorageKeys;

    public Quota(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public Map<StorageType, String> getQuotaStorageKeys() {
        return quotaStorageKeys;
    }

    public void setQuotaStorageKeys(Map<StorageType, String> quotaStorageKeys) {
        this.quotaStorageKeys = quotaStorageKeys;
    }

    public enum  StorageType {
        HINCRBY
    }
}
