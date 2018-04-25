package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.schema.action.Action;

import java.util.List;
import java.util.Map;

/**
 * RTD数据流处理配置信息
 *
 * @author hanhan.zhang
 * */
public class RTDDomainSchema {

    private int version;

    /**
     * 存储主键:
     *
     * Sum/WindowCount: domainKey + field  ==> Domain
     *
     * Aggregate: domainKey + secondKey ==> Domain
     *
     * Join:
     * */
    private String domainKey;

    /**数据源*/
    private List<RTDDomainSource> source;

    /**数据源计算*/
    private Map<String, List<Action>> operation;

    public RTDDomainSchema(int version, String domainKey, List<RTDDomainSource> source, Map<String, List<Action>> operation) {
        this.version = version;
        this.domainKey = domainKey;
        this.source = source;
        this.operation = operation;
    }

    public int getVersion() {
        return version;
    }

    public String getDomainKey() {
        return domainKey;
    }

    public List<RTDDomainSource> getSource() {
        return source;
    }

    public List<Action> getOperation(String action) {
        assert operation != null;
        return this.operation.get(action);
    }
}
