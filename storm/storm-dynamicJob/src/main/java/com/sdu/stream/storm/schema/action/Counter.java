package com.sdu.stream.storm.schema.action;

import com.google.common.collect.Lists;
import com.sdu.stream.storm.parse.DataRow;
import com.sdu.stream.storm.utils.ColumnNotFoundException;
import com.sdu.stream.storm.utils.Quota;
import com.sdu.stream.storm.utils.QuotaResult;
import com.sdu.stream.storm.utils.RTDCalculateException;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;

import java.util.List;
import java.util.Map;

/**
 * Topic下指定字段"求和"操作
 *
 * @author hanhan.zhang
 * */
public class Counter implements Action<String, Integer> {

    private String topic;

    private List<Quota> quotas;

    private KeyValueState<String, Integer> keyValueState;

    public Counter(String topic, List<Quota> quotas) {
        this.topic = topic;
        this.quotas = quotas;
    }

    public String getTopic() {
        return topic;
    }

    public List<Quota> getQuotas() {
        return quotas;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void setState(KeyValueState<String, Integer> keyValueState) {
        this.keyValueState = keyValueState;
    }

    @Override
    public ActionType actionType() {
        return ActionType.COUNT;
    }

}
