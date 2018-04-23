package com.sdu.stream.storm.schema.action;

import com.sdu.stream.storm.parse.DataRow;
import com.sdu.stream.storm.utils.RTDCalculateException;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public interface Action<K, V> {

    void prepare(Map stormConf, TopologyContext context);

    void setState(KeyValueState<K, V> keyValueState);

    void execute(DataRow dataRow) throws RTDCalculateException;

    ActionType actionType();

}
