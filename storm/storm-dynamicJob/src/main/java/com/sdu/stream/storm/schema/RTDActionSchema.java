package com.sdu.stream.storm.schema;

import com.sdu.stream.storm.schema.action.Action;

import java.util.Set;

/**
 * RTD数据流处理配置信息
 *
 * @author hanhan.zhang
 * */
public interface RTDActionSchema<T extends Action> {

    Set<String> actionTopics();

    T getTopicAction(String topic);

}
