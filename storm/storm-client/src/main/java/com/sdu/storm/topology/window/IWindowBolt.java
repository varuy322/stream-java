package com.sdu.storm.topology.window;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
interface IWindowBolt<T extends Tuple> extends IComponent {

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    void cleanup();

    /** delete window from state backend */
    void execute(List<T> windowTuples, TimeWindow window);
}
