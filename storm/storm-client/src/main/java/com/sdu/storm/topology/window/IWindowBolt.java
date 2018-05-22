package com.sdu.storm.topology.window;

import com.sdu.storm.topology.types.WindowTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;

import java.util.List;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
interface IWindowBolt extends IComponent {

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    void cleanup();

    /** delete window from state backend */
    void execute(List<WindowTuple> windowTuples, TimeWindow window);
}
