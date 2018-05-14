package com.sdu.storm.topology.window;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IComponent;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * {@link IWindowBolt}保证窗口和状态状态一一对应
 *
 * @author hanhan.zhang
 * */
interface IWindowBolt<T extends Tuple> extends IComponent {

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    void cleanup();

    /**
     * 每个窗口都有属于自身的状态, 该方法在execute方法前被调用
     *
     * @param window: 上游Component发送的Tuple数据所属的Window
     * */
    Object initWindowState(TimeWindow window);

    /**
     * @param tuple: 上游Component发送的Tuple数据
     * @param windowState: Tuple数据所属的Window状态
     * @param window: Tuple所属的窗口(Tuple可能属于多个窗口, 该方法会被迭代调用)
     * */
    void execute(T tuple, Object windowState, TimeWindow window);

    /**当窗口计算完成后清除窗口*/
    void purgeWindow(Object windowState, TimeWindow window);

}
