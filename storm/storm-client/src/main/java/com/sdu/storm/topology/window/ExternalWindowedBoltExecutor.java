package com.sdu.storm.topology.window;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.EvictionPolicy;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.TriggerPolicy;
import org.apache.storm.windowing.WindowLifecycleListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.storm.Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM;

/**
 * ExternalWindowedBoltExecutor组件初始化流程:
 *
 * 1: Listener
 *
 *
 * ExternalWindowedBoltExecutor消费Tuple数据流程:
 *
 * 1: 追踪并判断Tuple数据是否低于Watermark线({@link ExternalWaterMarkEventGenerator#track(GlobalStreamId, long)})
 *
 * 2: 若是Tuple数据高于Watermark线, 则Tuple数据由{@link ExternalWindowManager}管理
 *
 * 3: 若是Tuple数据低于Watermark线, 则Tuple为晚到数据且定义晚到数据流{@link #lateTupleStream}, Tuple数据由下游Bolt处理
 *
 * @author hanhan.zhang
 * */
public class ExternalWindowedBoltExecutor implements IRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalWindowedBoltExecutor.class);

    public static final String LATE_TUPLE_FIELD = "late_tuple";

    private final IWindowedBolt bolt;
    private TimestampExtractor timestampExtractor;

    // 管理Tuple Event及Watermark Event
    private transient ExternalWindowManager<Tuple> windowManager;
    // 触发窗口生成策略
    private transient TriggerPolicy<Tuple> triggerPolicy;
    // 窗口数据驱除策略
    private transient EvictionPolicy<Tuple> evictionPolicy;
    // 监听处理已驱除窗口内Tuple数据
    private transient WindowLifecycleListener<Tuple> lifecycleListener;
    // Watermark Event事件生成器
    private transient ExternalWaterMarkEventGenerator<Tuple> waterMarkEventGenerator;
    // 晚到数据处理流
    private transient String lateTupleStream;

    private transient ExternalWindowedOutputCollector windowedOutputCollector;

    public ExternalWindowedBoltExecutor(IWindowedBolt bolt) {
        this.bolt = bolt;
        this.timestampExtractor = bolt.getTimestampExtractor();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.windowedOutputCollector = new ExternalWindowedOutputCollector(collector);
        this.bolt.prepare(stormConf, context, this.windowedOutputCollector);
        this.lifecycleListener = buildWindowLifecycleListener();
        this.windowManager = buildExternalWindowManager(this.lifecycleListener, stormConf, context);

        if (waterMarkEventGenerator != null) {
            LOGGER.debug("Watermark event generator started ...");
            waterMarkEventGenerator.start();
        }
        LOGGER.info("Window trigger policy started ...");
        triggerPolicy.start();
    }

    @Override
    public void execute(Tuple input) {
        // step1: 判断是否可从Tuple获取时间戳
        // step2: 默认使用系统时间(即Tuple流入Storm系统的时间戳)
        if (isTupleTs()) {
            // step1': 判断当前Tuple是否已超时
            long ts = timestampExtractor.extractTimestamp(input);
            if (waterMarkEventGenerator.track(input.getSourceGlobalStreamId(), ts)) {
                // 当前Tuple在水位线之上, 应当存储在WindowManager中
                windowManager.add(input, ts);
            } else {
                // 当前Tuple在水位线之下, 认为Tuple已过期, 发送到过期数据处理流中(对当前Tuple消费确认)
                if (lateTupleStream != null) {
                    windowedOutputCollector.emit(lateTupleStream, input, new Values(input));
                } else {
                    LOGGER.info("Received a late tuple {} with ts {}. This will not be processed.", input, ts);
                }
                windowedOutputCollector.ack(input);
            }
        } else {
            windowManager.add(input);
        }
    }

    @Override
    public void cleanup() {
        if (waterMarkEventGenerator != null) {
            waterMarkEventGenerator.shutdown();
        }
        windowManager.shutdown();
        bolt.cleanup();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 如果定义晚到数据流处理, 则Bolt输出晚到的Tuple数据
        String lateTupleStream = (String) getComponentConfiguration().get(TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
        if (lateTupleStream != null) {
            declarer.declareStream(lateTupleStream, new Fields(LATE_TUPLE_FIELD));
        }
        bolt.declareOutputFields(declarer);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return bolt.getComponentConfiguration();
    }

    private boolean isTupleTs() {
        return timestampExtractor != null;
    }

    private WindowLifecycleListener<Tuple> buildWindowLifecycleListener() {
        return new WindowLifecycleListener<Tuple>() {
            @Override
            public void onExpiry(List<Tuple> events) {
                // 窗口内已驱除Tuple数据, 需要消费确认(ack)
                for (Tuple tuple : events) {
                    windowedOutputCollector.ack(tuple);
                }
            }

            @Override
            public void onActivation(List<Tuple> events, List<Tuple> newEvents, List<Tuple> expired) {
                // TODO:
            }
        };
    }

    private ExternalWindowManager<Tuple> buildExternalWindowManager(WindowLifecycleListener<Tuple> listener, Map stormConf, TopologyContext context) {
        return null;
    }

    private static class ExternalWindowedOutputCollector extends OutputCollector {

        ExternalWindowedOutputCollector(IOutputCollector delegate) {
            super(delegate);
        }
    }
}
