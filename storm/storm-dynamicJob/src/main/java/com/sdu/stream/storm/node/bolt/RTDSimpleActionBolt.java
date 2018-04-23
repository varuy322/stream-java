package com.sdu.stream.storm.node.bolt;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sdu.stream.storm.parse.DataRow;
import com.sdu.stream.storm.schema.RTDAggregateActionSchema;
import com.sdu.stream.storm.schema.RTDAviatorActionSchema;
import com.sdu.stream.storm.schema.RTDConf;
import com.sdu.stream.storm.schema.RTDCountActionSchema;
import com.sdu.stream.storm.schema.action.Aggregator;
import com.sdu.stream.storm.schema.action.Aviator;
import com.sdu.stream.storm.schema.action.Counter;
import com.sdu.stream.storm.utils.JsonUtils;
import org.apache.storm.redis.state.RedisKeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.sdu.stream.storm.schema.ActionSchemaType.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 订阅"Topic"数据流, 数据计算:
 *
 * 1: 字段"求和"
 *
 * 2: 字段"聚合"
 *
 * 3: EL表达式计算
 *
 * @author hanhan.zhang
 * */
public class RTDSimpleActionBolt extends RTDBaseStatefulBolt<RedisKeyValueState<String, Number>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RTDSimpleActionBolt.class);

    public static final String RTD_FLUSH_INTERVAL = "rtd.flush.interval";

    private String topic;

    private OutputCollector collector;

    private final Object versionLock = new Object();
    private int version;
    private Counter counter;
    private Aggregator aggregator;
    private Aviator aviator;

    private RedisKeyValueState<String, Number> countState;

    private ScheduledExecutorService scheduledExecutor;

    public RTDSimpleActionBolt(String topic) {
        this.topic = topic;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        RTDConf conf = checkAndGetRTDConf(stormConf);
        updateAction(conf);
        this.collector = collector;
        ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(false)
                                                          .setNameFormat("RTDQuotaFlushExecutor")
                                                          .setUncaughtExceptionHandler((t, e) -> {
                                                               String threadName = t.getName();
                                                               LOGGER.error("Thread occur exception, name: {}", threadName, e);
                                                              // TODO: 监控
                                                          })
                                                          .build();
        int interval = ((Number) stormConf.getOrDefault(RTD_FLUSH_INTERVAL, 60 * 1000)).intValue();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(factory);
        this.scheduledExecutor.scheduleWithFixedDelay(new FlushTask(), interval, interval, MILLISECONDS);
    }

    @Override
    public void initState(RedisKeyValueState<String, Number> state) {
        this.countState = state;
    }

    @Override
    public void schemaUpdate(int version, String schemaJson) {
        if (version <= this.version) {
            LOGGER.debug("RTD schema version already out of data, current version: {}", this.version);
            return;
        }
        RTDConf conf = JsonUtils.fromJson(schemaJson, RTDConf.class);
        updateAction(conf);
    }

    @Override
    public void executeBySchema(Tuple tuple) {
        String topic = tuple.getStringByField("topic");
        String dataRowJson = tuple.getStringByField("dataRow");
        if (topic == null || !topic.equals(this.topic)) {
            LOGGER.warn("Simple action bolt receive not subscribe topic: {}", topic);
            return;
        }

        DataRow dataRow = JsonUtils.fromJson(dataRowJson, DataRow.class);
        doInvokeCount(dataRow);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(topic, new Fields("topic", "dataRow"));
    }

    private void doInvokeCount(DataRow dataRow) {

    }

    private void updateAction(RTDConf conf) {
        RTDCountActionSchema countActionSchema = conf.getRTDActionSchema(COUNT);
        RTDAggregateActionSchema aggregateActionSchema = conf.getRTDActionSchema(AGGREGATE);
        RTDAviatorActionSchema aviatorActionSchema = conf.getRTDActionSchema(EL);

        synchronized (versionLock) {
            this.version = conf.getVersion();
            if (countActionSchema != null) {
                this.counter = countActionSchema.getTopicAction(topic);
            }
            if (aggregateActionSchema != null) {
                this.aggregator = aggregateActionSchema.getTopicAction(topic);
            }
            if (aviatorActionSchema != null) {
                this.aviator = aviatorActionSchema.getTopicAction(topic);
            }
        }
    }

    private class FlushTask implements Runnable {

        @Override
        public void run() {

        }

    }

}
