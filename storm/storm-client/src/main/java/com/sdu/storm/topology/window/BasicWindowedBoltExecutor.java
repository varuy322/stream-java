package com.sdu.storm.topology.window;

import com.codahale.metrics.Counter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sdu.storm.state.AbstractKeyedStateBackend;
import com.sdu.storm.state.KeyGroupRange;
import com.sdu.storm.state.ListState;
import com.sdu.storm.state.ListStateDescriptor;
import com.sdu.storm.state.rocksdb.OptionsFactory;
import com.sdu.storm.state.rocksdb.RocksDBKeyedStateBackend;
import com.sdu.storm.state.rocksdb.RocksDBListState;
import com.sdu.storm.state.rocksdb.RocksDBStateBackend;
import com.sdu.storm.state.typeutils.base.StringSerializer;
import com.sdu.storm.utils.StormRuntimeException;
import com.sdu.storm.utils.TernaryBoolean;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.sdu.storm.state.rocksdb.PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM;
import static com.sdu.storm.topology.utils.StormUtils.*;
import static com.sdu.storm.topology.window.TimeCharacteristic.EVENT_TIME;

/**
 *
 * @author hanhan.zhang
 * */
public class BasicWindowedBoltExecutor implements IRichBolt, WindowTrigger {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicWindowedBoltExecutor.class);

    private static final String LATE_TUPLE = "Late_Tuple";

    private static final long DEFAULT_MAX_LAG_MS = 0;
    private static final long MIN_WATERMARK_INTERVAL = 10L;
    private static final long DEFAULT_WATERMARK_INTERVAL = 1000L;

    private transient OutputCollector collector;

    private transient WindowAssigner<Tuple> windowAssigner;
    private transient Map<TimeWindow, Trigger<Tuple>> windowToTriggers;


    private transient volatile long currentTupleTs;
    private transient WindowContext windowContext;

    // ------------------------------------window state-------------------------------------
    private transient AbstractKeyedStateBackend<TimeWindow> keyedStateBackend;
    private transient ListStateDescriptor<Tuple> windowStateDescriptor;
    /** The bolt state store namespace */
    private transient String windowStateNamespace;
    /** The state in which the window contents is stored. Each window is a key */
    private transient ListState<Tuple> windowState;

    /**************************************watermark相关*******************************************/
    private transient TimeCharacteristic timeCharacteristic;
    private transient TimestampExtractor timestampExtractor;
    private transient WatermarkGenerator watermarkGenerator;
    private transient long maxLagMs;
    // 所有数据流水位线
    private long currentWatermark = Long.MIN_VALUE;
    private transient Counter lateTupleMetric;
    private transient String lateTupleStream;
    // watermark event trigger
    private transient ScheduledThreadPoolExecutor timerTriggerThreadPool;
    private transient ConcurrentMap<TimeWindow, ScheduledFuture<?>> eventTimeTimerFutures;

    private BasicWindowBolt<Tuple> windowBolt;

    public BasicWindowedBoltExecutor(BasicWindowBolt<Tuple> windowBolt) {
        this.windowBolt = windowBolt;
        this.windowStateDescriptor = windowBolt.getStateDescriptor();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.windowAssigner = windowBolt.getWindowAssigner();

        this.windowContext = new WindowContext();
        this.windowToTriggers = Maps.newConcurrentMap();

        try {
            initWindowState(stormConf, context);
        } catch (Exception e) {
            throw new StormRuntimeException("initialize window state backend failure", e);
        }

        if (WindowAssigner.isEventTime(this.windowAssigner)) {
            this.timeCharacteristic = EVENT_TIME;
        } else {
            this.timeCharacteristic = TimeCharacteristic.NONE;
        }

        // TODO: 配置线程参数
        Thread.UncaughtExceptionHandler handler = (thread, cause) -> {
            // TODO: 报警
            LOGGER.error("Thread {} occur exception", thread.getName(), cause);
        };
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("Time-Trigger-Thread-%d")
                                                          .setUncaughtExceptionHandler(handler)
                                                          .build();
        this.timerTriggerThreadPool = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(4, factory);
        // task cancel should remove from queue
        this.timerTriggerThreadPool.setRemoveOnCancelPolicy(true);
        this.eventTimeTimerFutures = Maps.newConcurrentMap();

        if (this.timestampExtractor != null) {
            lateTupleStream = (String) stormConf.get(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
        }

        validate(stormConf);
        windowBolt.prepare(stormConf, context, collector);

        /***************************watermark相关属性*************************/
        this.timestampExtractor = windowBolt.getTimestampExtractor();
        this.watermarkGenerator = windowBolt.getWatermarkGenerator();
        if (windowBolt.getMaxLagMs() < 0) {
            this.maxLagMs = DEFAULT_MAX_LAG_MS;
        } else {
            this.maxLagMs = windowBolt.getMaxLagMs();
        }

        if (this.timeCharacteristic == EVENT_TIME) {
            if (this.watermarkGenerator == null) {
                LOGGER.info("watermark generator is not set, using default periodic watermark generator with" +
                            " max lag:{}, watermark interval:{}", maxLagMs, DEFAULT_WATERMARK_INTERVAL);
                this.watermarkGenerator = PeriodicWatermarkGenerator.of(maxLagMs, DEFAULT_WATERMARK_INTERVAL);
            }
            this.watermarkGenerator.prepare(stormConf, context);
            this.startWatermarkGenerator();
            // Late stream counter
            this.lateTupleMetric = context.registerCounter("LateTupleNum");
        }

    }

    @Override
    public void execute(Tuple input) {
        if (timeCharacteristic == EVENT_TIME) {
            this.currentTupleTs = timestampExtractor.extractTimestamp(input);
        } else {
            this.currentTupleTs = System.currentTimeMillis();
        }

        // 晚到数据
        if (currentTupleTs < currentWatermark) {
            lateTupleMetric.inc();
            if (lateTupleStream != null) {
                collector.emit(lateTupleStream, Lists.newArrayList(input));
            }
            return;
        }

        // 追踪Tuple时间戳
        if (timeCharacteristic == EVENT_TIME && watermarkGenerator != null) {
            watermarkGenerator.track(input.getSourceGlobalStreamId(), currentTupleTs);
        }

        // 划分窗口
        Collection<TimeWindow> windows = windowAssigner.assignWindows(input, currentTupleTs);
        for (TimeWindow window : windows) {
            // Window State
            keyedStateBackend.setCurrentKey(window);
            try {
                List<Tuple> windowTuples = windowState.get();
                if (windowTuples == null) {
                    windowTuples = Lists.newLinkedList();
                }
                windowTuples.add(input);
                windowState.add(input);
            } catch (Exception e) {
                // TODO:
            }

            // 注册映射关系
            if (!windowToTriggers.containsKey(window)) {
                createTriggerForWindow(windowAssigner, window, windowToTriggers);
            }

            // 是否驱除元素
            Trigger<Tuple> trigger = windowToTriggers.get(window);
            if (trigger == null) {
                throw new RuntimeException("failed to get trigger for window:" + window +
                        " with assigner:" + windowAssigner + ", current ts:" + currentTupleTs);
            }

            // 是否驱除元素只是对Count Window生效, 基于Timestamp的Trigger返回的CONTINUE
            TriggerResult result = trigger.onElement(input, currentTupleTs, window, windowContext);
            if (result == TriggerResult.FIRE) {
                this.trigger(window);
            }
        }
    }

    @Override
    public void cleanup() {
        windowBolt.cleanup();
        timerTriggerThreadPool.shutdown();
        try {
            if (!timerTriggerThreadPool.awaitTermination(2, TimeUnit.SECONDS)) {
                timerTriggerThreadPool.shutdownNow();
            }
        } catch (InterruptedException ie) {
            timerTriggerThreadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        try {
            if (windowState != null) {
                windowState.clear();
            }
            if (keyedStateBackend != null) {
                keyedStateBackend.dispose();
            }
        } catch (Exception e) {
            LOGGER.error("Clean window state failure", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        windowBolt.declareOutputFields(declarer);

        // Late Tuple Stream
        String lateTupleStream = (String) getComponentConfiguration().get(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
        if (lateTupleStream != null) {
            declarer.declareStream(lateTupleStream, new Fields(LATE_TUPLE));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return windowBolt.getComponentConfiguration();
    }

    @Override
    public void trigger(TimeWindow window) {
        keyedStateBackend.setCurrentKey(window);
        try {
            List<Tuple> windowTuples = windowState.get();
            windowBolt.execute(windowTuples, window);
            // 清除窗口数据
            windowState.clear();
            windowToTriggers.remove(window);
            if (timeCharacteristic == EVENT_TIME) {
                windowContext.deleteEventTimeTimer(window);
            }
        } catch (Exception e) {
            // TODO: 异常处理
        }
    }

    @SuppressWarnings("unchecked")
    private void initWindowState(Map stormConf, TopologyContext context) throws Exception {
        this.windowStateNamespace = context.getThisComponentId() + "-" + context.getThisTaskId();

        String stateBackendType = (String) stormConf.getOrDefault(STORM_STATE_BACKEND_TYPE, STORM_STATE_BACKEND_ROCKSDB);
        if (stateBackendType.equals(STORM_STATE_BACKEND_ROCKSDB)) {
            String rocksDBStoreDir = (String) stormConf.get(STORM_STATE_ROCKSDB_STORE_DIR);
            if (rocksDBStoreDir == null || rocksDBStoreDir.isEmpty()) {
                throw new IllegalArgumentException("RocksDB store directory empty");
            }
            // TODO: State checkpoint
            RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(null, TernaryBoolean.fromBoxedBoolean(false));
            rocksDBStateBackend.setDbStoragePath(rocksDBStoreDir);
            rocksDBStateBackend.setPredefinedOptions(SPINNING_DISK_OPTIMIZED_HIGH_MEM);

            String optionCls = (String) stormConf.get(STORM_STATE_ROCKSDB_OPTIONS_CLASS);
            if (optionCls != null && !optionCls.isEmpty()) {
                OptionsFactory optionsFactory = (OptionsFactory) Class.forName(optionCls).newInstance();
                rocksDBStateBackend.setOptionsFactory(optionsFactory);
            }

            this.keyedStateBackend = rocksDBStateBackend.createKeyedStateBackend(
                   stormConf,
                   context.getThisComponentId(),
                   context.getThisTaskId(),
                   TimeWindow.TimeWindowSerializer.INSTANCE,
                   10,
                   new KeyGroupRange(0, 9));

            this.windowState = ((RocksDBKeyedStateBackend) keyedStateBackend).createListState(
                    StringSerializer.INSTANCE,
                    windowStateDescriptor);
            ((RocksDBListState<TimeWindow, String, Tuple>) windowState).setCurrentNamespace(windowStateNamespace);
       } else {
           // TODO:
       }
    }



    private int getMaxSpoutPending(Map stormConf) {
        int maxPending = Integer.MAX_VALUE;
        if (stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING) != null) {
            maxPending = ((Number) stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING)).intValue();
        }
        return maxPending;
    }

    private void ensureCountLessThanMaxPending(long count, long maxPending) {
        if (count > maxPending) {
            throw new IllegalArgumentException("Window count (length + sliding interval) value " + count +
                    " is more than " + Config.TOPOLOGY_MAX_SPOUT_PENDING +
                    " value " + maxPending);
        }
    }

    private void validate(Map stormConf) {
        long size = windowBolt.getSize();
        long slide = windowBolt.getSlide();
        long stateSize = windowBolt.getStateSize();

        if (timeCharacteristic == EVENT_TIME) {
            //todo: size must be dividable too
            if (stateSize > 0 && stateSize % size != 0) {
                throw new IllegalArgumentException("state window size and window size must be dividable!");
            }
        } else {
            // 对于滑动窗口, 待确认的Tuple数据长度等于窗口长度
            int maxSpoutPending = getMaxSpoutPending(stormConf);
            ensureCountLessThanMaxPending(size, maxSpoutPending);
            if (slide != BasicWindowBolt.DEFAULT_SLIDE) {
                ensureCountLessThanMaxPending(slide, maxSpoutPending);
            }
        }
    }

    private void startWatermarkGenerator() {
        long watermarkInterval = watermarkGenerator.getWatermarkInterval();
        if (watermarkInterval < MIN_WATERMARK_INTERVAL) {
            throw new IllegalArgumentException("watermark interval must be greater than 10ms!");
        }

        this.timerTriggerThreadPool.scheduleAtFixedRate(() -> {
            long newWatermark = watermarkGenerator.getCurrentWatermark();
            if (newWatermark > currentWatermark) {
                LOGGER.debug("Generating new watermark: {}", newWatermark);
                currentWatermark = newWatermark;
                //
                checkEventTimeWindows();
            }
        }, watermarkInterval, watermarkInterval, TimeUnit.MILLISECONDS);
    }

    private void checkEventTimeWindows() {
        for (Iterator<TimeWindow> iterator = windowToTriggers.keySet().iterator();
                iterator.hasNext();) {
            TimeWindow window = iterator.next();
            if (window.getEnd() < currentWatermark) {
                // 当前窗口已位于水平线之下, 清除窗口
                windowContext.deleteEventTimeTimer(window);
                windowContext.registerEventTimeTimer(0, window);
                // TODO: 当前任务是否清除
            } else {
                // 注册窗口清除任务
                windowContext.registerEventTimeTimer(window.getEnd() + maxLagMs, window);
            }
        }
    }

    private void createTriggerForWindow(WindowAssigner<Tuple> windowAssigner, TimeWindow window, Map<TimeWindow, Trigger<Tuple>> windowToTriggers) {
        if (windowAssigner instanceof TumblingCountWindows || windowAssigner instanceof SlidingCountWindows) {
            windowToTriggers.put(window, CountTrigger.of(window.getEnd() - window.getStart()));
        } else if (windowAssigner instanceof TumblingEventTimeWindow || windowAssigner instanceof SlidingEventTimeWindows) {
            windowToTriggers.put(window, EventTimeTrigger.create());
        }
    }

    private class WindowContext implements Trigger.TriggerContext {

        private ScheduledFuture<?> registerFuture(long expectedEnd, TimeWindow window, WindowTrigger target) {
            long now = (timeCharacteristic == EVENT_TIME && currentTupleTs > 0) ? currentTupleTs
                                                                                : System.currentTimeMillis();
            long delay;
            if (expectedEnd <= 0) {
                delay = 0;
            } else {
                // add 5ms delay to avoid the boundary problem
                delay = expectedEnd - now + 5L;
                // expired windows that are restored by previous successful checkpoint
                // should fire immediately
                if (delay < 0) {
                    delay = 0;
                }
            }
            LOGGER.info("registering future, tuple ts: {}, delay: {}, window: {}", currentTupleTs, delay, window);
            return timerTriggerThreadPool.schedule(()-> target.trigger(window), delay, TimeUnit.MILLISECONDS);
        }

        @Override
        public long getMaxLagMs() {
            return BasicWindowedBoltExecutor.this.maxLagMs;
        }

        @Override
        public ScheduledFuture<?> deleteEventTimeTimer(TimeWindow window) {
            ScheduledFuture<?> future = eventTimeTimerFutures.remove(window);
            if (future != null) {
                future.cancel(true);
            }
            return future;
        }

        @Override
        public void registerEventTimeTimer(long time, TimeWindow window) {
            if (!eventTimeTimerFutures.containsKey(window)) {
                eventTimeTimerFutures.put(window, registerFuture(time, window, BasicWindowedBoltExecutor.this));
            }
        }
    }
}
