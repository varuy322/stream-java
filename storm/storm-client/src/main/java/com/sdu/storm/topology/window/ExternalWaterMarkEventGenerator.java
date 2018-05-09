package com.sdu.storm.topology.window;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sdu.storm.topology.WatermarkCalculateException;
import org.apache.storm.generated.GlobalStreamId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * {@link ExternalWaterMarkEventGenerator}周期性计算当前水位线
 *
 * @author hanhan.zhang
 * */
public class ExternalWaterMarkEventGenerator<T> implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalWaterMarkEventGenerator.class);

    //
    private ExternalWindowManager<T> windowManager;
    // 记录当前窗口所有输入数据流
    private Set<GlobalStreamId> inputStreams;
    // 记录当前窗口所有输入数据流最大时间戳
    private Map<GlobalStreamId, Long> streamToTs;
    // WaterMarkEvent事件线程
    private final ScheduledExecutorService executorService;
    private ScheduledFuture<?> executorFuture;

    // Tuple最大延迟时间
    private final int eventTsLag;
    // 水位线计算时间间隔
    private final int interval;

    // 上次计算的水位线
    //  计算方式:
    //     step1: 计算窗口内数据输入流的最大水位线
    //     step2: 选取所有数据输入流水位线的最小值
    private volatile long lastWatermarkTs;

    /**
     * @param intervalMs: 水位线计算时间间隔
     * @param eventTsLagMs: Tuple最大延迟时间
     * */
    public ExternalWaterMarkEventGenerator(ExternalWindowManager<T> windowManager,
                                           int intervalMs,
                                           int eventTsLagMs,
                                           Set<GlobalStreamId> inputStreams) {
        this.streamToTs = Maps.newConcurrentMap();
        this.windowManager = windowManager;
        this.inputStreams = inputStreams;
        this.eventTsLag = eventTsLagMs;
        this.interval = intervalMs;


        // WaterMarkEvent事件线程
        Thread.UncaughtExceptionHandler handler = (thread, cause) -> {
            // TODO: 监控
            LOGGER.error("Occur exception, threadName: {}", thread.getName(), cause);
        };
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("watermark-event-generator-%d")
                                                          .setDaemon(true)
                                                          .setUncaughtExceptionHandler(handler)
                                                          .build();
        this.executorService = Executors.newSingleThreadScheduledExecutor(factory);
    }

    /**
     * 跟踪窗口内输入数据流当前最大的时间戳
     *
     * @return true: 当前Tuple高于水位线
     *         false: 当前Tuple低于水位线
     * */
    public boolean track(GlobalStreamId streamId, long ts) {
        Long maxTs = streamToTs.get(streamId);
        if (maxTs == null || maxTs < ts) {
            // 更新当前窗口内输入数据流最大的时间戳
            streamToTs.put(streamId, ts);
        }

        // 检测上次水位线计算是否成功
        checkFailures();
        return ts > lastWatermarkTs;
    }

    private void checkFailures() {
        if (executorFuture != null && executorFuture.isDone()) {
            try {
                executorFuture.get();
            } catch (InterruptedException ex) {
                LOGGER.error("Watermark calculate failure ", ex);
                throw new WatermarkCalculateException(ex);
            } catch (ExecutionException ex) {
                LOGGER.error("Watermark calculate failure ", ex);
                throw new WatermarkCalculateException(ex.getCause());
            }
        }
    }

    /**计算水位线*/
    private long computeWaterMarkTs() {
        long ts = 0;
        if (streamToTs.size() >= inputStreams.size()) {
            ts = Long.MAX_VALUE;
            for (Map.Entry<GlobalStreamId, Long> entry : streamToTs.entrySet()) {
                ts = Math.min(ts, entry.getValue());
            }
        }
        return ts - eventTsLag;
    }

    @Override
    public void run() {
        try {
            long waterMarkTs = computeWaterMarkTs();
            if (waterMarkTs > lastWatermarkTs) {
                // 投递WatermarkEvent
                windowManager.add(new WatermarkEvent<>(waterMarkTs));
                lastWatermarkTs = waterMarkTs;
            }
        } catch (Throwable e) {
            LOGGER.error("Failed while processing watermark event ", e);
            throw e;
        }
    }

    public void start() {
        executorFuture = executorService.scheduleAtFixedRate(this, interval, interval, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        LOGGER.debug("Shutting down ExternalWaterMarkEventGenerator");
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
