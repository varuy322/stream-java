package com.sdu.storm.topology.window;

/**
 * @author hanhan.zhang
 * */
public enum TriggerResult {

    // 窗口不做任何操作
    CONTINUE(false, false),

    // 计算窗口数据且清除窗口
    FIRE_AND_PURGE(true, true),

    // 计算窗口数据
    FIRE(true, false),

    // 清除窗口
    PURGE(false, true);

    // 是否计算窗口元素结果
    private final boolean fire;
    // 是否清除窗口
    private final boolean purge;

    TriggerResult(boolean fire, boolean purge) {
        this.fire = fire;
        this.purge = purge;
    }
}
