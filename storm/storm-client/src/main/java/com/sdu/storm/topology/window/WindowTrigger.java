package com.sdu.storm.topology.window;

public interface WindowTrigger {

    void trigger(TimeWindow window);

}
