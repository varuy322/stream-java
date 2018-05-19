package com.sdu.storm.topology.window;

import org.apache.storm.tuple.Tuple;

public interface KeySelector<IN extends Tuple, KEY> {

    KEY getKey(IN tuple);

}
