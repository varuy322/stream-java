package com.sdu.storm.state.redis;

import com.sdu.storm.state.InternalKvState;
import com.sdu.storm.state.State;

public abstract class AbstractRedisDBState<K, N, V, S extends State> implements InternalKvState<K, N, V>, State {
}
