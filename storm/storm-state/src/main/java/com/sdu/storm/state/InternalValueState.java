package com.sdu.storm.state;

public interface InternalValueState<K, N, T> extends InternalKvState<K, N, T>, ValueState<T> {
}
