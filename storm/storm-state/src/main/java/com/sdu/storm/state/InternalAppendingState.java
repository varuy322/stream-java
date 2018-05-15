package com.sdu.storm.state;

/**
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> The type of elements added to the state
 * @param <SV> The type of elements in the state
 * @param <OUT> The type of the resulting element in the state
 *
 * @author hanhan.zhang
 * */
public interface InternalAppendingState<K, N, IN, SV, OUT> extends InternalKvState<K, N, SV>, AppendState<IN, OUT>{
}
