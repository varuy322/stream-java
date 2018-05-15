package com.sdu.storm.state;

/**
 * @param <IN> Type of the value that can be added to the state
 * @param <OUT> Type of the value that can be retrieved from the state
 *
 * @author hanhan.zhang
 * */
public interface AppendState<IN, OUT> extends State {

    OUT get() throws Exception;

    void add(IN value) throws Exception;

}
