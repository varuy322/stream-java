package com.sdu.storm.state;

import java.util.List;

public interface ListState<T> extends MergeState<T, List<T>> {

    void update(List<T> values) throws Exception;

    void addAll(List<T> values) throws Exception;
}
