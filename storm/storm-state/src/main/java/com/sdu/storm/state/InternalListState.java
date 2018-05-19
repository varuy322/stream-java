package com.sdu.storm.state;

import java.util.List;

public interface InternalListState<K, N, T> extends InternalMergingState<K, N, T, List<T>, List<T>>, ListState<T> {
}
