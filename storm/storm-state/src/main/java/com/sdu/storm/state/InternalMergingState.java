package com.sdu.storm.state;

import java.util.Collection;

public interface InternalMergingState<K, N, IN, SV, OUT> extends InternalAppendingState<K, N, IN, SV, OUT>, MergeState<IN, OUT> {

    void mergeNamespaces(N target, Collection<N> sources) throws Exception;

}
