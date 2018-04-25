package com.sdu.stream.storm.schema.action;

import java.util.List;

/**
 *
 * @author hanhan.zhang
 * */
public class Aggregator implements Action {

    private String aggregateKey;
    private List<AggregatorDesc> aggregateDesc;

    public Aggregator(String aggregateKey, List<AggregatorDesc> aggregateDesc) {
        this.aggregateKey = aggregateKey;
        this.aggregateDesc = aggregateDesc;
    }

    public String getAggregateKey() {
        return aggregateKey;
    }

    public List<AggregatorDesc> getAggregateDesc() {
        return aggregateDesc;
    }

    @Override
    public ActionType actionType() {
        return ActionType.Aggregate;
    }

    public static class AggregatorDesc {
        private String topic;
        private String field;
        private int threshold;
        private String alias;

        public AggregatorDesc(String topic, String field, int threshold, String alias) {
            this.topic = topic;
            this.field = field;
            this.threshold = threshold;
            this.alias = alias;
        }

        public String getTopic() {
            return topic;
        }

        public String getField() {
            return field;
        }

        public int getThreshold() {
            return threshold;
        }

        public String getAlias() {
            return alias;
        }
    }

    public static Aggregator of(String topic, List<AggregatorDesc> aggregateDesc) {
        return new Aggregator(topic, aggregateDesc);
    }

}
