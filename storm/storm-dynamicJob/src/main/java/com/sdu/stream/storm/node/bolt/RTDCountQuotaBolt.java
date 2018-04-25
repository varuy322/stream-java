package com.sdu.stream.storm.node.bolt;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.stream.storm.parse.DataRow;
import com.sdu.stream.storm.schema.RTDDomainSchema;
import com.sdu.stream.storm.schema.action.Action;
import com.sdu.stream.storm.schema.action.ActionType;
import com.sdu.stream.storm.schema.action.Sum;
import com.sdu.stream.storm.storage.IStorageBackend;
import com.sdu.stream.storm.utils.ColumnNotFoundException;
import com.sdu.stream.storm.utils.JsonUtils;
import com.sdu.stream.storm.utils.Quota;
import com.sdu.stream.storm.utils.RTDSchemaException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.sdu.stream.storm.utils.JsonUtils.toJson;
import static com.sdu.stream.storm.utils.Quota.StorageType.HINCRBY;
import static com.sdu.stream.storm.utils.StormConf.SUM_OPERATION;

/**
 * Sum、WindowCount统计指标 =====>> 构建Domain
 *
 * DomainKey: {@link RTDDomainSchema#domainKey} + 指标聚合字段
 *
 * {@link RTDCountQuotaBolt}输出格式:
 *
 * +-------+-------------+------------+---------+---------------+-----------+-------+
 * | topic | domain name | domain key | version | version count | timestamp | quota |
 * +-------+-------------+------------+---------+---------------+-----------+-------+
 *
 * @author hanhan.zhang
 * */
public class RTDCountQuotaBolt extends RTDStorageBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(RTDCountQuotaBolt.class);

    private String topic;
    private String storageNamespace = "$sum_";

    private IStorageBackend storageBackend;
    private OutputCollector collector;

    private volatile int version;
    private volatile String domain;
    private volatile Map<String, List<String>> quotas;

    public RTDCountQuotaBolt(String topic) {
        this.topic = topic;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        this.collector = collector;

        RTDDomainSchema domainSchema = checkAndGetRTDConf(stormConf);
        this.version = domainSchema.getVersion();
        this.quotas = doUpdateOperation(domainSchema);
    }

    @Override
    public String buildStorageBackendNamespace() {
        return storageNamespace;
    }

    @Override
    public void initStorageBackend(IStorageBackend storageBackend) {
        this.storageBackend = storageBackend;
    }

    @Override
    public void schemaUpdate(int version, String schemaJson) {
        if (version <= this.version) {
            LOGGER.debug("RTD schema version already out of data, current version: {}", this.version);
            return;
        }

        RTDDomainSchema domainSchema = JsonUtils.fromJson(schemaJson, RTDDomainSchema.class);
        this.version = version;
        this.quotas = doUpdateOperation(domainSchema);
    }

    @Override
    public void executeBySchema(Tuple tuple) {
        String streamTopic = tuple.getStringByField(RTD_ACTION_TOPIC);
        if (streamTopic.equals(this.topic)) {
            String dataJson = tuple.getStringByField(RTD_ACTION_RESULT);
            DataRow dataRow = JsonUtils.fromJson(dataJson, DataRow.class);

            Quota quota = new Quota(this.topic);
            Map<Quota.StorageType, String> storageKeys = Maps.newHashMap();
            for (Map.Entry<String, List<String>> entry : quotas.entrySet()) {
                String quotaName = entry.getKey();
                List<String> columns = entry.getValue();
                StringBuilder sb = new StringBuilder();
                boolean first = true;
                for (String column : columns) {
                    try {
                        if (!first) {
                            sb.append("_");
                        } else {
                            first = false;
                        }
                        Object value = dataRow.getColumnValue(column);
                        sb.append(value);
                    } catch (ColumnNotFoundException e) {
                        // TODO: 报警
                        LOGGER.error("RTD column execute sum action failure, column: {}", column);
                    }
                }
                String storageKey = storageNamespace + quotaName;
                storageKeys.put(HINCRBY, storageKey);
            }

            quota.setQuotaStorageKeys(storageKeys);
            collector.emit(this.topic, tuple, Lists.newArrayList(toJson(quota)));
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(this.topic, new Fields(RTD_ACTION_TOPIC, RTD_ACTION_TYPE, RTD_ACTION_RESULT));
    }

    private Map<String, List<String>> doUpdateOperation(RTDDomainSchema domainSchema) {
        List<Action> actions = domainSchema.getOperation(SUM_OPERATION);
        if (actions == null || actions.isEmpty()) {
            throw new IllegalArgumentException("RTD sum operation config empty !!!");
        }
        Map<String, List<String>> quotas = Maps.newHashMap();
        for (Action action : actions) {
            if (action.actionType() != ActionType.Sum) {
                throw new RTDSchemaException("Action type should be Sum, but type: " + action.actionType());
            }
            Sum sum = (Sum) action;
            if (sum.getTopic().equals(this.topic)) {
                quotas.put(sum.getTopic(), sum.getFields());
            }
        }

        return quotas;
    }
}
