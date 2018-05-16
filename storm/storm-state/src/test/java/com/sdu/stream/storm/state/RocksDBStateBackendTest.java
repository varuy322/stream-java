package com.sdu.stream.storm.state;

import com.google.common.collect.Maps;
import com.sdu.storm.configuration.ConfigConstants;
import com.sdu.storm.state.*;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.state.typeutils.base.IntSerializer;
import com.sdu.storm.state.typeutils.base.ListSerializer;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;
import org.junit.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static com.sdu.storm.state.PredefinedOptions.SPINNING_DISK_OPTIMIZED;

public class RocksDBStateBackendTest extends StateBackendTestBase<RocksDBStateBackend> {

    @Override
    public RocksDBStateBackend getStateBackend() throws IOException {
        // TODO: StateBackend尚未实现SNAPSHOT
        String checkpointDataUri = "hdfs://127.0.0.1:54310/user/hadoop/";

        RocksDBStateBackend stateBackend = new RocksDBStateBackend(checkpointDataUri, true);

        // 初始化操作:
        //  1: rocks db数据存储目录
        //  2: rocks DBOptions及ColumnFamilyOptions配置
        //     NOTE:
        //        PredefinedOptions预定义DBOptions及ColumnFamilyOptions配置
        //        如果配置OptionsFactory, 则优先使用OptionsFactory
        stateBackend.setDbStoragePath("file://Users/hanhan.zhang/tmp");
        stateBackend.setPredefinedOptions(SPINNING_DISK_OPTIMIZED);
        stateBackend.setOptionsFactory(new RocksDBOptionsFactory());

        return stateBackend;
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testRockDBListState() throws IOException {
        RocksDBStateBackend stateBackend = getStateBackend();

        // 需设置RockDB初始化目录
        Map stormConf = Maps.newHashMap();
        stormConf.put(ConfigConstants.TOPOLOGY_STATE_DIR, "file://Users/hanhan.zhang/tmp");

        RocksDBKeyedStateBackend<TimeWindow> windowKeyStateBackend = (RocksDBKeyedStateBackend<TimeWindow>) stateBackend.createKeyedStateBackend(
                stormConf,
                "TestBolt",
                0,
                new TimeWindowTypeSerializer(),
                10,
                new KeyGroupRange(0, 9));

        // StateDescriptor用于构建State
        // Note:
        //  1: StateName必须在RocksDBKeyedStateBackend作用域内唯一
        //  2: TypeSerializer必须是ListTypeSerializer
        ListSerializer<Integer> typeSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>(
                "TestListState",
                typeSerializer,
                Collections.emptyList());


//        windowKeyStateBackend.createListState()

    }

    private static final class RocksDBOptionsFactory implements OptionsFactory {

        @Override
        public DBOptions createDBOptions(DBOptions currentOptions) {
            return SPINNING_DISK_OPTIMIZED.createDBOptions();
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
            return SPINNING_DISK_OPTIMIZED.createColumnOptions();
        }

    }

    private static final class TimeWindow {

        private final long start;
        private final long end;

        TimeWindow(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }

    private static final class TimeWindowTypeSerializer extends TypeSerializer<TimeWindow> {

        @Override
        public TimeWindow createInstance() {
            return new TimeWindow(0, -1);
        }

        @Override
        public int getLength() {
            return 16;
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public void serialize(TimeWindow record, DataOutputView target) throws IOException {
            target.writeLong(record.start);
            target.writeLong(record.end);
        }

        @Override
        public TimeWindow deserialize(DataInputView source) throws IOException {
            return new TimeWindow(source.readLong(), source.readLong());
        }

        @Override
        public TypeSerializer<TimeWindow> duplicate() {
            return new TimeWindowTypeSerializer();
        }

        @Override
        public TimeWindow copy(TimeWindow from) {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }

        @Override
        public boolean canEqual(Object obj) {
            return false;
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }
}
