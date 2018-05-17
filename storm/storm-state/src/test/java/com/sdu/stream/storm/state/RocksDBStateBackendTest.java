package com.sdu.stream.storm.state;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.storm.configuration.ConfigConstants;
import com.sdu.storm.state.*;
import com.sdu.storm.state.rocksdb.OptionsFactory;
import com.sdu.storm.state.rocksdb.RocksDBKeyedStateBackend;
import com.sdu.storm.state.rocksdb.RocksDBStateBackend;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.state.typeutils.base.IntSerializer;
import com.sdu.storm.state.typeutils.base.ListSerializer;
import com.sdu.storm.state.typeutils.base.StringSerializer;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;
import org.junit.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static com.sdu.storm.state.rocksdb.PredefinedOptions.SPINNING_DISK_OPTIMIZED;

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
        //        OptionsFactory是的对预定义DBOptions及ColumnFamilyOptions选项重配置及扩充
        stateBackend.setDbStoragePath("file:/Users/hanhan.zhang/tmp/rocksdb-store");
        stateBackend.setPredefinedOptions(SPINNING_DISK_OPTIMIZED);
        stateBackend.setOptionsFactory(new RocksDBOptionsFactory());

        return stateBackend;
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testRockDBListState() throws Exception {
        RocksDBStateBackend stateBackend = getStateBackend();

        // 需设置RockDB初始化目录
        Map stormConf = Maps.newHashMap();
        stormConf.put(ConfigConstants.TOPOLOGY_STATE_ROCKSDB_LIB_DIR, "/Users/hanhan.zhang/tmp/rocksdb-lib");

        // RocksDBKeyedStateBackend实例化时初始化RocksDB
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

        // State与RocksDB ColumnHandler对应, 即:
        //  1: 同一State的数据都存储同一个ColumnHandler句柄中(两者映射关系通过State Name建立, 故State Name需唯一)
        //  2:
        ListSerializer<Integer> typeSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>(
                "TestListState",
                typeSerializer,
                Collections.emptyList());

        // State Namespace
        StringSerializer namespaceSerializer = StringSerializer.INSTANCE;

        InternalListState<TimeWindow, String, Integer> timeWindowListState =
                windowKeyStateBackend.createListState(namespaceSerializer, stateDescriptor);

        // 存储数据(同namespace下存储多个KEY)
        timeWindowListState.setCurrentNamespace("namespace1");

        TimeWindow testWindowKey1 = new TimeWindow(1, 10);
        windowKeyStateBackend.setCurrentKey(testWindowKey1);
        timeWindowListState.addAll(Lists.newArrayList(1, 2));

        TimeWindow testWindowKey2 = new TimeWindow(5, 15);
        windowKeyStateBackend.setCurrentKey(testWindowKey2);
        timeWindowListState.addAll(Lists.newArrayList(3, 5));

        // 读取数据
        timeWindowListState.setCurrentNamespace("namespace1");
        windowKeyStateBackend.setCurrentKey(testWindowKey1);
        Iterator<Integer> windowElement1 = timeWindowListState.get().iterator();
        while (windowElement1.hasNext()) {
            System.out.println("Namespace: namespace1, KEY: " + testWindowKey1 + ", Element: " + windowElement1.next());
        }

        windowKeyStateBackend.setCurrentKey(testWindowKey2);
        timeWindowListState.add(6);
        Iterator<Integer> windowElement2 = timeWindowListState.get().iterator();
        while (windowElement2.hasNext()) {
            System.out.println("Namespace: namespace1, KEY: " + testWindowKey2 + ", Element: " + windowElement2.next());
        }

//        // 按照namespace读取数据
//        Stream<TimeWindow> stream = windowKeyStateBackend.getKeys("TestListState", "namespace1");
//        System.out.println("Namespace[namespace1] has key: ");
//        stream.forEach(System.out::println);
    }

    private static final class RocksDBOptionsFactory implements OptionsFactory {

        @Override
        public DBOptions createDBOptions(DBOptions currentOptions) {
            return currentOptions;
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
            return currentOptions;
        }

    }

    private static final class TimeWindow {

        private final long start;
        private final long end;

        TimeWindow(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return "TimeWindow[" +
                    "start=" + start +
                    ", end=" + end +
                    ']';
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
