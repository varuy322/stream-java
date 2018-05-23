package com.sdu.stream.state;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.stream.state.rocksdb.OptionsFactory;
import com.sdu.stream.state.rocksdb.PredefinedOptions;
import com.sdu.stream.state.rocksdb.RocksDBStateUtils;
import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.seralizer.base.IntSerializer;
import com.sdu.stream.state.seralizer.base.StringSerializer;
import org.junit.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RocksDBKeyStateBackendTest extends KeyStateBackendTest<RocksDBKeyStateBackendTest.TimeWindow> {

    @Override
    public KeyedStateBackend<TimeWindow> createKeyStateBackend() throws IOException {
        File rocksDBBaseDir = new File("/Users/hanhan.zhang/tmp/rocks");
        PredefinedOptions options = PredefinedOptions.SPINNING_DISK_OPTIMIZED;
        OptionsFactory factory = new RocksDBOptionsFactory();
        return RocksDBStateUtils.createRocksDBKeyStateBackend(
                rocksDBBaseDir,
                options,
                factory,
                TimeWindowSerializer.INSTANCE);
    }

    @Test
    public void testRocksDBListState() throws Exception {
        KeyedStateBackend<TimeWindow> stateBackend = createKeyStateBackend();

        ListStateDescriptor<Integer> stateDesc = new ListStateDescriptor<>(
                "ListState",
                IntSerializer.INSTANCE,
                Lists.newLinkedList());
        InternalListState<String, TimeWindow, Integer> listState = stateBackend.createListState(
                StringSerializer.INSTANCE,
                stateDesc);


        // 添加数据
        String namespace1 = "nsp1";
        TimeWindow window1 = new TimeWindow(0, 3);
        listState.add(namespace1, window1, 1);
        listState.add(namespace1, window1, 2);
        listState.add(namespace1, window1, 3);

        String namespace2 = "nsp2";
        TimeWindow window2 = new TimeWindow(5, 8);
        listState.add(namespace2, window2, 5);
        listState.add(namespace2, window2, 6);
        listState.add(namespace2, window2, 7);

        listState.add(namespace1, window1, 10);

        // 读取数据
        List<Integer> windowElements1 = listState.get(namespace1, window1);
        for (int ele : windowElements1) {
            System.out.println("Namespace: " + namespace1 + ", UserKey: " + window1 + ", Element: " + ele);
        }

        System.out.println("=================================================================");

        // 更新数据
        listState.update(namespace1, window1, Lists.newArrayList(11, 12, 13));

        windowElements1 = listState.get(namespace1, window1);
        for (int ele : windowElements1) {
            System.out.println("Namespace: " + namespace1 + ", UserKey: " + window1 + ", Element: " + ele);
        }

        System.out.println("=================================================================");

        List<Integer> windowElement2 = listState.get(namespace2, window2);
        for (int ele : windowElement2) {
            System.out.println("Namespace: " + namespace2 + ", UserKey: " + window2 + ", Element: " + ele);
        }
    }

    @Test
    public void testRocksDBMapState() throws Exception {
        KeyedStateBackend<TimeWindow> stateBackend = createKeyStateBackend();

        InternalMapState<String, TimeWindow, String, Integer> mapState = stateBackend.createMapState(
                StringSerializer.INSTANCE,
                new MapStateDescriptor<>(
                        "MapState",
                        StringSerializer.INSTANCE,
                        IntSerializer.INSTANCE,
                        Maps.newHashMap()));

        // 存数据
        String namespace1 = "nsp1";
        TimeWindow window1 = new TimeWindow(0, 3);
        mapState.put(namespace1, window1, "A", 1);
        mapState.put(namespace1, window1, "B", 2);

        String namespace2 = "nsp2";
        TimeWindow window2 = new TimeWindow(5, 7);
        mapState.put(namespace2, window2, "C", 3);
        mapState.put(namespace2, window2, "E", 5);

        // 读取数据
        Iterator<String> it = mapState.keys(namespace1, window1).iterator();
        while (it.hasNext()) {
            String field = it.next();
            int value = mapState.get(namespace1, window1, field);
            System.out.println("Namespace: " + namespace1 + ", UserKey: " + window1 + ", Field: " + field + ", Value: " + value);
        }
        System.out.println("===============================================================================");

        it = mapState.keys(namespace2, window2).iterator();
        while (it.hasNext()) {
            String field = it.next();
            int value = mapState.get(namespace2, window2, field);
            System.out.println("Namespace: " + namespace2 + ", UserKey: " + window2 + ", Field: " + field + ", Value: " + value);
        }
    }


    @Test
    public void testRocksDBValueState() throws Exception {
        KeyedStateBackend<TimeWindow> stateBackend = createKeyStateBackend();

        InternalValueState<String, TimeWindow, String> valueState = stateBackend.createValueState(
                StringSerializer.INSTANCE,
                new ValueStateDescriptor<>(
                        "ValueState",
                        StringSerializer.INSTANCE,
                        "Hello RocksDB"));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // 更新数据
        String namespace1 = "nsp1";
        TimeWindow window1 = new TimeWindow(0, 3);
        String value = String.format("Now date: %s", sdf.format(new Date()));
        valueState.update(namespace1, window1, value);
        // 读取数据
        String rocksValue = valueState.value(namespace1, window1);
        if (rocksValue.equals(value)) {
            System.out.println("Namespace: " + namespace1 + ", UserKey: " + window1 + ", Value: " + value);
            System.out.println("===============================================================================");
        }

        TimeUnit.SECONDS.sleep(1);

        String namespace2 = "nsp2";
        TimeWindow window2 = new TimeWindow(5, 7);
        value = String.format("Now date: %s", sdf.format(new Date()));
        valueState.update(namespace2, window2, value);
        // 读取数据
        rocksValue = valueState.value(namespace2, window2);
        if (rocksValue.equals(value)) {
            System.out.println("Namespace: " + namespace2 + ", UserKey: " + window2 + ", Value: " + value);
            System.out.println("===============================================================================");
        }

        TimeUnit.SECONDS.sleep(1);
        // 更新数据
        value = String.format("Now date: %s", sdf.format(new Date()));
        valueState.update(namespace1, window1, value);
        rocksValue = valueState.value(namespace1, window1);
        if (rocksValue.equals(value)) {
            System.out.println("Namespace: " + namespace1 + ", UserKey: " + window1 + ", Value: " + value);
            System.out.println("===============================================================================");
        }
    }

    private class RocksDBOptionsFactory implements OptionsFactory {
        @Override
        public DBOptions createDBOptions(DBOptions currentOptions) {
            return currentOptions;
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions) {
            return currentOptions;
        }
    }

    public static class TimeWindowSerializer extends TypeSerializer<TimeWindow> {

        public static TimeWindowSerializer INSTANCE = new TimeWindowSerializer();

        private TimeWindowSerializer() {}

        @Override
        public void serializer(TimeWindow element, DataOutput output) throws IOException {
            output.writeLong(element.start);
            output.writeLong(element.end);
        }

        @Override
        public TimeWindow deserialize(DataInput input) throws IOException {
            return new TimeWindow(input.readLong(), input.readLong());
        }
    }

    public static class TimeWindow {

        private long start;
        private long end;

        public TimeWindow(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return "TimeWindow[start=" + start + ", end=" + end + ']';
        }
    }

}
