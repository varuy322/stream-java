package com.sdu.stream.flink.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.sdu.stream.flink.storage.StorageConf.ROCKSDB_DELETE_ON_EXIT;
import static com.sdu.stream.flink.storage.StorageConf.ROCKSDB_ROOT_DIR;

/**
 * 基于RockDB构建以每小时为单位的Key/Value存储窗口
 *
 * @author hanhan.zhang
 * */
public class RocksDBStorageBackend extends AbstractRocksStorageBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStorageBackend.class);

    private RocksDB rocksDB;
    private Map<Integer, ColumnFamilyHandle> hourWindowHandlers = Maps.newConcurrentMap();
    private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(task ->
            new Thread("RocksDB-Window-Scheduler-Cleaner")
    );

    @Override
    public void initRocksDB(StorageConf conf) throws Exception {
        LOGGER.info("Start initializer RocksDB, root dir: {}", rootDir);
        // 初始化RocksDB
        int now = nowHour();
        DBOptions dbOptions = null;
        try {
            dbOptions = new DBOptions().setCreateMissingColumnFamilies(true).setCreateIfMissing(true);

            List<ColumnFamilyDescriptor> columnFamilyNames = Lists.newArrayList();
            // RocksDB默认存储(必须), 文件句柄
            columnFamilyNames.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
            columnFamilyNames.add(new ColumnFamilyDescriptor(String.valueOf(now).getBytes()));

            List<Integer> hours = Lists.newArrayList(24, now);

            List<ColumnFamilyHandle> columnFamilyHandles = Lists.newArrayList();
            rocksDB = RocksDB.open(dbOptions, rootDir, columnFamilyNames, columnFamilyHandles);

            for (int i = 0; i < hours.size(); ++i) {
                hourWindowHandlers.put(hours.get(i), columnFamilyHandles.get(i));
            }
        } finally {
            if (dbOptions != null) {
                dbOptions.close();
            }
        }

        // 定时清除, 只保留两个小时缓存数据
        scheduler.scheduleAtFixedRate(() -> {
            int hour = nowHour();
            for (int i = 0; i < hour - 1; ++i) {
                ColumnFamilyHandle handle = hourWindowHandlers.get(i);
                if (handle != null) {
                    try {
                        rocksDB.dropColumnFamily(handle);
                    } catch (RocksDBException e) {
                        LOGGER.info("Delete RocksDB column family handle of {} failure", hour, e);
                    }
                }
            }
        }, 0, 1, TimeUnit.HOURS);
    }

    @Override
    public void put(String key, String value, int timeoutSecond) throws Exception{
        throw new UnsupportedOperationException("RocksDBStorageBackend unsupported set key expire time");
    }

    @Override
    public void put(String key, String value) throws Exception {
        int hour = nowHour();
        ColumnFamilyHandle handle = hourWindowHandlers.get(hour);
        // thread safe
        if (handle == null) {
            synchronized (this) {
                // double check
                handle = hourWindowHandlers.get(hour);
                if (handle == null) {
                    handle = createNowColumnFamilyHandle(hour);
                    hourWindowHandlers.put(hour, handle);
                }
            }
        }
        rocksDB.put(handle, key.getBytes(), value.getBytes());
    }

    @Override
    public String get(String key) throws Exception {
        for (Map.Entry<Integer, ColumnFamilyHandle> entry : hourWindowHandlers.entrySet()) {
            ColumnFamilyHandle handle = entry.getValue();
            if (handle == null) {
                continue;
            }
            byte[] values = rocksDB.get(handle, key.getBytes());
            if (values != null) {
                return new String(values);
            }
        }
        return null;
    }

    @Override
    public void remove(String key) throws Exception {
        for (Map.Entry<Integer, ColumnFamilyHandle> entry : hourWindowHandlers.entrySet()) {
            ColumnFamilyHandle handle = entry.getValue();
            if (handle == null) {
                continue;
            }
            rocksDB.remove(handle, key.getBytes());
        }
    }

    @Override
    public void dispose() throws Exception {
        LOGGER.info("Start close RocksDB, root dir: {}", this.rootDir);

        for (Map.Entry<Integer, ColumnFamilyHandle> entry : hourWindowHandlers.entrySet()) {
            ColumnFamilyHandle handle = entry.getValue();
            if (handle != null) {
                handle.close();
            }
        }

        if (rocksDB != null) {
            rocksDB.close();
        }

        LOGGER.info("Successfully close RocksDB, root dir: {}", this.rootDir);
    }

    private synchronized ColumnFamilyHandle createNowColumnFamilyHandle(int hour) throws RocksDBException {
        ColumnFamilyDescriptor name = new ColumnFamilyDescriptor(String.valueOf(hour).getBytes());
        return rocksDB.createColumnFamily(name);
    }

    private static int nowHour() {
        return LocalDateTime.now().get(ChronoField.HOUR_OF_DAY);
    }

    public static void main(String[] args) throws Exception {
        StorageConf conf = new StorageConf();
        conf.setString(ROCKSDB_ROOT_DIR, "/Users/hanhan.zhang/tmp/db");
        conf.setString(ROCKSDB_DELETE_ON_EXIT, "true");

        RocksDBStorageBackend storageBackend = new RocksDBStorageBackend();
        // 初始化
        storageBackend.init(conf);

        // 存放数据
        storageBackend.put("A", "1");
        storageBackend.put("B", "2");

        while (true) {
            System.out.println(storageBackend.get("B"));
            TimeUnit.SECONDS.sleep(20);
        }
    }
}
