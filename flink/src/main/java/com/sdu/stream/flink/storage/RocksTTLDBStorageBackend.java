package com.sdu.stream.flink.storage;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.math.NumberUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.split;

/**
 *
 * @author hanhan.zhang
 * */
public class RocksTTLDBStorageBackend extends AbstractRocksStorageBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksTTLDBStorageBackend.class);

    private static final String ROCKSDB_TTL_LIST = "rocksdb.ttl";

    private TtlDB ttlDB;
    private TreeMap<Integer, ColumnFamilyHandle> windowsHandlers = new TreeMap<>();

    @Override
    public void initRocksDB(StorageConf conf) throws Exception {
        LOGGER.info("Start initializer RockDB, root dir: {}", this.rootDir);

        String ttl = conf.getString(ROCKSDB_TTL_LIST, "");
        if (isEmpty(ttl)) {
            throw new RocksDBException("RocksDB ttl list empty");
        }

        String[] timeouts = split(ttl, ",");

        DBOptions dbOptions = null;

        List<ColumnFamilyDescriptor> columnFamilyNames = Lists.newArrayList();
        columnFamilyNames.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (String timeout : timeouts) {
            byte[] columnFamilyName = String.valueOf(timeout).getBytes();
            columnFamilyNames.add(new ColumnFamilyDescriptor(columnFamilyName));
        }

        List<Integer> ttlValues = Lists.newArrayList();
        // Default column family with infinite lifetime
        // ATTENSION, the first must be 0, RocksDB.java API has this limitation
        ttlValues.add(0);
        // new column family with list second ttl
        ttlValues.addAll(Arrays.stream(timeouts).map(NumberUtils::toInt).collect(Collectors.toList()));

        try {
            dbOptions = new DBOptions().setCreateMissingColumnFamilies(true).setCreateIfMissing(true);

            List<ColumnFamilyHandle> columnFamilyHandles = Lists.newArrayList();

            ttlDB = TtlDB.open(dbOptions,
                    rootDir,
                    columnFamilyNames,
                    columnFamilyHandles,
                    ttlValues,
                    false);
            for (int i = 0; i < ttlValues.size(); ++i) {
                windowsHandlers.put(ttlValues.get(i), columnFamilyHandles.get(i));
            }

            LOGGER.info("Successfully initializer RocksDB, root dir {}", rootDir);
        } finally {
            if (dbOptions != null) {
                dbOptions.close();
            }
        }
    }

    @Override
    public void put(String key, String value, int timeoutSecond) {

    }

    @Override
    public void put(String key, String value) {

    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public void remove(String key) {

    }

    @Override
    public void dispose() throws Exception {
        LOGGER.info("Start close RocksDB, root dir: {}", this.rootDir);

        for (Map.Entry<Integer, ColumnFamilyHandle> entry : windowsHandlers.entrySet()) {
            ColumnFamilyHandle handle = entry.getValue();
            if (handle != null) {
                handle.close();
            }
        }

        if (ttlDB != null) {
            ttlDB.close();
        }

        LOGGER.info("Successfully close RocksDB, root dir: {}", this.rootDir);
    }


}
