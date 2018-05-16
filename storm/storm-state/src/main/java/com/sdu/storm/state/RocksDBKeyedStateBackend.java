package com.sdu.storm.state;

import com.google.common.collect.Lists;
import com.sdu.storm.configuration.ConfigConstants;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.FileUtils;
import com.sdu.storm.utils.Preconditions;
import com.sdu.storm.types.Tuple2;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 *
 * StateBackendTestBase
 *
 * @author hanhan.zhang
 * */
public class RocksDBKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKeyedStateBackend.class);

    /** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
    public static final String MERGE_OPERATOR_NAME = "stringappendtest";

    /** Number of bytes required to prefix the key groups. */
    private final int keyGroupPrefixBytes;

    // -------------------------------------------------------------------------------------------

    /**
     * Our RocksDB database, this is used by the actual subclasses of {@link AbstractRocksDBState}
     * to store state. The different k/v states that we have don't each have their own RocksDB
     * instance. They all write to this instance but to their own column family.
     */
    protected RocksDB db;

    /** Path where this configured instance stores its data directory. */
    private final File instanceBasePath;

    /** Path where this configured instance stores its RocksDB database. */
    private final File instanceRocksDBPath;

    /** The column family options from the options factory. */
    private final ColumnFamilyOptions columnOptions;

    /** The DB options from the options factory. */
    private final DBOptions dbOptions;

    /**
     * The write options to use in the states. We disable write ahead logging.
     */
    private final WriteOptions writeOptions;

    /**
     * We are not using the default column family for Storm state ops, but we still need to remember this handle so that
     * we can close it properly when the backend is closed. This is required by RocksDB's native memory management.
     */
    private ColumnFamilyHandle defaultColumnFamily;

    // -------------------------------------------------------------------------------------------

    /**
     * Information about the k/v states as we create them. This is used to retrieve the
     * column family that is used for a state and also for sanity checks when restoring.
     */
    private final Map<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation;

    /** True if incremental checkpointing is enabled. */
    private final boolean enableIncrementalCheckpointing;

    public RocksDBKeyedStateBackend(File instanceRocksDBPath,
                                    DBOptions dbOptions,
                                    ColumnFamilyOptions columnFamilyOptions,
                                    TypeSerializer<K> keySerializer,
                                    int numberOfKeyGroups,
                                    KeyGroupRange keyGroupRange,
                                    boolean enableIncrementalCheckpointing) throws IOException {
        super(keySerializer, numberOfKeyGroups, keyGroupRange);

        this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;

        this.instanceBasePath = Preconditions.checkNotNull(instanceRocksDBPath);
        this.instanceRocksDBPath = new File(instanceBasePath, "db");
        checkAndCreateDirectory(instanceBasePath);
        if (instanceRocksDBPath.exists()) {
            // Clear the base directory when the backend is created
            // in case something crashed and the backend never reached dispose()
            cleanInstanceBasePath();
        }

        this.dbOptions = Preconditions.checkNotNull(dbOptions);
        this.columnOptions = Preconditions.checkNotNull(columnFamilyOptions)
                                          .setMergeOperatorName(MERGE_OPERATOR_NAME);
        this.writeOptions = new WriteOptions().setDisableWAL(true);

        this.keyGroupPrefixBytes = getNumberOfKeyGroups() > (Byte.MAX_VALUE + 1) ? 2 : 1;

        this.kvStateInformation = new LinkedHashMap<>();

        // Flink restore()触发RockDB初始化操作
        createDB();
    }

    @Override
    public  <N, T> InternalListState<K, N, T> createListState(
            TypeSerializer<N> namespaceSerializer,
            ListStateDescriptor<T> stateDesc) throws Exception {
        Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, List<T>>> registerResult =
                tryRegisterKvStateInformation(stateDesc, namespaceSerializer);

        return new RocksDBListState<>(
                registerResult.f0,
                registerResult.f1.getNamespaceSerializer(),
                registerResult.f1.getStateSerializer(),
                stateDesc.getDefaultValue(),
                stateDesc.getElementSerializer(),
                this);
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return null;
    }

    @Override
    public void dispose() throws Exception {
        // TODO: 待实现
    }

    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    private void createDB() throws IOException {
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
        this.db = openDB(instanceRocksDBPath.getAbsolutePath(), Collections.emptyList(), columnFamilyHandles);
        this.defaultColumnFamily = columnFamilyHandles.get(0);
    }


    private RocksDB openDB(String path,
                           List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
                           List<ColumnFamilyHandle> stateColumnFamilyHandles) throws IOException {
        // RocksDB Column Description
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = Lists.newArrayListWithCapacity(1 + stateColumnFamilyDescriptors.size());

        // we add the required descriptor for the default CF in FIRST position, see
        // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions));
        columnFamilyDescriptors.addAll(stateColumnFamilyDescriptors);

        RocksDB dbRef;

        try {
            dbRef = RocksDB.open(
                    Preconditions.checkNotNull(dbOptions),
                    Preconditions.checkNotNull(path),
                    columnFamilyDescriptors,
                    stateColumnFamilyHandles);
        } catch (RocksDBException e) {
            throw new IOException("Error while opening RocksDB instance.", e);
        }

        // requested + default CF
        Preconditions.checkState(1 + stateColumnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
                "Not all requested column family handles have been created");

        return dbRef;
    }

    private <N, S> Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<N, S>> tryRegisterKvStateInformation(
            StateDescriptor<?, S> stateDesc,
            TypeSerializer<N> namespaceSerializer) throws IOException {
        Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>> stateInfo =
                kvStateInformation.get(stateDesc.getName());

        RegisteredKeyedBackendStateMetaInfo<N, S> newMetaInfo = null;
        if (stateInfo != null) {
            // State Snapshot
            // TODO:
            throw new RuntimeException("TODO");
        }  else {
            String stateName = stateDesc.getName();
            newMetaInfo = new RegisteredKeyedBackendStateMetaInfo<>(
                    stateDesc.getType(),
                    stateName,
                    namespaceSerializer,
                    stateDesc.getSerializer());

            ColumnFamilyHandle columnFamily = createColumnFamily(stateName);
            stateInfo = Tuple2.of(columnFamily, newMetaInfo);
            kvStateInformation.put(stateDesc.getName(), stateInfo);
        }

        return Tuple2.of(stateInfo.f0, newMetaInfo);
    }

    private ColumnFamilyHandle createColumnFamily(String stateName) throws IOException {
        byte[] nameBytes = stateName.getBytes(ConfigConstants.DEFAULT_CHARSET);
        Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
                "The chosen state name 'default' collides with the name of the default column family!");

        ColumnFamilyDescriptor columnDescriptor = new ColumnFamilyDescriptor(nameBytes, columnOptions);

        try {
            return db.createColumnFamily(columnDescriptor);
        } catch (RocksDBException e) {
            throw new IOException("Error creating ColumnFamilyHandle.", e);
        }
    }

    private static void checkAndCreateDirectory(File directory) throws IOException {
        if (directory.exists()) {
            if (!directory.isDirectory()) {
                throw new IOException("Not a directory: " + directory);
            }
        } else {
            if (!directory.mkdirs()) {
                throw new IOException(
                        String.format("Could not create RocksDB data directory at %s.", directory));
            }
        }
    }

    private void cleanInstanceBasePath() {
        LOGGER.info("Deleting existing instance base directory {}.", instanceBasePath);

        try {
            FileUtils.deleteDirectory(instanceBasePath);
        } catch (IOException ex) {
            LOGGER.warn("Could not delete instance base path for RocksDB: " + instanceBasePath, ex);
        }
    }
}
