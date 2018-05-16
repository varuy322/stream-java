package com.sdu.storm.state;

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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
