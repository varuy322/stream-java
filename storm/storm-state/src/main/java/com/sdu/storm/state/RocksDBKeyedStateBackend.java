package com.sdu.storm.state;

import com.sdu.storm.conf.ConfigConstants;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.Preconditions;
import com.sdu.storm.types.Tuple2;
import org.rocksdb.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * StateBackendTestBase
 *
 * @author hanhan.zhang
 * */
public class RocksDBKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    /** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
    public static final String MERGE_OPERATOR_NAME = "stringappendtest";

    /** Number of bytes required to prefix the key groups. */
    private final int keyGroupPrefixBytes;

    /**
     * Our RocksDB database, this is used by the actual subclasses of {@link AbstractRocksDBState}
     * to store state. The different k/v states that we have don't each have their own RocksDB
     * instance. They all write to this instance but to their own column family.
     */
    protected RocksDB db;

    /** The column family options from the options factory. */
    private final ColumnFamilyOptions columnOptions;

    /** The DB options from the options factory. */
    private final DBOptions dbOptions;


    /**
     * Information about the k/v states as we create them. This is used to retrieve the
     * column family that is used for a state and also for sanity checks when restoring.
     */
    private final Map<String, Tuple2<ColumnFamilyHandle, RegisteredKeyedBackendStateMetaInfo<?, ?>>> kvStateInformation;


    public RocksDBKeyedStateBackend(DBOptions dbOptions,
                                    ColumnFamilyOptions columnFamilyOptions,
                                    TypeSerializer<K> keySerializer, int numberOfKeyGroups) {
        super(keySerializer, numberOfKeyGroups);

        this.dbOptions = Preconditions.checkNotNull(dbOptions);
        this.columnOptions = Preconditions.checkNotNull(columnFamilyOptions)
                                          .setMergeOperatorName(MERGE_OPERATOR_NAME);


        this.keyGroupPrefixBytes = getNumberOfKeyGroups() > (Byte.MAX_VALUE + 1) ? 2 : 1;

        this.kvStateInformation = new LinkedHashMap<>();


    }

    @Override
    protected <N, T> InternalListState<K, N, T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
        return null;
    }

    public WriteOptions getWriteOptions() {
        throw new RuntimeException("");
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
}
