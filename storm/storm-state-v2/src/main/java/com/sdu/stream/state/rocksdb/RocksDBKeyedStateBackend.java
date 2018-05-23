package com.sdu.stream.state.rocksdb;

import com.google.common.collect.Lists;
import com.sdu.stream.state.*;
import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.utils.ConfigConstants;
import com.sdu.stream.state.utils.FileUtils;
import com.sdu.stream.state.utils.Preconditions;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Stream;

import static com.sdu.stream.state.utils.IOUtils.closeQuietly;
import static java.lang.String.format;
import static org.rocksdb.RocksDB.DEFAULT_COLUMN_FAMILY;

public class RocksDBKeyedStateBackend<KEY> implements KeyedStateBackend<KEY> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKeyedStateBackend.class);

    private TypeSerializer<KEY> keyTypeSerializer;

    //----------------------------------RocksDB-------------------------------------

    /** Flag whether the native library has been loaded. */
    private static boolean rocksDbInitialized = false;
    /** The number of (re)tries for loading the RocksDB JNI library. */
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

    /** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
    public static final String MERGE_OPERATOR_NAME = "stringappendtest";

    /** RocksDB instance */
    protected RocksDB db;

    /** RocksDB根目录 */
    private final File rocksBaseDirectory;

    /** RocksDB数据存储目录 */
    private final File rocksDBDirectory;

    /** RocksDB lib目录 */
    private final File rocksLibDirectory;

    /** The column family options from the options factory. */
    private final ColumnFamilyOptions columnOptions;

    /** The DB options from the options factory. */
    private final DBOptions dbOptions;

    /** RocksDB写选项(弃用WAL) */
    private final WriteOptions writeOptions;

    /**
     * We are not using the default column family for Storm state ops, but we still need to remember this handle so that
     * we can close it properly when the backend is closed. This is required by RocksDB's native memory management.
     */
    private ColumnFamilyHandle defaultColumnFamily;

    /**  维护State与RocksDB数据存储句柄的映射关系 */
    private final Map<String, ColumnFamilyHandle> kvStateInformation;


    public RocksDBKeyedStateBackend(File instanceBasePath,
                                    DBOptions dbOptions,
                                    ColumnFamilyOptions columnFamilyOptions,
                                    TypeSerializer<KEY> keySerializer) throws IOException {
        this.keyTypeSerializer = Preconditions.checkNotNull(keySerializer);

        this.rocksBaseDirectory = instanceBasePath;
        checkAndCreateDirectory(instanceBasePath);

        this.rocksLibDirectory = new File(rocksBaseDirectory, "lib");
        ensureRocksDBIsLoaded(rocksLibDirectory);

        String rocksDBDirectoryPath = rocksBaseDirectory.getAbsolutePath() + "/store/" + UUID.randomUUID();
        this.rocksDBDirectory = new File(rocksDBDirectoryPath, "db");
        if (rocksDBDirectory.exists()) {
            // 删除RocksDB存储目录数据, 否则创建RocksDB实例异常
            FileUtils.cleanDirectory(rocksDBDirectory);
        } else {
            checkAndCreateDirectory(rocksDBDirectory);
        }

        this.kvStateInformation = new LinkedHashMap<>();

        this.dbOptions = Preconditions.checkNotNull(dbOptions);
        this.columnOptions = Preconditions.checkNotNull(columnFamilyOptions)
                                          .setMergeOperatorName(MERGE_OPERATOR_NAME);
        this.writeOptions = new WriteOptions().setDisableWAL(true);


        // 构建RocksDB实例
        createDB();
    }

    @Override
    public <N> Stream<KEY> getKeys(String state, N namespace) {
        return null;
    }

    @Override
    public <N, T> InternalValueState<N, KEY, T> createValueState(TypeSerializer<N> namespaceSerializer,
                                                                 ValueStateDescriptor<T> stateDesc) throws IOException {
        String stateName = stateDesc.getName();
        ColumnFamilyHandle columnFamily = createColumnFamily(stateName);
        kvStateInformation.put(stateName, columnFamily);
        return new RocksDBValueState<>(
                columnFamily,
                namespaceSerializer,
                stateDesc.getSerializer(),
                this);
    }

    @Override
    public <N, T> InternalListState<N, KEY, T> createListState(TypeSerializer<N> namespaceSerializer,
                                                               ListStateDescriptor<T> stateDesc) throws IOException {
        String stateName = stateDesc.getName();
        ColumnFamilyHandle columnFamily = createColumnFamily(stateName);
        kvStateInformation.put(stateName, columnFamily);
        return new RocksDBListState<>(
                columnFamily,
                namespaceSerializer,
                this,
                stateDesc.getElementSerializer());
    }

    @Override
    public <N, UK, UV> InternalMapState<N, KEY, UK, UV> createMapState(TypeSerializer<N> namespaceSerializer,
                                                                       MapStateDescriptor<UK, UV> stateDesc) throws IOException {
        String stateName = stateDesc.getName();
        ColumnFamilyHandle columnFamily = createColumnFamily(stateName);
        kvStateInformation.put(stateName, columnFamily);

        return new RocksDBMapState<>(
                columnFamily,
                namespaceSerializer,
                stateDesc.getUserKeySerializer(),
                stateDesc.getUserValueSerializer(),
                this);
    }

    @Override
    public TypeSerializer<KEY> getKeyTypeSerializer() {
        return keyTypeSerializer;
    }

    public WriteOptions getWriteOptions() {
        return writeOptions;
    }


    private void createDB() throws IOException {
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(1);
        this.db = openDB(rocksDBDirectory.getAbsolutePath(), Collections.emptyList(), columnFamilyHandles);
        this.defaultColumnFamily = columnFamilyHandles.get(0);
    }

    private RocksDB openDB(String path,
                           List<ColumnFamilyDescriptor> stateColumnFamilyDescriptors,
                           List<ColumnFamilyHandle> stateColumnFamilyHandles) throws IOException {
        // RocksDB Column Description
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = Lists.newArrayListWithCapacity(1 + stateColumnFamilyDescriptors.size());

        // we add the required descriptor for the default CF in FIRST position, see
        // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(DEFAULT_COLUMN_FAMILY, columnOptions));
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

    private static void ensureRocksDBIsLoaded(File rocksLibDir) throws IOException {
        synchronized (RocksDBKeyedStateBackend.class) {
            if (!rocksDbInitialized) {
                final File tempDirParent = rocksLibDir.getAbsoluteFile();
                LOGGER.info("Attempting to load RocksDB native library and store it under '{}'", tempDirParent);

                Throwable lastException = null;
                for (int i = 0; i < ROCKSDB_LIB_LOADING_ATTEMPTS; ++i) {
                    try {
                        // when multiple instances of this class and RocksDB exist in different
                        // class loaders, then we can see the following exception:
                        // "java.lang.UnsatisfiedLinkError: Native Library /path/to/temp/dir/librocksdbjni-linux64.so
                        // already loaded in another class loader"

                        // to avoid that, we need to add a random element to the library file path
                        // (I know, seems like an unnecessary hack, since the JVM obviously can handle multiple
                        //  instances of the same JNI library being loaded in different class loaders, but
                        //  apparently not when coming from the same file path, so there we go)

                        final File rocksLibFolder = new File(tempDirParent, "rocksdb-lib-" + UUID.randomUUID());

                        // make sure the temp path exists
                        LOGGER.debug("Attempting to create RocksDB native library folder {}", rocksLibFolder);
                        // noinspection ResultOfMethodCallIgnored
                        rocksLibFolder.mkdirs();

                        // explicitly load the JNI dependency if it has not been loaded before
                        NativeLibraryLoader.getInstance().loadLibrary(rocksLibFolder.getAbsolutePath());

                        // this initialization here should validate that the loading succeeded
                        RocksDB.loadLibrary();

                        // seems to have worked
                        LOGGER.info("Successfully loaded RocksDB native library");
                        rocksDbInitialized = true;
                        return;
                    } catch (Throwable t) {
                        lastException = t;
                        LOGGER.debug("RocksDB JNI library loading attempt {} failed", i, t);

                        // try to force RocksDB to attempt reloading the library
                        try {
                            resetRocksDBLoadedFlag();
                        } catch (Throwable tt) {
                            LOGGER.debug("Failed to reset 'initialized' flag in RocksDB native code loader", tt);
                        }
                    }
                }

                throw new IOException("Could not load the native RocksDB library", lastException);
            }
        }
    }

    private static void resetRocksDBLoadedFlag() throws Exception {
        final Field initField = org.rocksdb.NativeLibraryLoader.class.getDeclaredField("initialized");
        initField.setAccessible(true);
        initField.setBoolean(null, false);
    }

    private ColumnFamilyHandle createColumnFamily(String stateName) throws IOException {
        byte[] nameBytes = stateName.getBytes(ConfigConstants.DEFAULT_CHARSET);
        Preconditions.checkState(!Arrays.equals(DEFAULT_COLUMN_FAMILY, nameBytes),
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
                throw new IOException(directory + " is not a directory:");
            }
        } else {
            if (!directory.mkdirs()) {
                throw new IOException(
                        format("Could not create RocksDB data directory at %s.", directory));
            }
        }
    }

    @Override
    public void dispose() throws Exception {
        // IMPORTANT: null reference to signal potential async checkpoint workers that the db was disposed, as
        // working on the disposed object results in SEGFAULTS.
        if (db != null) {

            // RocksDB's native memory management requires that *all* CFs (including default) are closed before the
            // DB is closed. See:
            // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
            // Start with default CF ...
            closeQuietly(defaultColumnFamily);

            // ... continue with the ones created by storm...
            for (ColumnFamilyHandle handle : kvStateInformation.values()) {
                closeQuietly(handle);
            }

            // ... and finally close the DB instance ...
            closeQuietly(db);

            // invalidate the reference
            db = null;

            closeQuietly(columnOptions);
            closeQuietly(dbOptions);
            closeQuietly(writeOptions);
            kvStateInformation.clear();

            // TODO: 目录删除
        }
    }
}
