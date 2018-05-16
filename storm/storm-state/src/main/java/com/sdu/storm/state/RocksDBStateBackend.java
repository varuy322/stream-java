package com.sdu.storm.state;

import com.sdu.storm.configuration.CheckpointingOptions;
import com.sdu.storm.configuration.ConfigConstants;
import com.sdu.storm.fs.Path;
import com.sdu.storm.state.filesystem.FsStateBackend;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.AbstractID;
import com.sdu.storm.utils.TernaryBoolean;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.*;

import static com.sdu.storm.utils.Preconditions.checkNotNull;
import static java.lang.String.format;

public class RocksDBStateBackend extends AbstractStateBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStateBackend.class);

    //------------------------------------RockDB-----------------------------------------

    /** Flag whether the native library has been loaded. */
    private static boolean rocksDbInitialized = false;
    /** The number of (re)tries for loading the RocksDB JNI library. */
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

    /** Base paths for RocksDB directory, as initialized. */
    private transient File[] initializedDbBasePaths;

    /** Base paths for RocksDB directory, as configured.
     * Null if not yet set, in which case the configuration values will be used.
     * The configuration defaults to the TaskManager's temp directories. */
    @Nullable
    private File[] localRocksDbDirectories;

    /** The index of the next directory to be used from {@link #initializedDbBasePaths}.*/
    private transient int nextDirectory;

    /** Whether we already lazily initialized our local storage directories. */
    private transient boolean isInitialized;

    private PredefinedOptions predefinedOptions = PredefinedOptions.DEFAULT;

    private OptionsFactory optionsFactory;

    //------------------------------------Checkpoint--------------------------------------

    /** This determines if incremental checkpointing is enabled. */
    private final TernaryBoolean enableIncrementalCheckpointing;

    /** The state backend that we use for creating checkpoint streams. */
    private final StateBackend checkpointStreamBackend;

    /**
     * Creates a new {@code RocksDBStateBackend} that stores its checkpoint data in the
     * file system and location defined by the given URI.
     *
     * <p>A state backend that stores checkpoints in HDFS or S3 must specify the file system
     * host and port in the URI, or have the Hadoop configuration that describes the file system
     * (host / high-availability group / possibly credentials) either referenced from the Flink
     * config, or included in the classpath.
     *
     * @param checkpointDataUri The URI describing the filesystem and path to the checkpoint data directory.
     * @param enableIncrementalCheckpointing True if incremental checkpointing is enabled.
     * @throws IOException Thrown, if no file system can be found for the scheme in the URI.
     */
    public RocksDBStateBackend(String checkpointDataUri, boolean enableIncrementalCheckpointing) throws IOException {
        this(new FsStateBackend(checkpointDataUri), TernaryBoolean.fromBoxedBoolean(enableIncrementalCheckpointing));
    }

    public RocksDBStateBackend(StateBackend checkpointStreamBackend, TernaryBoolean enableIncrementalCheckpointing) {
        this.checkpointStreamBackend = checkNotNull(checkpointStreamBackend);
        this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
    }


    public void setPredefinedOptions(PredefinedOptions predefinedOptions) {
        this.predefinedOptions = predefinedOptions;
    }

    public void setOptionsFactory(OptionsFactory optionsFactory) {
        this.optionsFactory = optionsFactory;
    }

    public void setDbStoragePath(String path) {
        setDbStoragePath(path == null ? null : new String[] {path});
    }

    private void setDbStoragePath(String ... paths) {
        if (paths == null) {
            localRocksDbDirectories = null;
        } else if (paths.length == 0){
            throw new IllegalArgumentException("empty paths");
        } else {
            File[] pp = new File[paths.length];

            for (int i = 0; i < paths.length; ++i) {
                String rawPath = paths[i];
                String path;

                if (rawPath == null) {
                    throw new IllegalArgumentException("null path");
                }

                URI uri = null;
                try {
                    uri = new Path(rawPath).toUri();
                } catch (Exception e) {
                    // ignore
                }

                if (uri != null && uri.getScheme() != null) {
                    if ("file".equalsIgnoreCase(uri.getScheme())) {
                        path = uri.getPath();
                    } else {
                        throw new IllegalArgumentException("Path " + rawPath + " has a non-local scheme");
                    }
                } else {
                    path = rawPath;
                }

                pp[i] = new File(path);
                if (!pp[i].isAbsolute()) {
                    throw new IllegalArgumentException("Relative paths are not supported");
                }
            }

            localRocksDbDirectories = pp;

        }
    }

    public DBOptions getDbOptions() {
        // initial options from pre-defined profile
        DBOptions opt = predefinedOptions.createDBOptions();

        // add user-defined options, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createDBOptions(opt);
        }

        // add necessary default options
        opt = opt.setCreateIfMissing(true);

        return opt;
    }

    public ColumnFamilyOptions getColumnOptions() {
        // initial options from pre-defined profile
        ColumnFamilyOptions opt = predefinedOptions.createColumnOptions();

        // add user-defined options, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createColumnOptions(opt);
        }

        return opt;
    }

    /**
     * Gets whether incremental checkpoints are enabled for this state backend.
     */
    public boolean isIncrementalCheckpointsEnabled() {
        return enableIncrementalCheckpointing.getOrDefault(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.defaultValue());
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Map stormConf,
            String component,
            int taskId,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange) throws IOException {

        // first, make sure that the RocksDB JNI library is loaded
        // we do this explicitly here to have better error handling
        String tmpDir = (String) stormConf.get(ConfigConstants.TOPOLOGY_STATE_DIR);
        ensureRocksDBIsLoaded(tmpDir);

        // initialize rock db store path
        lazyInitializeForJob();

        // chose rock db store path
        File instanceBasePath = new File(
                getNextStoragePath(),
                "component_" + component + "_task_" + taskId + "_uuid_" + UUID.randomUUID());

        return new RocksDBKeyedStateBackend<>(
                instanceBasePath,
                getDbOptions(),
                getColumnOptions(),
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                isIncrementalCheckpointsEnabled());
    }

    /** check and initialize {@link #nextDirectory} */
    private void lazyInitializeForJob() throws IOException {
        if (isInitialized) {
            return;
        }

        if (localRocksDbDirectories == null || localRocksDbDirectories.length == 0) {
            throw new IllegalStateException("Local DB files directory should initialize by setDbStoragePath()");
        }

        List<File> dirs = new ArrayList<>(localRocksDbDirectories.length);
        StringBuilder errorMessage = new StringBuilder();

        for (File f : localRocksDbDirectories) {
            File testDir = new File(f, UUID.randomUUID().toString());
            if (!testDir.mkdirs()) {
                String msg = format("Local DB files directory %s does not exist and cannot be created.", f);
                LOGGER.error(msg);
                errorMessage.append(msg);
            } else {
                dirs.add(f);
            }
            //noinspection ResultOfMethodCallIgnored
            testDir.delete();
        }

        if (dirs.isEmpty()) {
            throw new IOException("No local storage directories available. " + errorMessage);
        } else {
            initializedDbBasePaths = dirs.toArray(new File[dirs.size()]);
        }

        isInitialized = true;
        nextDirectory = new Random().nextInt(initializedDbBasePaths.length);
    }

    private File getNextStoragePath() {
        int ni = nextDirectory + 1;
        ni = ni >= initializedDbBasePaths.length ? 0 : ni;
        nextDirectory = ni;

        return initializedDbBasePaths[ni];
    }

    /** load rockdb library */
    private void ensureRocksDBIsLoaded(String tempDirectory) throws IOException {
        // 防止其他线程在同一个目录创建, 锁RocksDBStateBackend.class对象
        synchronized (RocksDBStateBackend.class) {
            if (!rocksDbInitialized) {
                final File tempDirParent = new File(tempDirectory).getAbsoluteFile();
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

                        final File rocksLibFolder = new File(tempDirParent, "rocksdb-lib-" + new AbstractID());

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
}
