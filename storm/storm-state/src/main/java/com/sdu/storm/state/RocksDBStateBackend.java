package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.AbstractID;
import org.rocksdb.NativeLibraryLoader;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

public class RocksDBStateBackend extends AbstractStateBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBStateBackend.class);

    /** Flag whether the native library has been loaded. */
    private static boolean rocksDbInitialized = false;
    /** The number of (re)tries for loading the RocksDB JNI library. */
    private static final int ROCKSDB_LIB_LOADING_ATTEMPTS = 3;

    private PredefinedOptions predefinedOptions = PredefinedOptions.DEFAULT;

    private OptionsFactory optionsFactory;

    public void setPredefinedOptions(PredefinedOptions predefinedOptions) {
        this.predefinedOptions = predefinedOptions;
    }

    public void setOptionsFactory(OptionsFactory optionsFactory) {
        this.optionsFactory = optionsFactory;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange) {
        return null;
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
