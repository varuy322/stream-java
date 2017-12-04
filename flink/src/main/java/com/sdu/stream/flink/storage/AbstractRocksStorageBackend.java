package com.sdu.stream.flink.storage;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * @author hanhan.zhang
 * */
public abstract class AbstractRocksStorageBackend implements StorageBackend {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRocksStorageBackend.class);

    protected String rootDir;

    @Override
    public void init(StorageConf conf) throws Exception {
        // step1: 创建RocksDB缓存目录
        String confDir = conf.getRocksDBDir("");
        if (isEmpty(confDir)) {
            throw new RocksDBException("RocksDB root dir empty");
        }
        File rootDir = new File(confDir);
        if (!rootDir.exists()) {
            boolean success = rootDir.mkdir();
            if (!success) {
                throw new RocksDBException("RocksDB create dir failure, path: " + confDir);
            }
        }
        this.rootDir = rootDir.getAbsolutePath();

        // step2: 进程关闭时, 删除存储文件
        boolean deleteOnExit = conf.isRocksDBDeleteOnExit(true);
        if (deleteOnExit) {
            Runtime.getRuntime().addShutdownHook(new Shutdown(this.rootDir));
        }

        // step3: 初始化RocksDB
        initRocksDB(conf);
    }

    public abstract void initRocksDB(StorageConf conf) throws Exception;

    private class Shutdown extends Thread {

        private String dirPath;

        Shutdown(String dirPath) {
            super("RocksDB-Shutdown-Hook");
            this.dirPath = dirPath;
        }

        @Override
        public void run() {
            File dir = new File(dirPath);
            deleteFile(dir);
        }

        private void deleteFile(File file) {
            if(file.exists()) {
                if (file.isFile()) {
                    if (!file.delete()) {
                        LOGGER.warn("Delete RocksDB file {} failure", file.getAbsolutePath());
                    }
                } else {
                    File[] files = file.listFiles();
                    if (files != null && files.length > 0) {
                        for (File f : files) {
                            deleteFile(f);
                        }
                    }
                }
            }
        }
    }
}
