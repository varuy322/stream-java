package com.sdu.stream.state.rocksdb;

import org.rocksdb.*;

public enum PredefinedOptions {

    DEFAULT {
        @Override
        public DBOptions createDBOptions() {
            return new DBOptions()
                    .setCreateIfMissing(true)
                    .setUseFsync(false);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions() {
            return new ColumnFamilyOptions();
        }
    },

    SPINNING_DISK_OPTIMIZED {
        @Override
        public DBOptions createDBOptions() {
            return new DBOptions()
                    .setCreateIfMissing(true)
                    .setIncreaseParallelism(4)
                    .setUseFsync(false)
                    .setMaxOpenFiles(-1);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions() {
            return new ColumnFamilyOptions()
                    .setCompactionStyle(CompactionStyle.LEVEL)
                    .setLevelCompactionDynamicLevelBytes(true);
        }
    },

    SPINNING_DISK_OPTIMIZED_HIGH_MEM {
        @Override
        public DBOptions createDBOptions() {
            return new DBOptions()
                    .setCreateIfMissing(true)
                    .setIncreaseParallelism(4)
                    .setUseFsync(false)
                    .setMaxOpenFiles(-1);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions() {
            final long blockCacheSize = 256 * 1024 * 1024;
            final long blockSize = 128 * 1024;
            final long targetFileSize = 256 * 1024 * 1024;
            final long writeBufferSize = 64 * 1024 * 1024;

            return new ColumnFamilyOptions()
                    .setLevelCompactionDynamicLevelBytes(true)
                    .setCompactionStyle(CompactionStyle.LEVEL)
                    .setTargetFileSizeBase(targetFileSize)
                    .setMaxBytesForLevelBase(4 * targetFileSize)
                    .setWriteBufferSize(writeBufferSize)
                    .setMinWriteBufferNumberToMerge(3)
                    .setMaxWriteBufferNumber(4)
                    .setTableFormatConfig(
                            new BlockBasedTableConfig()
                                    .setBlockCacheSize(blockCacheSize)
                                    .setBlockSize(blockSize)
                                    .setFilter(new BloomFilter())
                    );
        }
    },

    FLASH_SSD_OPTIMIZED {
        @Override
        public DBOptions createDBOptions() {
            return new DBOptions()
                    .setCreateIfMissing(true)
                    .setIncreaseParallelism(4)
                    .setUseFsync(false)
                    .setMaxOpenFiles(-1);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions() {
            return new ColumnFamilyOptions();
        }
    };

    public abstract DBOptions createDBOptions();

    public abstract ColumnFamilyOptions createColumnOptions();

}
