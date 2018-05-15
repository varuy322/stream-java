package com.sdu.storm.state;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

public enum PredefinedOptions {

    DEFAULT {
        @Override
        public DBOptions createDBOptions() {
            return null;
        }

        @Override
        public ColumnFamilyOptions createColumnOptions() {
            return null;
        }
    },

    SPINNING_DISK_OPTIMIZED {
        @Override
        public DBOptions createDBOptions() {
            return null;
        }

        @Override
        public ColumnFamilyOptions createColumnOptions() {
            return null;
        }
    },

    SPINNING_DISK_OPTIMIZED_HIGH_MEM {
        @Override
        public DBOptions createDBOptions() {
            return null;
        }

        @Override
        public ColumnFamilyOptions createColumnOptions() {
            return null;
        }
    },

    FLASH_SSD_OPTIMIZED {
        @Override
        public DBOptions createDBOptions() {
            return null;
        }

        @Override
        public ColumnFamilyOptions createColumnOptions() {
            return null;
        }
    };

    public abstract DBOptions createDBOptions();

    public abstract ColumnFamilyOptions createColumnOptions();

}
