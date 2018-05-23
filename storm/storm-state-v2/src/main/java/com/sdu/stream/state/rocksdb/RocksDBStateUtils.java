package com.sdu.stream.state.rocksdb;

import com.sdu.stream.state.seralizer.TypeSerializer;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.File;
import java.io.IOException;

public class RocksDBStateUtils {

    public static  <KEY> RocksDBKeyedStateBackend<KEY> createRocksDBKeyStateBackend(
            File rocksDBBaseDir,
            PredefinedOptions options,
            TypeSerializer<KEY> keySerializer) throws IOException {
        return createRocksDBKeyStateBackend(
                rocksDBBaseDir,
                options,
                null,
                keySerializer);
    }


    public static  <KEY> RocksDBKeyedStateBackend<KEY> createRocksDBKeyStateBackend(
            File rocksDBBaseDir,
            PredefinedOptions options,
            OptionsFactory factory,
            TypeSerializer<KEY> keySerializer) throws IOException {
        DBOptions dbOptions = options.createDBOptions();
        ColumnFamilyOptions columnFamilyOptions = options.createColumnOptions();

        if (factory != null) {
            dbOptions = factory.createDBOptions(dbOptions);
            columnFamilyOptions = factory.createColumnOptions(columnFamilyOptions);
        }

        return new RocksDBKeyedStateBackend<>(
                rocksDBBaseDir,
                dbOptions,
                columnFamilyOptions,
                keySerializer);
    }

}
