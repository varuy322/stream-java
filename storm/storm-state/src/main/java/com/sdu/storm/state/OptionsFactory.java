package com.sdu.storm.state;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.Serializable;

public interface OptionsFactory extends Serializable {

    DBOptions createDBOptions(DBOptions currentOptions);

    ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions);

}
