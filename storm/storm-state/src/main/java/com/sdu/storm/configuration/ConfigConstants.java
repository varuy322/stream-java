package com.sdu.storm.configuration;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class ConfigConstants {

    public static final String TOPOLOGY_STATE_ROCKSDB_LIB_DIR = "storm.topology.state.rocksdb.lib.tmpdir";

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

}
