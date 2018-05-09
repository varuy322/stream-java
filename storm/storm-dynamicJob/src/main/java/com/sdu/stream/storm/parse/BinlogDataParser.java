package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.utils.RTDParseException;

/**
 * MySql Binlog日志解析(基于阿里巴巴canal)
 *
 * @See AviaterRegexFilter
 *
 * @author hanhan.zhang
 * */
public class BinlogDataParser implements DataParser<byte[]> {

    @Override
    public DataRow parse(byte[] data) throws RTDParseException {
        return null;
    }

}
