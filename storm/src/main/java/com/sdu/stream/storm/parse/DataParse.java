package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.utils.RTDParseException;
import org.apache.storm.tuple.Tuple;

/**
 * 数据解析
 *
 * @author hanhan.zhang
 * */
public interface DataParse<T> {

    Tuple parse(T data) throws RTDParseException;

}
