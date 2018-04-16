package com.sdu.stream.storm.parse;

import com.sdu.stream.storm.utils.RTDParseException;

/**
 * 数据解析
 *
 * @author hanhan.zhang
 * */
public interface DataParser<T> {

    DataRow parse(T data) throws RTDParseException;

}
