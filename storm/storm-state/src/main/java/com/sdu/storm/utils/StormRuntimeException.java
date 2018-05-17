package com.sdu.storm.utils;

public class StormRuntimeException extends RuntimeException {

    public StormRuntimeException(String message) {
        super(message);
    }

    public StormRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
