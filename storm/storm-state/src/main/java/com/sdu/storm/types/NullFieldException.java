package com.sdu.storm.types;

public class NullFieldException extends RuntimeException {

    private final int fieldPos;

    public NullFieldException(int fieldIdx) {
        super("Field " + fieldIdx + " is null, but expected to hold a value.");
        this.fieldPos = fieldIdx;
    }

}
