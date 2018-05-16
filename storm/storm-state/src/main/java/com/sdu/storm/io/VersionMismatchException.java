package com.sdu.storm.io;

import java.io.IOException;

public class VersionMismatchException extends IOException {

    public VersionMismatchException(String message) {
        super(message);
    }
}
