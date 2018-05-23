package com.sdu.stream.state;

import java.io.IOException;

public abstract class KeyStateBackendTest<KEY> {

    public abstract KeyedStateBackend<KEY> createKeyStateBackend() throws IOException;

}
