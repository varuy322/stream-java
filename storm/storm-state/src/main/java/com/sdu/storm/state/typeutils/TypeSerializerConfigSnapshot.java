package com.sdu.storm.state.typeutils;

import com.sdu.storm.io.VersionedIOReadableWritable;

public abstract class TypeSerializerConfigSnapshot extends VersionedIOReadableWritable {

    /** The user code class loader; only relevant if this configuration instance was deserialized from binary form. */
    private ClassLoader userCodeClassLoader;

    public ClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }

    public void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = userCodeClassLoader;
    }

    public abstract boolean equals(Object obj);

    public abstract int hashCode();
}
