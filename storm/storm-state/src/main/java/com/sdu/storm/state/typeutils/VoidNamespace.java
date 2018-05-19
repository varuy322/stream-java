package com.sdu.storm.state.typeutils;

public class VoidNamespace {

    /** The singleton instance */
    public static final VoidNamespace INSTANCE = new VoidNamespace();

    /** Getter for the singleton instance */
    public static VoidNamespace get() {
        return INSTANCE;
    }

    /** This class should not be instantiated */
    private VoidNamespace() {}

    @Override
    public int hashCode() {
        return 99;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }


}
