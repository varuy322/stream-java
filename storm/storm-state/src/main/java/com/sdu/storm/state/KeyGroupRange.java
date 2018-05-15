package com.sdu.storm.state;

import com.sdu.storm.utils.Preconditions;
import com.sun.istack.internal.NotNull;

import java.io.Serializable;
import java.util.Iterator;

public class KeyGroupRange implements KeyGroupsList, Serializable {

    /** The empty key-group */
    public static final KeyGroupRange EMPTY_KEY_GROUP_RANGE = new KeyGroupRange();

    private final int startKeyGroup;
    private final int endKeyGroup;

    /**
     * Empty KeyGroup Constructor
     */
    private KeyGroupRange() {
        this.startKeyGroup = 0;
        this.endKeyGroup = -1;
    }

    /**
     * Defines the range [startKeyGroup, endKeyGroup]
     *
     * @param startKeyGroup start of the range (inclusive)
     * @param endKeyGroup end of the range (inclusive)
     */
    public KeyGroupRange(int startKeyGroup, int endKeyGroup) {
        Preconditions.checkArgument(startKeyGroup >= 0);
        Preconditions.checkArgument(startKeyGroup <= endKeyGroup);
        this.startKeyGroup = startKeyGroup;
        this.endKeyGroup = endKeyGroup;
        Preconditions.checkArgument(getNumberOfKeyGroups() >= 0, "Potential overflow detected.");
    }

    @Override
    public int getNumberOfKeyGroups() {
        return endKeyGroup - startKeyGroup + 1;
    }

    @Override
    public int getKeyGroupId(int idx) {
        if (idx < 0 || idx > getNumberOfKeyGroups()) {
            throw new IndexOutOfBoundsException("Key group index out of bounds: " + idx);
        }
        return startKeyGroup + idx;
    }

    @Override
    public boolean contains(int keyGroupId) {
        return keyGroupId >= startKeyGroup && keyGroupId <= endKeyGroup;
    }

    @NotNull
    @Override
    public Iterator<Integer> iterator() {
        return new KeyGroupIterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KeyGroupRange)) {
            return false;
        }

        KeyGroupRange that = (KeyGroupRange) o;
        return startKeyGroup == that.startKeyGroup && endKeyGroup == that.endKeyGroup;
    }


    @Override
    public int hashCode() {
        int result = startKeyGroup;
        result = 31 * result + endKeyGroup;
        return result;
    }

    @Override
    public String toString() {
        return "KeyGroupRange{" +
                "startKeyGroup=" + startKeyGroup +
                ", endKeyGroup=" + endKeyGroup +
                '}';
    }

    private class KeyGroupIterator implements Iterator<Integer> {

        int iteratorPos = 0;

        @Override
        public boolean hasNext() {
            return iteratorPos < getNumberOfKeyGroups();
        }

        @Override
        public Integer next() {
            int rv = startKeyGroup + iteratorPos;
            ++iteratorPos;
            return rv;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Unsupported by this iterator!");
        }
    }
}
