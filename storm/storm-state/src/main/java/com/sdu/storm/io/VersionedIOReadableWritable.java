package com.sdu.storm.io;

import com.sdu.storm.fs.IOReadableWritable;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;
import java.util.Arrays;

public abstract class VersionedIOReadableWritable implements IOReadableWritable, Versioned {

    private int readVersion = Integer.MIN_VALUE;

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeInt(getVersion());
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.readVersion = in.readInt();
        resolveVersionRead(this.readVersion);
    }

    public int getReadVersion() {
        return (readVersion == Integer.MIN_VALUE) ? getVersion() : readVersion;
    }

    public int[] getCompatibleVersions() {
        return new int[] {getVersion()};
    }

    private void resolveVersionRead(int readVersion) throws VersionMismatchException {

        int[] compatibleVersions = getCompatibleVersions();
        for (int compatibleVersion : compatibleVersions) {
            if (compatibleVersion == readVersion) {
                return;
            }
        }

        throw new VersionMismatchException(
                "Incompatible version: found " + readVersion + ", compatible versions are " + Arrays.toString(compatibleVersions));
    }
}
