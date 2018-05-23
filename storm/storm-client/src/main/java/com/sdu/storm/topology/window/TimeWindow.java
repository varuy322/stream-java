package com.sdu.storm.topology.window;

import com.sdu.stream.state.seralizer.TypeSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 *
 * @author hanhan.zhang
 * */
public class TimeWindow implements Serializable {

    private final long start;
    private final long end;

    public TimeWindow(long start, long end) {
        this.start = start;
        this.end = end;
    }

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimeWindow window = (TimeWindow) o;

        return end == window.end && start == window.start;
    }

    @Override
    public int hashCode() {
        int result = (int) (start ^ (start >>> 32));
        result = 31 * result + (int) (end ^ (end >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "TimeWindow[start=" + start + ", end=" + end + ']';
    }

    public static final class TimeWindowSerializer extends TypeSerializer<TimeWindow> {

        public static final TimeWindowSerializer INSTANCE = new TimeWindowSerializer();

        private TimeWindowSerializer() { }

        @Override
        public void serializer(TimeWindow record, DataOutput target) throws IOException {
            target.writeLong(record.getStart());
            target.writeLong(record.getEnd());
        }

        @Override
        public TimeWindow deserialize(DataInput source) throws IOException {
            return new TimeWindow(source.readLong(), source.readLong());
        }
    }
}
