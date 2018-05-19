package com.sdu.storm.topology.window;

import com.sdu.storm.state.typeutils.base.TypeSerializerSingleton;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;

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

    public static final class TimeWindowSerializer extends TypeSerializerSingleton<TimeWindow> {

        public static final TimeWindowSerializer INSTANCE = new TimeWindowSerializer();

        private TimeWindowSerializer() { }

        @Override
        public TimeWindow createInstance() {
            return null;
        }

        @Override
        public int getLength() {
            return 0;
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public void serialize(TimeWindow record, DataOutputView target) throws IOException {
            target.writeLong(record.getStart());
            target.writeLong(record.getEnd());
        }

        @Override
        public TimeWindow deserialize(DataInputView source) throws IOException {
            return new TimeWindow(source.readLong(), source.readLong());
        }

        @Override
        public TimeWindow copy(TimeWindow from) {
            return from;
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof TimeWindowSerializer;
        }
    }
}
