package com.sdu.storm.topology.types;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.storm.state.typeutils.base.*;
import com.sdu.storm.topology.types.base.*;
import com.sdu.storm.utils.DataInputView;
import com.sdu.storm.utils.DataOutputView;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class WindowTuple implements Serializable {

    // Storm Component
    private int sourceTaskId;
    private String sourceComponentId;

    // Storm Tuple
    private Map<String, Integer> fieldIndex;
    private List<TupleObject<?>> values;

    private WindowTuple(
            int sourceTaskId,
            String sourceComponentId,
            Map<String, Integer> fieldIndex,
            List<TupleObject<?>> values) {
        this.sourceTaskId = sourceTaskId;
        this.sourceComponentId = sourceComponentId;
        this.fieldIndex = fieldIndex;
        this.values = values;
    }

    public String getSourceComponent() {
        return sourceComponentId;
    }

    public int getSourceTask() {
        return sourceTaskId;
    }

    public int size() {
        return fieldIndex.size();
    }

    public boolean contains(String field) {
        return fieldIndex.containsKey(field);
    }


    public int fieldIndex(String field) {
        return fieldIndex.getOrDefault(field, -1);
    }

    public String getString(int i) {
        TupleString tuple = (TupleString) values.get(i);
        return tuple.getValue();
    }

    public Integer getInteger(int i) {
        TupleInt tuple = (TupleInt) values.get(i);
        return tuple.getValue();
    }

    public Long getLong(int i) {
        TupleLong tuple = (TupleLong) values.get(i);
        return tuple.getValue();
    }

    public Boolean getBoolean(int i) {
        TupleBoolean tuple = (TupleBoolean) values.get(i);
        return tuple.getValue();
    }

    public Short getShort(int i) {
        TupleShort tuple = (TupleShort) values.get(i);
        return tuple.getValue();
    }

    public Byte getByte(int i) {
        TupleByte tuple = (TupleByte) values.get(i);
        return tuple.getValue();
    }

    public Double getDouble(int i) {
        TupleDouble tuple = (TupleDouble) values.get(i);
        return tuple.getValue();
    }

    public Float getFloat(int i) {
        TupleFloat tuple = (TupleFloat) values.get(i);
        return tuple.getValue();
    }

    public String getStringByField(String field) {
        return getString(fieldIndex.get(field));
    }

    public Integer getIntegerByField(String field) {
        return getInteger(fieldIndex.get(field));
    }

    public Long getLongByField(String field) {
        return getLong(fieldIndex.get(field));
    }

    public Boolean getBooleanByField(String field) {
        return getBoolean(fieldIndex.get(field));
    }

    public Short getShortByField(String field) {
        return getShort(fieldIndex.get(field));
    }

    public Byte getByteByField(String field) {
        return getByte(fieldIndex.get(field));
    }

    public Double getDoubleByField(String field) {
        return getDouble(fieldIndex.get(field));
    }

    public Float getFloatByField(String field) {
        return getFloat(fieldIndex.get(field));
    }


    public List<TupleObject<?>> getValues() {
        return values;
    }

    public static WindowTuple apply(Tuple input) {
        Map<String, Integer> fieldIndex = Maps.newHashMap();
        Fields fields = input.getFields();
        Iterator<String> it = fields.iterator();
        while (it.hasNext()) {
            String field = it.next();
            fieldIndex.put(field, fields.fieldIndex(field));
        }

        List<TupleObject<?>> values = Lists.newArrayListWithCapacity(input.getValues().size());
        for (Object value : input.getValues()) {
            if (value instanceof String) {
                values.add(TupleString.of((String) value));
            } else if (value.getClass() == Integer.class || value.getClass() == int.class) {
                values.add(TupleInt.of((Integer) value));
            } else if (value.getClass() == Short.class || value.getClass() == short.class) {
                values.add(TupleShort.of((Short) value));
            } else if (value.getClass() == Float.class || value.getClass() == float.class) {
                values.add(TupleFloat.of((Float) value));
            } else if (value.getClass() == Double.class || value.getClass() == double.class) {
                values.add(TupleDouble.of((Double) value));
            } else if (value.getClass() == Long.class || value.getClass() == long.class) {
                values.add(TupleLong.of((Long) value));
            } else if (value.getClass() == Boolean.class || value.getClass() == boolean.class) {
                values.add(TupleBoolean.of((Boolean) value));
            } else if (value.getClass() == Byte.class || value.getClass() == byte.class) {
                values.add(TupleByte.of((Byte) value));
            } else {
                throw new IllegalArgumentException("Unsupported tuple value type: " + value.getClass());
            }
        }

        return new WindowTuple(
                input.getSourceTask(),
                input.getSourceComponent(),
                fieldIndex,
                values);

    }

    public static final class WindowTupleSerializer extends TypeSerializerSingleton<WindowTuple> {

        public static final WindowTupleSerializer INSTANCE = new WindowTupleSerializer();

        private MapSerializer<String, Integer> _serializer = new MapSerializer<>(
                StringSerializer.INSTANCE,
                IntSerializer.INSTANCE);

        private WindowTupleSerializer() {}

        @Override
        public WindowTuple createInstance() {
            throw new UnsupportedOperationException("Unsupported create WindowTuple instance");
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public boolean isImmutableType() {
            return false;
        }

        @Override
        public void serialize(WindowTuple record, DataOutputView target) throws IOException {
            // sourceTaskId
            IntSerializer.INSTANCE.serialize(record.getSourceTask(), target);

            // sourceComponent
            StringSerializer.INSTANCE.serialize(record.getSourceComponent(), target);

            // field index
            _serializer.serialize(record.fieldIndex, target);

            // field value
            List<TupleObject<?>> tuples = record.getValues();
            if (tuples == null) {
                target.writeBoolean(true);
                return;
            }
            target.writeBoolean(false);
            target.writeInt(record.getValues().size());
            for (TupleObject<?> tuple : record.getValues()) {
                TupleObject.TupleType type = tuple.tupleType();
                target.writeInt(type.ordinal());
                switch (type) {
                    case TUPLE_INT:
                        IntSerializer.INSTANCE.serialize((Integer) tuple.getValue(), target);
                        break;
                    case TUPLE_FLOAT:
                        FloatSerializer.INSTANCE.serialize((Float) tuple.getValue(), target);
                        break;
                    case TUPLE_DOUBLE:
                        DoubleSerializer.INSTANCE.serialize((Double) tuple.getValue(), target);
                        break;
                    case TUPLE_BYTE:
                        ByteSerializer.INSTANCE.serialize((Byte) tuple.getValue(), target);
                        break;
                    case TUPLE_BOOLEAN:
                        BooleanSerializer.INSTANCE.serialize((Boolean) tuple.getValue(), target);
                        break;
                    case TUPLE_LONG:
                        LongSerializer.INSTANCE.serialize((Long) tuple.getValue(), target);
                        break;
                    case TUPLE_SHORT:
                        ShortSerializer.INSTANCE.serialize((Short) tuple.getValue(), target);
                        break;
                    case TUPLE_STRING:
                        StringSerializer.INSTANCE.serialize((String) tuple.getValue(), target);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported tuple type: " + type);
                }
            }
        }

        @Override
        public WindowTuple deserialize(DataInputView source) throws IOException {
            // sourceTaskId
            int sourceTaskId = IntSerializer.INSTANCE.deserialize(source);

            // source component
            String sourceComponentId = StringSerializer.INSTANCE.deserialize(source);

            // field index
            Map<String, Integer> fieldIndex = _serializer.deserialize(source);

            // field value
            boolean isNull = source.readBoolean();
            List<TupleObject<?>> values;
            if (isNull) {
                values = emptyList();
            } else {
                int size = source.readInt();
                values = Lists.newArrayListWithCapacity(size);
                for (int i = 0; i < size; ++i) {
                    TupleObject.TupleType type = TupleObject.TupleType.values()[source.readInt()];
                    switch (type) {
                        case TUPLE_INT:
                            values.add(TupleInt.of(IntSerializer.INSTANCE.deserialize(source)));
                            break;
                        case TUPLE_FLOAT:
                            values.add(TupleFloat.of(FloatSerializer.INSTANCE.deserialize(source)));
                            break;
                        case TUPLE_DOUBLE:
                            values.add(TupleDouble.of(DoubleSerializer.INSTANCE.deserialize(source)));
                            break;
                        case TUPLE_BYTE:
                            values.add(TupleByte.of(ByteSerializer.INSTANCE.deserialize(source)));
                            break;
                        case TUPLE_BOOLEAN:
                            values.add(TupleBoolean.of(BooleanSerializer.INSTANCE.deserialize(source)));
                            break;
                        case TUPLE_LONG:
                            values.add(TupleLong.of(LongSerializer.INSTANCE.deserialize(source)));
                            break;
                        case TUPLE_SHORT:
                            values.add(TupleShort.of(ShortSerializer.INSTANCE.deserialize(source)));
                            break;
                        case TUPLE_STRING:
                            values.add(TupleString.of(StringSerializer.INSTANCE.deserialize(source)));
                            break;
                        default:
                            throw new IllegalArgumentException("Unsupported tuple type: " + type);
                    }
                }
            }

            return new WindowTuple(
                    sourceTaskId,
                    sourceComponentId,
                    fieldIndex,
                    values);
        }

        @Override
        public WindowTuple copy(WindowTuple from) {
            throw new UnsupportedOperationException("Unsupported copy WindowTuple");
        }

        @Override
        public boolean canEqual(Object obj) {
            return obj instanceof WindowTupleSerializer;
        }

    }
}
