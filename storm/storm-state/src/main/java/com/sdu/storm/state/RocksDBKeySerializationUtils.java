package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.ByteArrayOutputStreamWithPos;
import com.sdu.storm.utils.DataOutputView;

import java.io.IOException;

public class RocksDBKeySerializationUtils {


    public static boolean isAmbiguousKeyPossible(TypeSerializer keySerializer, TypeSerializer namespaceSerializer) {
        return (keySerializer.getLength() < 0) && (namespaceSerializer.getLength() < 0);
    }

    public static void writeKeyGroup(
            int keyGroup,
            int keyGroupPrefixBytes,
            DataOutputView keySerializationDateDataOutputView) throws IOException {
        for (int i = keyGroupPrefixBytes; --i >= 0; ) {
            keySerializationDateDataOutputView.writeByte(keyGroup >>> (i << 3));
        }
    }

    public static <K> void writeKey(
            K key,
            TypeSerializer<K> keySerializer,
            ByteArrayOutputStreamWithPos keySerializationStream,
            DataOutputView keySerializationDataOutputView,
            boolean ambiguousKeyPossible) throws IOException {
        //write key
        int beforeWrite = keySerializationStream.getPosition();
        keySerializer.serialize(key, keySerializationDataOutputView);

        if (ambiguousKeyPossible) {
            //write size of key
            writeLengthFrom(beforeWrite, keySerializationStream, keySerializationDataOutputView);
        }
    }

    public static <N> void writeNameSpace(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            ByteArrayOutputStreamWithPos keySerializationStream,
            DataOutputView keySerializationDataOutputView,
            boolean ambiguousKeyPossible) throws IOException {

        int beforeWrite = keySerializationStream.getPosition();
        namespaceSerializer.serialize(namespace, keySerializationDataOutputView);

        if (ambiguousKeyPossible) {
            //write length of namespace
            writeLengthFrom(beforeWrite, keySerializationStream, keySerializationDataOutputView);
        }
    }

    private static void writeLengthFrom(
            int fromPosition,
            ByteArrayOutputStreamWithPos keySerializationStream,
            DataOutputView keySerializationDateDataOutputView) throws IOException {
        int length = keySerializationStream.getPosition() - fromPosition;
        writeVariableIntBytes(length, keySerializationDateDataOutputView);
    }

    private static void writeVariableIntBytes(
            int value,
            DataOutputView keySerializationDateDataOutputView)
            throws IOException {
        do {
            keySerializationDateDataOutputView.writeByte(value);
            value >>>= 8;
        } while (value != 0);
    }
}
