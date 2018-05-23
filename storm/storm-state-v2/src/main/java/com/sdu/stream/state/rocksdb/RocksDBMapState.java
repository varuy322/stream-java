package com.sdu.stream.state.rocksdb;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.sdu.stream.state.InternalMapState;
import com.sdu.stream.state.seralizer.TypeSerializer;
import com.sdu.stream.state.utils.Preconditions;
import org.apache.logging.log4j.core.util.StringEncoder;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;

public class RocksDBMapState<N, K, UK, UV>
        extends AbstractRocksDBState<N, K, Map<UK, UV>>
        implements InternalMapState<N, K, UK, UV> {

    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;

    public RocksDBMapState(ColumnFamilyHandle columnFamily,
                           TypeSerializer<N> namespaceSerializer,
                           TypeSerializer<UK> userKeySerializer,
                           TypeSerializer<UV> userValueSerializer,
                           RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, backend);

        this.userKeySerializer = Preconditions.checkNotNull(userKeySerializer);
        this.userValueSerializer = Preconditions.checkNotNull(userValueSerializer);
    }

    @Override
    public UV get(N namespace, K userKey, UK field) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(64);
        ByteArrayDataOutput output = ByteStreams.newDataOutput(stream);
        byte[] rawKeyBytes = serializerMapKey(namespace, userKey, field, output);
        byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

//        System.out.println("Serializer key: " + Base64.getEncoder().encodeToString(rawKeyBytes));
//        System.out.println("Serializer value: " + Base64.getEncoder().encodeToString(rawValueBytes));

        return rawValueBytes == null ? null : deserializeUserValue(userValueSerializer, ByteStreams.newDataInput(rawValueBytes));
    }

    @Override
    public void put(N namespace, K userKey, UK field, UV value) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(64);
        ByteArrayDataOutput output = ByteStreams.newDataOutput(stream);
        byte[] rawKeyBytes = serializerMapKey(namespace, userKey, field, output);

        stream.reset();
        byte[] rwaUserValue = serializeUserValue(value, userValueSerializer, ByteStreams.newDataOutput(stream));

//        System.out.println("Serializer key: " + Base64.getEncoder().encodeToString(rawKeyBytes));
//        System.out.println("Serializer value: " + Base64.getEncoder().encodeToString(rwaUserValue));

        backend.db.put(columnFamily, writeOptions, rawKeyBytes, rwaUserValue);
    }

    @Override
    public void putAll(N namespace, K userKey, Map<UK, UV> values) throws Exception {
        if (values == null) {
            return;
        }

        for (Map.Entry<UK, UV> entry : values.entrySet()) {
            put(namespace, userKey, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(N namespace, K userKey, UK field) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(64);
        ByteArrayDataOutput output = ByteStreams.newDataOutput(stream);
        byte[] rawKeyBytes = serializerMapKey(namespace, userKey, field, output);

        backend.db.delete(columnFamily, writeOptions, rawKeyBytes);
    }

    @Override
    public boolean contains(N namespace, K userKey, UK field) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(64);
        ByteArrayDataOutput output = ByteStreams.newDataOutput(stream);
        byte[] rawKeyBytes = serializerMapKey(namespace, userKey, field, output);
        byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

        return rawValueBytes != null;
    }

    @Override
    public Iterable<UK> keys(N namespace, K userKey) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(64);
        ByteArrayDataOutput output = ByteStreams.newDataOutput(stream);
        final byte[] prefixBytes = serializerMapPrefixKey(namespace, userKey, output);

        return () -> new RocksDBMapIterator<UK>(
                backend.db,
                prefixBytes,
                userKeySerializer,
                userValueSerializer) {
            @Override
            public UK next() {
                RocksDBMapEntry entry = nextEntry();
                return (entry == null ? null : entry.getKey());
            }
        };
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeyTypeSerializer();
    }

    private byte[] serializerMapPrefixKey(N namespace, K key, ByteArrayDataOutput output) throws IOException {
        namespaceSerializer.serializer(namespace, output);
        getKeySerializer().serializer(key, output);
        return output.toByteArray();
    }

    private byte[] serializerMapKey(N namespace, K key, UK field, ByteArrayDataOutput output) throws IOException {
        namespaceSerializer.serializer(namespace, output);
        getKeySerializer().serializer(key, output);
        userKeySerializer.serializer(field, output);
        return output.toByteArray();
    }

    private UK deserializeUserKey(byte[] rawKeyBytes, TypeSerializer<UK> keySerializer) throws IOException {
        ByteArrayDataInput input = ByteStreams.newDataInput(rawKeyBytes);
        namespaceSerializer.deserialize(input);
        getKeySerializer().deserialize(input);

        return keySerializer.deserialize(input);
    }

    private byte[] serializeUserValue(UV value, TypeSerializer<UV> valueSerializer, ByteArrayDataOutput output) throws IOException {
        if (value == null) {
            output.writeBoolean(true);
        } else {
            output.writeBoolean(false);
            valueSerializer.serializer(value, output);
        }
        return output.toByteArray();
    }

    private UV deserializeUserValue(TypeSerializer<UV> valueSerializer, ByteArrayDataInput input) throws IOException {
        boolean isNull = input.readBoolean();
        return isNull ? null : valueSerializer.deserialize(input);
    }

    private abstract class RocksDBMapIterator<T> implements Iterator<T> {
        // 缓存从RocksDB读取的数据, 减少IO操作
        private static final int CACHE_SIZE_LIMIT = 128;

        private final RocksDB db;

        /** 存储前缀(Namespace + Key, 不包含field) */
        private final byte[] keyPrefixBytes;

        /** True: RocksDB存储数据遍历完成或遇到不同的前缀 */
        private boolean expired = false;

        /** 缓存存储在RocksDB中的数据 */
        private ArrayList<RocksDBMapEntry> cacheEntries = new ArrayList<>();
        private int cacheIndex = 0;

        private final TypeSerializer<UK> keySerializer;
        private final TypeSerializer<UV> valueSerializer;

        RocksDBMapIterator(
                final RocksDB db,
                final byte[] keyPrefixBytes,
                final TypeSerializer<UK> keySerializer,
                final TypeSerializer<UV> valueSerializer) {

            this.db = db;
            this.keyPrefixBytes = keyPrefixBytes;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        @Override
        public boolean hasNext() {
            loadCache();

            return (cacheIndex < cacheEntries.size());
        }

        @Override
        public void remove() {
            if (cacheIndex == 0 || cacheIndex > cacheEntries.size()) {
                throw new IllegalStateException("The remove operation must be called after a valid next operation.");
            }

            RocksDBMapEntry lastEntry = cacheEntries.get(cacheIndex - 1);
            lastEntry.remove();
        }

        final RocksDBMapEntry nextEntry() {
            loadCache();

            if (cacheIndex == cacheEntries.size()) {
                if (!expired) {
                    throw new IllegalStateException("");
                }

                return null;
            }

            RocksDBMapEntry entry = cacheEntries.get(cacheIndex);
            cacheIndex++;

            return entry;
        }

        private void loadCache() {
            // 当cacheIndex = cacheEntries.size, 则需要从RocksDB读取数据
            if (cacheIndex > cacheEntries.size()) {
                throw new IllegalStateException("RocksDB cache entry index cross border.");
            }

            // Load cache entries only when the cache is empty and there still exist unread entries
            if (cacheIndex < cacheEntries.size() || expired) {
                return;
            }

            // use try-with-resources to ensure RocksIterator can be release even some runtime exception
            // occurred in the below code block.
            try(RocksIterator iterator = db.newIterator(columnFamily))  {

				/*
				 * The iteration starts from the prefix bytes at the first loading. The cache then is
				 * reloaded when the next entry to return is the last one in the cache. At that time,
				 * we will start the iterating from the last returned entry.
 				 */
                RocksDBMapEntry lastEntry = cacheEntries.size() == 0 ? null : cacheEntries.get(cacheEntries.size() - 1);
                // lastEntry = null则是第一次从RocksDB读取存储数据
                byte[] startBytes = lastEntry == null ? keyPrefixBytes : lastEntry.rawKeyBytes;

                cacheEntries.clear();
                cacheIndex = 0;

                iterator.seek(startBytes);

				/*
				 * If the last returned entry is not deleted, it will be the first entry in the
				 * iterating. Skip it to avoid redundant access in such cases.
				 *
				 * 避免重复读取
				 */
                if (lastEntry != null && !lastEntry.deleted) {
                    iterator.next();
                }

                while (true) {
                    if (!iterator.isValid() || !underSameKey(iterator.key())) {
                        expired = true;
                        break;
                    }

                    if (cacheEntries.size() < CACHE_SIZE_LIMIT) {
                        RocksDBMapEntry entry = new RocksDBMapEntry(
                                db,
                                iterator.key(),
                                iterator.value(),
                                keySerializer,
                                valueSerializer);

                        cacheEntries.add(entry);

                        iterator.next();
                    } else {
                        break;
                    }
                }
            }
        }

        // rawKeyBytes的构成: Namespace + KEY + Field
        // 需要比较: Namespace + KEY这部分是否是当前需要的数据
        private boolean underSameKey(byte[] rawKeyBytes) {
            if (rawKeyBytes.length < keyPrefixBytes.length) {
                return false;
            }

            for (int i = keyPrefixBytes.length; --i >= 0; ) {
                if (rawKeyBytes[i] != keyPrefixBytes[i]) {
                    return false;
                }
            }

            return true;
        }
    }

    private class RocksDBMapEntry implements Map.Entry<UK, UV> {

        private final RocksDB db;

        /** The raw bytes of the key stored in RocksDB. Each user key is stored in RocksDB
         * with the format #KeyGroup#Key#Namespace#UserKey. */
        private final byte[] rawKeyBytes;

        /** The raw bytes of the value stored in RocksDB. */
        private byte[] rawValueBytes;

        /** True if the entry has been deleted. */
        private boolean deleted;

        /** The user key and value. The deserialization is performed lazily, i.e. the key
         * and the value is deserialized only when they are accessed. */
        private UK userKey;

        private UV userValue;

        private TypeSerializer<UK> keySerializer;

        private TypeSerializer<UV> valueSerializer;

        RocksDBMapEntry(
                @Nonnull final RocksDB db,
                @Nonnull final byte[] rawKeyBytes,
                @Nonnull final byte[] rawValueBytes,
                @Nonnull final TypeSerializer<UK> keySerializer,
                @Nonnull final TypeSerializer<UV> valueSerializer) {
            this.db = db;
            this.rawKeyBytes = rawKeyBytes;
            this.rawValueBytes = rawValueBytes;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.deleted = false;
        }

        @Override
        public UK getKey() {
            if (userKey == null) {
                try {
                    userKey = deserializeUserKey(rawKeyBytes, userKeySerializer);
                } catch (IOException e) {
                    throw new RuntimeException("Error while deserialize the user key.", e);
                }
            }
            return userKey;
        }

        @Override
        public UV getValue() {
            if (deleted) {
                return null;
            }
            if (userValue == null) {
                try {
                    userValue = deserializeUserValue(userValueSerializer, ByteStreams.newDataInput(rawValueBytes));
                } catch (IOException e) {
                    throw new RuntimeException("Error while deserialize the user value.", e);
                }
            }
            return userValue;
        }

        @Override
        public UV setValue(UV value) {
            if (deleted) {
                throw new IllegalStateException("The value has already been deleted.");
            }

            UV oldValue = getValue();

            try {
                userValue = value;
                rawValueBytes = serializeUserValue(value, valueSerializer, ByteStreams.newDataOutput(64));

                db.put(columnFamily, writeOptions, rawKeyBytes, rawValueBytes);
            } catch (IOException | RocksDBException e) {
                throw new RuntimeException("Error while putting data into RocksDB.", e);
            }
            return oldValue;
        }

        public void remove() {
            deleted = true;
            rawValueBytes = null;
            try {
                db.delete(columnFamily, writeOptions, rawKeyBytes);
            } catch (RocksDBException e) {
                throw new RuntimeException("Error while removing data from RocksDB.", e);
            }
        }
    }
}
