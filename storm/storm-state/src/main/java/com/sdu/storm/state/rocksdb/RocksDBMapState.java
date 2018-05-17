package com.sdu.storm.state.rocksdb;

import com.sdu.storm.state.InternalMapState;
import com.sdu.storm.state.MapState;
import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.state.typeutils.base.MapSerializer;
import com.sdu.storm.utils.ByteArrayInputStreamWithPos;
import com.sdu.storm.utils.DataInputViewStreamWrapper;
import com.sdu.storm.utils.Preconditions;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the map state.
 * @param <UV> The type of the values in the map state.
 * */
public class RocksDBMapState<K, N, UK, UV>
        extends AbstractRocksDBState<K, N, Map<UK, UV>, MapState<UK, UV>>
        implements InternalMapState<K, N, UK, UV>{

    private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBMapState.class);

    /** Serializer for the keys and values. */
    private final TypeSerializer<UK> userKeySerializer;
    private final TypeSerializer<UV> userValueSerializer;

    // Map结构在RocksDB存储形式为: KEY-VALUE
    // RocksDB读取数据KEY构成: KeyGroup + KEY + Namespace + UK
    //                      |                        |
    //                      +------------------------+
    //                             userKeyOffset
    // NOTE:
    //  userKeyOffset必须是固定长度, 否则计算异常
    private int userKeyOffset;

    public RocksDBMapState(ColumnFamilyHandle columnFamily,
                           TypeSerializer<N> namespaceSerializer,
                           TypeSerializer<Map<UK, UV>> valueSerializer,
                           Map<UK, UV> defaultValue,
                           RocksDBKeyedStateBackend<K> backend) {
        super(columnFamily, namespaceSerializer, valueSerializer, defaultValue, backend);

        Preconditions.checkState(valueSerializer instanceof MapSerializer, "Unexpected serializer type.");

        this.userKeySerializer = ((MapSerializer<UK, UV>) valueSerializer).getKeySerializer();
        this.userValueSerializer = ((MapSerializer<UK, UV>) valueSerializer).getValueSerializer();

    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return backend.getKeySerializer();
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    @Override
    public TypeSerializer<Map<UK, UV>> getValueSerializer() {
        return valueSerializer;
    }

    @Override
    public UV get(UK userKey) throws Exception {
        byte[] rawKeyBytes = serializeUserKeyWithCurrentKeyAndNamespace(userKey);
        byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

        return rawValueBytes == null ? null : deserializeUserValue(rawValueBytes, userValueSerializer);
    }

    @Override
    public void put(UK userKey, UV userValue) throws Exception {
        byte[] rawKeyBytes = serializeUserKeyWithCurrentKeyAndNamespace(userKey);
        byte[] rwaUserValue = serializeUserValue(userValue, userValueSerializer);

        backend.db.put(columnFamily, writeOptions, rawKeyBytes, rwaUserValue);
    }

    @Override
    public void putAll(Map<UK, UV> userMap) throws Exception {
        if (userMap == null) {
            return;
        }

        for (Map.Entry<UK, UV> entry : userMap.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void remove(UK userKey) throws Exception {
        byte[] rawKeyBytes = serializeUserKeyWithCurrentKeyAndNamespace(userKey);

        backend.db.delete(columnFamily, writeOptions, rawKeyBytes);
    }

    @Override
    public boolean contains(UK userKey) throws Exception {
        byte[] rawKeyBytes = serializeUserKeyWithCurrentKeyAndNamespace(userKey);
        byte[] rawValueBytes = backend.db.get(columnFamily, rawKeyBytes);

        return (rawValueBytes != null);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        final Iterator<Map.Entry<UK, UV>> iterator = iterator();

        // Return null to make the behavior consistent with other states.
        if (!iterator.hasNext()) {
            return null;
        } else {
            return () -> iterator;
        }
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        final byte[] prefixBytes = serializeCurrentKeyAndNamespace();

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
    public Iterable<UV> values() throws Exception {
        final byte[] prefixBytes = serializeCurrentKeyAndNamespace();

        return () -> new RocksDBMapIterator<UV>(
                            backend.db,
                            prefixBytes,
                            userKeySerializer,
                            userValueSerializer) {
            @Override
            public UV next() {
                RocksDBMapEntry entry = nextEntry();
                return (entry == null ? null : entry.getValue());
            }
        };
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        final byte[] prefixBytes = serializeCurrentKeyAndNamespace();

        return new RocksDBMapIterator<Map.Entry<UK, UV>>(backend.db, prefixBytes, userKeySerializer, userValueSerializer) {
            @Override
            public Map.Entry<UK, UV> next() {
                return nextEntry();
            }
        };
    }

    @Override
    public void clear() {
        try {
            Iterator<Map.Entry<UK, UV>> iterator = iterator();

            while (iterator.hasNext()) {
                iterator.next();
                iterator.remove();
            }
        } catch (Exception e) {
            LOGGER.warn("Error while cleaning the state.", e);
        }
    }


    private byte[] serializeCurrentKeyAndNamespace() throws IOException {
        writeCurrentKeyWithGroupAndNamespace();
        userKeyOffset = keySerializationStream.getPosition();

        return keySerializationStream.toByteArray();
    }

    private byte[] serializeUserKeyWithCurrentKeyAndNamespace(UK userKey) throws IOException {
        // Map在RocksDB存储采用KEY-VALUE
        // 存储KEY构成: namespace + keyGroup + key + UK
        serializeCurrentKeyAndNamespace();
        userKeySerializer.serialize(userKey, keySerializationDataOutputView);

        return keySerializationStream.toByteArray();
    }

    private UK deserializeUserKey(byte[] rawKeyBytes, TypeSerializer<UK> keySerializer) throws IOException {
        ByteArrayInputStreamWithPos bais = new ByteArrayInputStreamWithPos(rawKeyBytes);
        DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

        // 跳过Namespace + KeyGroup + KEY长度
        in.skipBytes(userKeyOffset);

        return keySerializer.deserialize(in);
    }

    private byte[] serializeUserValue(UV value, TypeSerializer<UV> valueSerializer) throws IOException {
        keySerializationStream.reset();

        if (value == null) {
            keySerializationDataOutputView.writeBoolean(true);
        } else {
            keySerializationDataOutputView.writeBoolean(false);
            valueSerializer.serialize(value, keySerializationDataOutputView);
        }
        return keySerializationStream.toByteArray();
    }

    private UV deserializeUserValue(byte[] rawValueBytes, TypeSerializer<UV> valueSerializer) throws IOException {
        ByteArrayInputStreamWithPos bais = new ByteArrayInputStreamWithPos(rawValueBytes);
        DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);

        boolean isNull = in.readBoolean();

        return isNull ? null : valueSerializer.deserialize(in);
    }

    private abstract class RocksDBMapIterator<T> implements Iterator<T> {
        // 缓存从RocksDB读取的数据, 减少IO操作
        private static final int CACHE_SIZE_LIMIT = 128;

        private final RocksDB db;

        /**
         * The prefix bytes of the key being accessed. All entries under the same key
         * have the same prefix, hence we can stop iterating once coming across an
         * entry with a different prefix.
         */
        private final byte[] keyPrefixBytes;

        /**
         * True if all entries have been accessed or the iterator has come across an
         * entry with a different prefix.
         */
        private boolean expired = false;

        /** A in-memory cache for the entries in the rocksdb. */
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
                    throw new IllegalStateException();
                }

                return null;
            }

            RocksDBMapEntry entry = cacheEntries.get(cacheIndex);
            cacheIndex++;

            return entry;
        }

        /** RocksDB读取存储KEY前缀为{@link #keyPrefixBytes} */
        private void loadCache() {
            // 当cacheIndex = cacheEntries.size, 则需要从RocksDB读取数据
            if (cacheIndex > cacheEntries.size()) {
                throw new IllegalStateException();
            }

            // Load cache entries only when the cache is empty and there still exist unread entries
            if (cacheIndex < cacheEntries.size() || expired) {
                return;
            }

            // use try-with-resources to ensure RocksIterator can be release even some runtime exception
            // occurred in the below code block.
            try (RocksIterator iterator = db.newIterator(columnFamily)) {

				/*
				 * The iteration starts from the prefix bytes at the first loading. The cache then is
				 * reloaded when the next entry to return is the last one in the cache. At that time,
				 * we will start the iterating from the last returned entry.
 				 */
                RocksDBMapEntry lastEntry = cacheEntries.size() == 0 ? null : cacheEntries.get(cacheEntries.size() - 1);
                byte[] startBytes = (lastEntry == null ? keyPrefixBytes : lastEntry.rawKeyBytes);

                cacheEntries.clear();
                cacheIndex = 0;

                iterator.seek(startBytes);

				/*
				 * If the last returned entry is not deleted, it will be the first entry in the
				 * iterating. Skip it to avoid redundant access in such cases.
				 */
                if (lastEntry != null && !lastEntry.deleted) {
                    iterator.next();
                }

                while (true) {
                    if (!iterator.isValid() || !underSameKey(iterator.key())) {
                        expired = true;
                        break;
                    }

                    if (cacheEntries.size() >= CACHE_SIZE_LIMIT) {
                        break;
                    }

                    RocksDBMapEntry entry = new RocksDBMapEntry(
                            db,
                            iterator.key(),
                            iterator.value(),
                            keySerializer,
                            valueSerializer);

                    cacheEntries.add(entry);

                    iterator.next();
                }
            }
        }

        // rawKeyBytes的构成: KeyGroup + KEY + Namespace
        // 需要比较: KEY + Namespace这部分是否是当前需要的数据
        private boolean underSameKey(byte[] rawKeyBytes) {
            if (rawKeyBytes.length < keyPrefixBytes.length) {
                return false;
            }

            for (int i = keyPrefixBytes.length; --i >= backend.getKeyGroupPrefixBytes(); ) {
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
                    userValue = deserializeUserValue(rawValueBytes, userValueSerializer);
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
                rawValueBytes = serializeUserValue(value, userValueSerializer);

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
