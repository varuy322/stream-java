package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.state.typeutils.TypeSerializerConfigSnapshot;
import com.sdu.storm.utils.Preconditions;

import java.util.Objects;

/**
 * @param <N> Type of namespace
 * @param <S> Type of state value
 *
 * @author hanhan.zhang
 * */
public class RegisteredKeyedBackendStateMetaInfo<N, S> {

    private final StateDescriptor.Type stateType;
    private final String name;
    private final TypeSerializer<N> namespaceSerializer;
    private final TypeSerializer<S> stateSerializer;

    public RegisteredKeyedBackendStateMetaInfo(StateDescriptor.Type stateType,
                                               String name,
                                               TypeSerializer<N> namespaceSerializer,
                                               TypeSerializer<S> stateSerializer) {
        this.stateType = Preconditions.checkNotNull(stateType);
        this.name = Preconditions.checkNotNull(name);
        this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
        this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
    }

    public StateDescriptor.Type getStateType() {
        return stateType;
    }

    public String getName() {
        return name;
    }

    public TypeSerializer<N> getNamespaceSerializer() {
        return namespaceSerializer;
    }

    public TypeSerializer<S> getStateSerializer() {
        return stateSerializer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RegisteredKeyedBackendStateMetaInfo<?, ?> that = (RegisteredKeyedBackendStateMetaInfo<?, ?>) o;

        if (!stateType.equals(that.stateType)) {
            return false;
        }

        if (!getName().equals(that.getName())) {
            return false;
        }

        return getStateSerializer().equals(that.getStateSerializer())
                && getNamespaceSerializer().equals(that.getNamespaceSerializer());
    }

    @Override
    public String toString() {
        return "RegisteredKeyedBackendStateMetaInfo{" +
                "stateType=" + stateType +
                ", name='" + name + '\'' +
                ", namespaceSerializer=" + namespaceSerializer +
                ", stateSerializer=" + stateSerializer +
                '}';
    }

    @Override
    public int hashCode() {
        int result = getName().hashCode();
        result = 31 * result + getStateType().hashCode();
        result = 31 * result + getNamespaceSerializer().hashCode();
        result = 31 * result + getStateSerializer().hashCode();
        return result;
    }

    public static class Snapshot<N, S> {

        private StateDescriptor.Type stateType;
        private String name;
        private TypeSerializer<N> namespaceSerializer;
        private TypeSerializer<S> stateSerializer;
        private TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot;
        private TypeSerializerConfigSnapshot stateSerializerConfigSnapshot;

        /** Empty constructor used when restoring the state meta info snapshot. */
        Snapshot() {}

        private Snapshot(
                StateDescriptor.Type stateType,
                String name,
                TypeSerializer<N> namespaceSerializer,
                TypeSerializer<S> stateSerializer,
                TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot,
                TypeSerializerConfigSnapshot stateSerializerConfigSnapshot) {

            this.stateType = Preconditions.checkNotNull(stateType);
            this.name = Preconditions.checkNotNull(name);
            this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
            this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
            this.namespaceSerializerConfigSnapshot = Preconditions.checkNotNull(namespaceSerializerConfigSnapshot);
            this.stateSerializerConfigSnapshot = Preconditions.checkNotNull(stateSerializerConfigSnapshot);
        }

        public StateDescriptor.Type getStateType() {
            return stateType;
        }

        void setStateType(StateDescriptor.Type stateType) {
            this.stateType = stateType;
        }

        public String getName() {
            return name;
        }

        void setName(String name) {
            this.name = name;
        }

        public TypeSerializer<N> getNamespaceSerializer() {
            return namespaceSerializer;
        }

        void setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
            this.namespaceSerializer = namespaceSerializer;
        }

        public TypeSerializer<S> getStateSerializer() {
            return stateSerializer;
        }

        void setStateSerializer(TypeSerializer<S> stateSerializer) {
            this.stateSerializer = stateSerializer;
        }

        public TypeSerializerConfigSnapshot getNamespaceSerializerConfigSnapshot() {
            return namespaceSerializerConfigSnapshot;
        }

        void setNamespaceSerializerConfigSnapshot(TypeSerializerConfigSnapshot namespaceSerializerConfigSnapshot) {
            this.namespaceSerializerConfigSnapshot = namespaceSerializerConfigSnapshot;
        }

        public TypeSerializerConfigSnapshot getStateSerializerConfigSnapshot() {
            return stateSerializerConfigSnapshot;
        }

        void setStateSerializerConfigSnapshot(TypeSerializerConfigSnapshot stateSerializerConfigSnapshot) {
            this.stateSerializerConfigSnapshot = stateSerializerConfigSnapshot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Snapshot<?, ?> that = (Snapshot<?, ?>) o;

            if (!stateType.equals(that.stateType)) {
                return false;
            }

            if (!getName().equals(that.getName())) {
                return false;
            }

            // need to check for nulls because serializer and config snapshots may be null on restore
            return Objects.equals(getStateSerializer(), that.getStateSerializer())
                    && Objects.equals(getNamespaceSerializer(), that.getNamespaceSerializer())
                    && Objects.equals(getNamespaceSerializerConfigSnapshot(), that.getNamespaceSerializerConfigSnapshot())
                    && Objects.equals(getStateSerializerConfigSnapshot(), that.getStateSerializerConfigSnapshot());
        }

        @Override
        public int hashCode() {
            // need to check for nulls because serializer and config snapshots may be null on restore
            int result = getName().hashCode();
            result = 31 * result + getStateType().hashCode();
            result = 31 * result + (getNamespaceSerializer() != null ? getNamespaceSerializer().hashCode() : 0);
            result = 31 * result + (getStateSerializer() != null ? getStateSerializer().hashCode() : 0);
            result = 31 * result + (getNamespaceSerializerConfigSnapshot() != null ? getNamespaceSerializerConfigSnapshot().hashCode() : 0);
            result = 31 * result + (getStateSerializerConfigSnapshot() != null ? getStateSerializerConfigSnapshot().hashCode() : 0);
            return result;
        }
    }
}
