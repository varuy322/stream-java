package com.sdu.storm.state;

import com.sdu.storm.state.typeutils.TypeSerializer;
import com.sdu.storm.utils.Preconditions;

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
}
