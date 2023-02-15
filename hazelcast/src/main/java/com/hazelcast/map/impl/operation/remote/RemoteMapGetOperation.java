package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.GetOperation;

import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.clusterId;

public class RemoteMapGetOperation extends GetOperation implements RemoteMapOperation {

    transient Object key;

    public RemoteMapGetOperation() {
    }

    public RemoteMapGetOperation(String name, Data dataKey, Object key) {
        super(name, dataKey);
        this.key = key;
    }

    @Override
    protected void runInternal() {
        key = mapServiceContext.toObject(dataKey);
        Object value = mapServiceContext.getRemoteClusterClient().getMap(clusterId(mapServiceContext) + name).get(key);
        result = extractResult(mapServiceContext.toData(value));
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMOTE_MAP_GET_OPERATION;
    }
}
