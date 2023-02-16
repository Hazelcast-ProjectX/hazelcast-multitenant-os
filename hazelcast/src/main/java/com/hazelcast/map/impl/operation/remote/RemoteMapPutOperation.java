package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.PutOperation;

import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.clusterId;
import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.extractCompactSchema;

public class RemoteMapPutOperation extends PutOperation implements RemoteMapOperation {

    public RemoteMapPutOperation() {
    }

    public RemoteMapPutOperation(String name, Data dataKey, Data dataValue) {
        super(name, dataKey, dataValue);
    }

    @Override
    protected void runInternal() {
        Object key = mapServiceContext.toObject(dataKey);
        Object value = mapServiceContext.toObject(dataValue);
        extractCompactSchema(mapContainer, mapServiceContext, key, value);
        oldValue = mapServiceContext.getRemoteClusterClient().getMap(
                clusterId(mapServiceContext) + name).put(key, value);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMOTE_MAP_PUT_OPERATION;
    }
}
