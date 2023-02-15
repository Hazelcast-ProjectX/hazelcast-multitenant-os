package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.PutIfAbsentWithExpiryOperation;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.clusterId;

public class RemoteMapPutIfAbsentOperation extends PutIfAbsentWithExpiryOperation implements RemoteMapOperation {

    public RemoteMapPutIfAbsentOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        super(name, key, value, ttl, maxIdle);
    }

    @Override
    protected void runInternal() {
        this.oldValue = mapServiceContext.getRemoteClusterClient()
                .getMap(clusterId(mapServiceContext) + name).putIfAbsent(
                dataKey, dataValue,
                getTtl(), TimeUnit.MILLISECONDS,
                getMaxIdle(), TimeUnit.MILLISECONDS
        );
        this.successful = oldValue == null;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMOTE_MAP_PUT_IF_ABSENT_OPERATION;
    }
}
