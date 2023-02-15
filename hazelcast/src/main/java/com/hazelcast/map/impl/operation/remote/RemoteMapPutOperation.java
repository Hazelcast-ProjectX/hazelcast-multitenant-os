package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.operation.PutOperation;

public class RemoteMapPutOperation extends PutOperation implements RemoteMapOperation {

    HazelcastInstance remoteClusterClient;

    public RemoteMapPutOperation(String name, Data dataKey, Data dataValue) {
        super(name, dataKey, dataValue);
    }

    @Override
    public void setRemoteClusterClient(HazelcastInstance client) {
        this.remoteClusterClient = client;
    }

    @Override
    protected void runInternal() {
        Object key = mapServiceContext.toObject(dataKey);
        Object value = mapServiceContext.toObject(dataValue);
        oldValue = remoteClusterClient.getMap(name).put(key, value);
    }
}
