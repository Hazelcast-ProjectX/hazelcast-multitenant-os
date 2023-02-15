package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.operation.GetOperation;

public class RemoteMapGetOperation extends GetOperation implements RemoteMapOperation {

    HazelcastInstance remoteClusterClient;
    Object key;

    public RemoteMapGetOperation(String name, Data dataKey, Object key) {
        super(name, dataKey);
        this.key = key;
    }

    @Override
    protected void runInternal() {
        Object value = remoteClusterClient.getMap(name).get(key);
        result = extractResult(mapServiceContext.toData(value));
    }

    @Override
    public void setRemoteClusterClient(HazelcastInstance client) {
        this.remoteClusterClient = client;
    }

    @Override
    public int getFactoryId() {
        throw new UnsupportedOperationException("local only");
    }
}
