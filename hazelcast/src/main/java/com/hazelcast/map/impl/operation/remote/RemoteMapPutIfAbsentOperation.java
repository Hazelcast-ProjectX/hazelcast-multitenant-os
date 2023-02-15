package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.operation.PutIfAbsentOperation;

import java.util.concurrent.TimeUnit;

public class RemoteMapPutIfAbsentOperation extends PutIfAbsentOperation implements RemoteMapOperation {

    HazelcastInstance remoteClusterClient;
    String clusterId;

    long ttl;
    long maxIdle;

    public RemoteMapPutIfAbsentOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        super(name, key, value);
        this.ttl = ttl;
        this.maxIdle = maxIdle;
    }

    @Override
    protected void runInternal() {
        this.oldValue = remoteClusterClient.getMap(clusterId + name).putIfAbsent(
                dataKey, dataValue,
                ttl, TimeUnit.MILLISECONDS,
                maxIdle, TimeUnit.MILLISECONDS
        );
        this.successful = oldValue == null;
    }


    @Override
    public void setRemoteClusterClient(HazelcastInstance client) {
        this.remoteClusterClient = client;
    }

    @Override
    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    @Override
    public int getFactoryId() {
        throw new UnsupportedOperationException("local only");
    }
}
