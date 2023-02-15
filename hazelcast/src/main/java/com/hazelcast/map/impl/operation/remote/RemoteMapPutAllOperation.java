package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.operation.PutAllOperation;

import java.util.HashMap;
import java.util.Map;

public class RemoteMapPutAllOperation extends PutAllOperation implements RemoteMapOperation {

    HazelcastInstance remoteClusterClient;
    String clusterId;

    public RemoteMapPutAllOperation(String name, MapEntries mapEntries, boolean triggerMapLoader) {
        super(name, mapEntries, triggerMapLoader);
    }

    @Override
    protected void runInternal() {
        // grossly inefficient
        Map<Object, Object> deserMapEntries = new HashMap<>();
        mapEntries.putAllToMap(mapServiceContext.getNodeEngine().getSerializationService(), deserMapEntries);
        remoteClusterClient.getMap(clusterId + name).putAll(deserMapEntries);
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
