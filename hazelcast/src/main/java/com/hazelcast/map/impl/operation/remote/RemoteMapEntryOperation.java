package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.operation.KeyBasedMapOperation;

public class RemoteMapEntryOperation extends KeyBasedMapOperation implements RemoteMapOperation {

    HazelcastInstance remoteClusterClient;
    String clusterId;

    EntryProcessor entryProcessor;

    public RemoteMapEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);

        this.entryProcessor = entryProcessor;
    }

    @Override
    protected void runInternal() {
        remoteClusterClient.getMap(clusterId + name).executeOnKey(dataKey, entryProcessor);
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

    @Override
    public int getClassId() {
        throw new UnsupportedOperationException("local only");
    }
}
