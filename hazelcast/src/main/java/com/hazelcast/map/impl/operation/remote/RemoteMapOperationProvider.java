package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.DefaultMapOperationProvider;
import com.hazelcast.map.impl.operation.MapOperation;

public class RemoteMapOperationProvider extends DefaultMapOperationProvider {

    private final HazelcastInstance remoteClusterClient;
    private final MapServiceContext mapServiceContext;

    public RemoteMapOperationProvider(MapServiceContext mapServiceContext, HazelcastInstance remoteClusterClient) {
        this.remoteClusterClient = remoteClusterClient;
        this.mapServiceContext = mapServiceContext;
    }

    @Override
    public MapOperation createGetOperation(String name, Data dataKey) {
        return inject(new RemoteMapGetOperation(name, dataKey, mapServiceContext.toObject(dataKey)));
    }

    @Override
    public MapOperation createPutOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        return inject(new RemoteMapPutOperation(name, key, value));
    }

    MapOperation inject(MapOperation mapOperation) {
        if (! (mapOperation instanceof RemoteMapOperation)) {
            throw new IllegalArgumentException("Map operation must be an instance of RemoteMapOperation");
        }
        mapOperation.setNodeEngine(mapServiceContext.getNodeEngine());
        mapOperation.setServiceName(MapService.SERVICE_NAME);
        mapOperation.setMapService(mapServiceContext.getService());
        ((RemoteMapOperation) mapOperation).setRemoteClusterClient(remoteClusterClient);
        // todo can cluster ID change over time?
        ((RemoteMapOperation) mapOperation)
                .setClusterId(mapServiceContext.getNodeEngine().getClusterService().getClusterId().toString() + ".");
        return mapOperation;
    }
}
