package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.operation.PutAllOperation;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.clusterId;
import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.extractCompactSchema;

public class RemoteMapPutAllOperation extends PutAllOperation implements RemoteMapOperation {

    public RemoteMapPutAllOperation() {
        super();
    }

    public RemoteMapPutAllOperation(String name, MapEntries mapEntries, boolean triggerMapLoader) {
        super(name, mapEntries, triggerMapLoader);
    }

    @Override
    protected void runInternal() {
        // grossly inefficient
        Map<Object, Object> deserMapEntries = new HashMap<>();
        mapEntries.putAllToMap(mapServiceContext.getNodeEngine().getSerializationService(), deserMapEntries);
        Map.Entry sampleEntry = deserMapEntries.entrySet().iterator().next();
        assert sampleEntry != null;
        extractCompactSchema(mapContainer, mapServiceContext, sampleEntry.getKey(), sampleEntry.getValue());
        mapServiceContext.getRemoteClusterClient().getMap(clusterId(mapServiceContext) + name).putAll(deserMapEntries);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMOTE_MAP_PUT_ALL_OPERATION;
    }
}
