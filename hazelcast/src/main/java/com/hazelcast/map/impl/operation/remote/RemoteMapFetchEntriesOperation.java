package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapFetchEntriesCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.operation.MapFetchEntriesOperation;

import static com.hazelcast.internal.iteration.IterationPointer.decodePointers;
import static com.hazelcast.internal.iteration.IterationPointer.encodePointers;
import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.clusterId;

public class RemoteMapFetchEntriesOperation extends MapFetchEntriesOperation implements RemoteMapOperation {

    public RemoteMapFetchEntriesOperation() {
    }

    public RemoteMapFetchEntriesOperation(String name, IterationPointer[] pointers, int fetchSize) {
        super(name, pointers, fetchSize);
    }

    @Override
    protected void runInternal() {
        HazelcastClientInstanceImpl client =
                ((HazelcastClientProxy) getMapContainer().getMapServiceContext().getRemoteClusterClient()).client;
        String remoteMapName = clusterId(mapServiceContext) + name;
        ClientMessage request = MapFetchEntriesCodec.encodeRequest(remoteMapName, encodePointers(pointers), fetchSize);
        ClientInvocation clientInvocation = new ClientInvocation(
                client,
                request,
                remoteMapName,
                getPartitionId()
        );
        MapFetchEntriesCodec.ResponseParameters responseParameters =
                MapFetchEntriesCodec.decodeResponse(clientInvocation.invoke().joinInternal());
        IterationPointer[] pointers = decodePointers(responseParameters.iterationPointers);
        response = new MapEntriesWithCursor(responseParameters.entries, pointers);
    }

    @Override
    public int getClassId() {
        return super.getClassId();
    }
}
