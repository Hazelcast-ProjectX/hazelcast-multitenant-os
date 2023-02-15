package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.operation.KeyBasedMapOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.clusterId;

public class RemoteMapEntryOperation extends KeyBasedMapOperation implements RemoteMapOperation {

    EntryProcessor entryProcessor;

    public RemoteMapEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);

        this.entryProcessor = entryProcessor;
    }

    @Override
    protected void runInternal() {
        mapServiceContext.getRemoteClusterClient()
                .getMap(clusterId(mapServiceContext) + name)
                // todo I think we need the deserialized key here, not Data
                .executeOnKey(dataKey, entryProcessor);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.REMOTE_MAP_ENTRY_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }
}
