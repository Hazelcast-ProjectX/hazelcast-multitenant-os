package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.core.HazelcastInstance;

public interface RemoteMapOperation {

    void setRemoteClusterClient(HazelcastInstance client);
}
