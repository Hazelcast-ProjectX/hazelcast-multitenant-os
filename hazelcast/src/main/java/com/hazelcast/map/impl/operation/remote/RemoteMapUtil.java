package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.map.impl.MapServiceContext;

public class RemoteMapUtil {

    private RemoteMapUtil() {
    }

    public static String clusterId(MapServiceContext mapServiceContext) {
        return mapServiceContext.getNodeEngine().getConfig().getClusterName() + ".";
    }
}
