package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;

public class RemoteMapUtil {

    private RemoteMapUtil() {
    }

    public static String clusterId(MapServiceContext mapServiceContext) {
        return mapServiceContext.getNodeEngine().getConfig().getClusterName() + ".";
    }

    public static void extractCompactSchema(MapContainer mapContainer, MapServiceContext mapServiceContext,
                                            Object key, Object value) {
        if (mapContainer.schemaResolved.compareAndSet(false, true)) {
            InternalSerializationService iss =
                    (InternalSerializationService) mapServiceContext.getNodeEngine().getSerializationService();
            try {
                if (iss.isCompactSerializable(key)) {
                    iss.extractSchemaFromObject(key);
                }
                if (iss.isCompactSerializable(value)) {
                    iss.extractSchemaFromObject(value);
                }
            } catch (IllegalArgumentException e) {
                // ignore -- shouldn't be thrown because we already check
            }
        }

    }
}
