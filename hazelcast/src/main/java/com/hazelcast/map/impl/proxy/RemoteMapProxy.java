package com.hazelcast.map.impl.proxy;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.map.impl.operation.remote.RemoteMapUtil.clusterId;

public class RemoteMapProxy<K, V> extends MapProxyImpl<K, V> {

    private final HazelcastInstance remoteClusterClient;

    public RemoteMapProxy(String name, MapService mapService, NodeEngine nodeEngine, MapConfig mapConfig) {
        super(name, mapService, nodeEngine, mapConfig);
        remoteClusterClient = mapService.getMapServiceContext().getRemoteClusterClient();
        if (remoteClusterClient == null) {
            throw new IllegalArgumentException("RemoteMapProxy cannot be instantiated without remote connection"
                    + " to storage cluster.");
        }
    }

    @Override
    public InternalCompletableFuture<V> getAsync(@Nonnull K key) {
        return (InternalCompletableFuture<V>) remoteClusterClient.getMap(prefixedMapName()).getAsync(key).toCompletableFuture();
    }

    @Override
    public InternalCompletableFuture<Void> putAllAsync(@NotNull Map<? extends K, ? extends V> map) {
        return (InternalCompletableFuture<Void>) remoteClusterClient.getMap(prefixedMapName()).putAllAsync(map);
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        return (Collection<V>) remoteClusterClient.getMap(prefixedMapName()).values();
    }

    @Override
    public Collection<V> values(@Nonnull Predicate predicate) {
        return (Collection<V>) remoteClusterClient.getMap(prefixedMapName()).values(predicate);
    }

    @Override
    public Set<K> keySet() {
        return (Set<K>) remoteClusterClient.getMap(prefixedMapName()).keySet();
    }

    @Override
    public int size() {
        return remoteClusterClient.getMap(prefixedMapName()).size();
    }

    private String prefixedMapName() {
        return clusterId(mapServiceContext) + name;
    }
}
