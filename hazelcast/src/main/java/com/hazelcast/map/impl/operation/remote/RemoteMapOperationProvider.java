package com.hazelcast.map.impl.operation.remote;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.iteration.IndexIterationPointer;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.DefaultMapOperationProvider;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.util.List;
import java.util.Set;
import java.util.UUID;

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

    @Override
    public MapOperation createPutIfAbsentOperation(String name, Data key, Data value, long ttl, long maxIdle) {
        return inject(new RemoteMapPutIfAbsentOperation(name, key, value, ttl, maxIdle));
    }

    @Override
    public MapOperation createPutAllOperation(String name, MapEntries mapEntries, boolean triggerMapLoader) {
        return inject(new RemoteMapPutAllOperation(name, mapEntries, triggerMapLoader));
    }

    @Override
    public MapOperation createEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        return inject(new RemoteMapEntryOperation(name, dataKey, entryProcessor));
    }


    MapOperation inject(MapOperation mapOperation) {
        if (!(mapOperation instanceof RemoteMapOperation)) {
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


    @Override
    public OperationFactory createMapSizeOperationFactory(String name) {
        new RuntimeException().printStackTrace();
        return super.createMapSizeOperationFactory(name);
    }

    @Override
    public MapOperation createTryPutOperation(String name, Data dataKey, Data value, long timeout) {
        new RuntimeException().printStackTrace();
        return super.createTryPutOperation(name, dataKey, value, timeout);
    }

    @Override
    public MapOperation createSetOperation(String name, Data dataKey, Data value, long ttl, long maxIdle) {
        new RuntimeException().printStackTrace();
        return super.createSetOperation(name, dataKey, value, ttl, maxIdle);
    }

    @Override
    public MapOperation createPutTransientOperation(String name, Data keyData, Data valueData, long ttl, long maxIdle) {
        new RuntimeException().printStackTrace();
        return super.createPutTransientOperation(name, keyData, valueData, ttl, maxIdle);
    }

    @Override
    public MapOperation createRemoveOperation(String name, Data key) {
        new RuntimeException().printStackTrace();
        return super.createRemoveOperation(name, key);
    }

    @Override
    public MapOperation createSetTtlOperation(String name, Data key, long ttl) {
        new RuntimeException().printStackTrace();
        return super.createSetTtlOperation(name, key, ttl);
    }

    @Override
    public MapOperation createTryRemoveOperation(String name, Data dataKey, long timeout) {
        new RuntimeException().printStackTrace();
        return super.createTryRemoveOperation(name, dataKey, timeout);
    }

    @Override
    public MapOperation createReplaceOperation(String name, Data dataKey, Data value) {
        new RuntimeException().printStackTrace();
        return super.createReplaceOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createRemoveIfSameOperation(String name, Data dataKey, Data value) {
        new RuntimeException().printStackTrace();
        return super.createRemoveIfSameOperation(name, dataKey, value);
    }

    @Override
    public MapOperation createReplaceIfSameOperation(String name, Data dataKey, Data expect, Data update) {
        new RuntimeException().printStackTrace();
        return super.createReplaceIfSameOperation(name, dataKey, expect, update);
    }

    @Override
    public MapOperation createDeleteOperation(String name, Data key, boolean disableWanReplicationEvent) {
        new RuntimeException().printStackTrace();
        return super.createDeleteOperation(name, key, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createClearOperation(String name) {
        new RuntimeException().printStackTrace();
        return super.createClearOperation(name);
    }

    @Override
    public MapOperation createEvictOperation(String name, Data dataKey, boolean asyncBackup) {
        new RuntimeException().printStackTrace();
        return super.createEvictOperation(name, dataKey, asyncBackup);
    }

    @Override
    public MapOperation createEvictAllOperation(String name) {
        new RuntimeException().printStackTrace();
        return super.createEvictAllOperation(name);
    }

    @Override
    public MapOperation createContainsKeyOperation(String name, Data dataKey) {
        new RuntimeException().printStackTrace();
        return super.createContainsKeyOperation(name, dataKey);
    }

    @Override
    public OperationFactory createContainsValueOperationFactory(String name, Data testValue) {
        new RuntimeException().printStackTrace();
        return super.createContainsValueOperationFactory(name, testValue);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(String name, List<Data> keys) {
        new RuntimeException().printStackTrace();
        return super.createGetAllOperationFactory(name, keys);
    }

    @Override
    public OperationFactory createEvictAllOperationFactory(String name) {
        new RuntimeException().printStackTrace();
        return super.createEvictAllOperationFactory(name);
    }

    @Override
    public OperationFactory createClearOperationFactory(String name) {
        new RuntimeException().printStackTrace();
        return super.createClearOperationFactory(name);
    }

    @Override
    public OperationFactory createMapFlushOperationFactory(String name) {
        new RuntimeException().printStackTrace();
        return super.createMapFlushOperationFactory(name);
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(String name, List<Data> keys, boolean replaceExistingValues) {
        new RuntimeException().printStackTrace();
        return super.createLoadAllOperationFactory(name, keys, replaceExistingValues);
    }

    @Override
    public MapOperation createGetEntryViewOperation(String name, Data dataKey) {
        new RuntimeException().printStackTrace();
        return super.createGetEntryViewOperation(name, dataKey);
    }

    @Override
    public OperationFactory createPartitionWideEntryOperationFactory(String name, EntryProcessor entryProcessor) {
        new RuntimeException().printStackTrace();
        return super.createPartitionWideEntryOperationFactory(name, entryProcessor);
    }

    @Override
    public MapOperation createTxnDeleteOperation(String name, Data dataKey, long version) {
        new RuntimeException().printStackTrace();
        return super.createTxnDeleteOperation(name, dataKey, version);
    }

    @Override
    public MapOperation createTxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl,
                                                     UUID ownerUuid, boolean shouldLoad, boolean blockReads) {
        new RuntimeException().printStackTrace();
        return super.createTxnLockAndGetOperation(name, dataKey, timeout, ttl, ownerUuid, shouldLoad, blockReads);
    }

    @Override
    public MapOperation createTxnSetOperation(String name, Data dataKey, Data value, long version, long ttl) {
        new RuntimeException().printStackTrace();
        return super.createTxnSetOperation(name, dataKey, value, version, ttl);
    }

    @Override
    public MapOperation createMergeOperation(String name, MapMergeTypes<Object, Object> mergingValue, SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy, boolean disableWanReplicationEvent) {
        new RuntimeException().printStackTrace();
        return super.createMergeOperation(name, mergingValue, mergePolicy, disableWanReplicationEvent);
    }

    @Override
    public MapOperation createMapFlushOperation(String name) {
        new RuntimeException().printStackTrace();
        return super.createMapFlushOperation(name);
    }

    @Override
    public MapOperation createLoadMapOperation(String name, boolean replaceExistingValues) {
        new RuntimeException().printStackTrace();
        return super.createLoadMapOperation(name, replaceExistingValues);
    }

    @Override
    public OperationFactory createPartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor, Predicate predicate) {
        new RuntimeException().printStackTrace();
        return super.createPartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate);
    }

    @Override
    public OperationFactory createMultipleEntryOperationFactory(String name, Set<Data> keys, EntryProcessor entryProcessor) {
        new RuntimeException().printStackTrace();
        return super.createMultipleEntryOperationFactory(name, keys, entryProcessor);
    }

    @Override
    public Operation createQueryOperation(Query query) {
        new RuntimeException().printStackTrace();
        return super.createQueryOperation(query);
    }

    @Override
    public MapOperation createQueryPartitionOperation(Query query) {
        new RuntimeException().printStackTrace();
        return super.createQueryPartitionOperation(query);
    }

    @Override
    public MapOperation createLoadAllOperation(String name, List<Data> keys, boolean replaceExistingValues) {
        new RuntimeException().printStackTrace();
        return super.createLoadAllOperation(name, keys, replaceExistingValues);
    }

    @Override
    public OperationFactory createPutAllOperationFactory(String name, int[] partitions, MapEntries[] mapEntries,
                                                         boolean triggerMapLoader) {
        new RuntimeException().printStackTrace();
        return super.createPutAllOperationFactory(name, partitions, mapEntries, triggerMapLoader);
    }

    @Override
    public OperationFactory createMergeOperationFactory(String name, int[] partitions, List<MapMergeTypes<Object,
            Object>>[] mergingEntries, SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy) {
        new RuntimeException().printStackTrace();
        return super.createMergeOperationFactory(name, partitions, mergingEntries, mergePolicy);
    }

    @Override
    public MapOperation createPutFromLoadAllOperation(String name, List<Data> keyValueSequence,
                                                      boolean includesExpirationTime) {
        new RuntimeException().printStackTrace();
        return super.createPutFromLoadAllOperation(name, keyValueSequence, includesExpirationTime);
    }

    @Override
    public MapOperation createFetchKeysOperation(String name, IterationPointer[] pointers, int fetchSize) {
        new RuntimeException().printStackTrace();
        return super.createFetchKeysOperation(name, pointers, fetchSize);
    }

    @Override
    public MapOperation createFetchEntriesOperation(String name, IterationPointer[] pointers, int fetchSize) {
        new RuntimeException().printStackTrace();
        return super.createFetchEntriesOperation(name, pointers, fetchSize);
    }

    @Override
    public MapOperation createFetchIndexOperation(String mapName, String indexName, IndexIterationPointer[] pointers, PartitionIdSet partitionIdSet, int sizeLimit) {
        new RuntimeException().printStackTrace();
        return super.createFetchIndexOperation(mapName, indexName, pointers, partitionIdSet, sizeLimit);
    }

    @Override
    public MapOperation createFetchWithQueryOperation(String name, IterationPointer[] pointers, int fetchSize, Query query) {
        new RuntimeException().printStackTrace();
        return super.createFetchWithQueryOperation(name, pointers, fetchSize, query);
    }
}
