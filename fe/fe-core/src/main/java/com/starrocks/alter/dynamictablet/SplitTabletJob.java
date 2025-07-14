// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.alter.dynamictablet;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.LockedObject;
import com.starrocks.lake.Utils;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.proto.TxnTypePB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class SplitTabletJob extends DynamicTabletJob {
    private static final Logger LOG = LogManager.getLogger(SplitTabletJob.class);

    public SplitTabletJob(long jobId, long dbId, long tableId,
            Map<Long, PhysicalPartitionContext> physicalPartitionContexts) {
        super(jobId, JobType.SPLIT_TABLET, dbId, tableId, physicalPartitionContexts);
    }

    // Begin and commit the split transaction
    @Override
    protected void runPendingJob() {
        updateNextVersion();

        registerDynamicTablets();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        watershedTxnId = globalStateMgr.getGlobalTransactionMgr().getTransactionIDGenerator()
                .getNextTransactionId();
        watershedGtid = globalStateMgr.getGtidGenerator().nextGtid();

        setJobState(JobState.PREPARING);
    }

    // Wait for previous versions published
    @Override
    protected void runPreparingJob() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }

                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();
                long commitVersion = physicalPartitionContext.getCommitVersion();
                long visibleVersion = physicalPartition.getVisibleVersion();
                if (commitVersion != visibleVersion + 1) {
                    Preconditions.checkState(visibleVersion < commitVersion,
                            "partition=" + physicalPartition.getId() + " visibleVersion="
                                    + visibleVersion + " commitVersion=" + commitVersion);
                    return;
                }
            }
        }

        setJobState(JobState.RUNNING);
    }

    // Publish the split transaction, replace old materialized indexes with new ones
    @Override
    protected void runRunningJob() {
        boolean allPartitionPublished = true;
        ThreadPoolExecutor publishThreadPool = GlobalStateMgr.getCurrentState().getPublishVersionDaemon()
                .getLakeTaskExecutor();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            boolean useAggregatePublish = olapTable.isFileBundling();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }

                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();
                int publishState = physicalPartitionContext.getPublishState();
                if (publishState > 0) { // Publish success
                    continue;
                }

                allPartitionPublished = false;

                if (publishState == 0) { // Publish in progress
                    continue;
                }

                // Publish async
                List<Tablet> tablets = new ArrayList<>();
                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    tablets.addAll(index.getTablets());
                }
                Future<Boolean> future = publishThreadPool.submit(() -> publishVersion(tablets,
                        physicalPartitionContext.getCommitVersion(), useAggregatePublish));
                physicalPartitionContext.setPublishFuture(future);
            }
        }

        if (!allPartitionPublished) {
            return;
        }

        replaceMaterializedIndexes();

        setJobState(JobState.CLEANING);
    }

    // Wait for all previous transactions finished, than delete old tablets
    @Override
    protected void runCleaningJob() {
        try {
            if (!GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().isPreviousTransactionsFinished(
                    watershedTxnId, dbId, List.of(tableId))) {
                return;
            }
        } catch (Exception e) {
            throw new DynamicTabletJobException("Failed to wait previous transactions finished", e);
        }

        removeMaterializedIndexes();

        unregisterDynamicTablets();

        setJobState(JobState.FINISHED);
    }

    // Clear new tablets
    @Override
    protected void runAbortingJob() {
        if (!canAbort()) {
            return;
        }

        unregisterDynamicTablets();

        setJobState(JobState.ABORTED);
    }

    // Can abort only when job state is PENDING
    @Override
    protected boolean canAbort() {
        return jobState == JobState.PENDING;
    }

    @Override
    public void replay() {
        switch (jobState) {
            case PENDING:
                addTabletsToInvertedIndex();
                break;
            case PREPARING:
                updateNextVersion();
                registerDynamicTablets();
                break;
            case RUNNING:
                break;
            case CLEANING:
                replaceMaterializedIndexes();
                break;
            case FINISHED:
                removeMaterializedIndexes();
                unregisterDynamicTablets();
                break;
            case ABORTING:
                break;
            case ABORTED:
                unregisterDynamicTablets();
                break;
            default:
                break;
        }
    }

    private void updateNextVersion() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();

                long commitVersion = physicalPartitionContext.getCommitVersion();
                if (commitVersion <= 0) { // Not in replay
                    commitVersion = physicalPartition.getNextVersion();
                    physicalPartitionContext.setCommitVersion(commitVersion);
                }
                physicalPartition.setNextVersion(commitVersion + 1);
            }
        }
    }

    private boolean publishVersion(List<Tablet> tablets, long commitVersion, boolean useAggregatePublish) {
        try {
            TxnInfoPB txnInfo = new TxnInfoPB();
            txnInfo.txnId = watershedTxnId;
            txnInfo.combinedTxnLog = false;
            txnInfo.commitTime = finishedTimeMs / 1000;
            txnInfo.txnType = TxnTypePB.TXN_SPLIT_TABLET;
            txnInfo.gtid = watershedGtid;

            // TODO: Add distribution columns
            Utils.publishVersion(tablets, txnInfo, commitVersion - 1, commitVersion, null,
                    WarehouseManager.DEFAULT_RESOURCE, null, useAggregatePublish);

            return true;
        } catch (Exception e) {
            LOG.warn("Failed to publish version for dynamic tablet job {}. ", this, e);
            return false;
        }
    }

    private void replaceMaterializedIndexes() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();

                long commitVersion = physicalPartitionContext.getCommitVersion();
                Preconditions.checkState(commitVersion == physicalPartition.getVisibleVersion() + 1,
                        "commit version: " + commitVersion + ", visible version: "
                                + physicalPartition.getVisibleVersion());
                physicalPartition.setVisibleVersion(commitVersion, finishedTimeMs);

                for (var indexEntry : physicalPartitionContext.getIndexContexts().entrySet()) {
                    long oldIndexId = indexEntry.getKey();
                    MaterializedIndex newIndex = indexEntry.getValue().getMaterializedIndex();
                    physicalPartition.replaceIndex(oldIndexId, newIndex);
                    olapTable.replaceIndex(oldIndexId, newIndex.getId());
                }
            }
        }
    }

    private void removeMaterializedIndexes() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();

                for (Long oldIndexId : physicalPartitionContext.getIndexContexts().keySet()) {
                    MaterializedIndex oldIndex = physicalPartition.deleteStaleIndex(oldIndexId);
                    for (Tablet tablet : oldIndex.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }
        }
    }

    private void addTabletsToInvertedIndex() {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }
                PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();

                TStorageMedium storageMedium = olapTable.getPartitionInfo()
                        .getDataProperty(physicalPartition.getParentId()).getStorageMedium();
                for (var indexEntry : physicalPartitionContext.getIndexContexts().entrySet()) {
                    MaterializedIndex newIndex = indexEntry.getValue().getMaterializedIndex();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexEntry.getKey());
                    TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartition.getId(), newIndex.getId(),
                            schemaHash, storageMedium, olapTable.isCloudNativeTableOrMaterializedView());

                    for (Tablet tablet : newIndex.getTablets()) {
                        invertedIndex.addTablet(tablet.getId(), tabletMeta);
                    }
                }
            }
        }
    }
}
