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
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.LockedObject;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TStorageMedium;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class SplitTabletJob extends DynamicTabletJob {
    private static final Logger LOG = LogManager.getLogger(SplitTabletJob.class);

    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId;

    @SerializedName(value = "watershedGtid")
    protected long watershedGtid;

    public SplitTabletJob(long jobId, long dbId, long tableId, Map<Long, DynamicTabletContext> dynamicTabletContexts) {
        super(jobId, JobType.SPLIT_TABLET, dbId, tableId, dynamicTabletContexts);
    }

    // Begin and commit the split transaction
    @Override
    protected void runPendingJob() {
        updateNextVersion(false);

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        watershedTxnId = globalStateMgr.getGlobalTransactionMgr().getTransactionIDGenerator()
                .getNextTransactionId();
        watershedGtid = globalStateMgr.getGtidGenerator().nextGtid();

        setJobState(JobState.PREPARING);
    }

    // Wait for previous version published
    @Override
    protected void runPreparingJob() {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            for (Map.Entry<Long, DynamicTabletContext> physicalPartitionEntry : dynamicTabletContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }

                DynamicTabletContext dynamicTabletContext = physicalPartitionEntry.getValue();
                long commitVersion = dynamicTabletContext.getCommitVersion();
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

    // Publish the split transaction, than replace old tablets with new tablets
    @Override
    protected void runRunningJob() {
        boolean allPublishFinished = true;
        ThreadPoolExecutor publishThreadPool = GlobalStateMgr.getCurrentState().getPublishVersionDaemon()
                .getLakeTaskExecutor();
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.READ)) {
            OlapTable olapTable = lockedTable.get();
            boolean useAggregatePublish = olapTable.isFileBundling();
            for (Map.Entry<Long, DynamicTabletContext> physicalPartitionEntry : dynamicTabletContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }

                DynamicTabletContext dynamicTabletContext = physicalPartitionEntry.getValue();
                int publishState = dynamicTabletContext.getPublishState();
                if (publishState > 0) { // Publish success
                    continue;
                }

                allPublishFinished = false;

                if (publishState == 0) { // Publish in progress
                    continue;
                }

                Future<Boolean> future = publishThreadPool.submit(() -> publishPhysicalPartition(physicalPartition,
                        dynamicTabletContext.getCommitVersion(), useAggregatePublish));
                dynamicTabletContext.setPublishFuture(future);
            }
        }

        if (!allPublishFinished) {
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

        setJobState(JobState.FINISHED);
    }

    // Clear new tablets
    @Override
    protected void runAbortingJob() {
        if (!canAbort()) {
            return;
        }

        clearDynamicTablets();

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
                setDynamicTablets();
                break;
            case PREPARING:
                updateNextVersion(true);
                break;
            case RUNNING:
                break;
            case CLEANING:
                replaceMaterializedIndexes();
                break;
            case FINISHED:
                removeMaterializedIndexes();
                break;
            case ABORTING:
                break;
            case ABORTED:
                clearDynamicTablets();
                break;
            default:
                break;
        }
    }

    private void updateNextVersion(boolean replay) {
        try (LockedObject<OlapTable> lockedTable = getLockedTable(LockType.WRITE)) {
            OlapTable olapTable = lockedTable.get();
            for (Map.Entry<Long, DynamicTabletContext> physicalPartitionEntry : dynamicTabletContexts.entrySet()) {
                PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(physicalPartitionEntry.getKey());
                if (physicalPartition == null) {
                    continue;
                }
                DynamicTabletContext dynamicTabletContext = physicalPartitionEntry.getValue();

                if (replay) {
                    long commitVersion = dynamicTabletContext.getCommitVersion();
                    physicalPartition.setNextVersion(commitVersion + 1);
                } else {
                    long commitVersion = physicalPartition.getNextVersion();
                    physicalPartition.setNextVersion(commitVersion + 1);
                    dynamicTabletContext.setCommitVersion(commitVersion);
                }
            }
        }
    }

    private boolean publishPhysicalPartition(PhysicalPartition physicalPartition, long commitVersion,
            boolean useAggregatePublish) {
        return false;
    }

    private void replaceMaterializedIndexes() {
        forEachDynamicTablets(LockType.WRITE, entry -> {
            PhysicalPartition physicalPartition = entry.getPhysicalPartition();
            long newIndexId = GlobalStateMgr.getCurrentState().getNextId();
            MaterializedIndex oldIndex = entry.getMaterializedIndex();
            MaterializedIndex newIndex = new MaterializedIndex(newIndexId,
                    MaterializedIndex.IndexState.NORMAL, oldIndex.getShardGroupId());

            DynamicTablets dynamicTablets = oldIndex.getDynamicTablets();
            Set<Long> oldTabletIds = dynamicTablets.getOldTabletIds();
            TStorageMedium storageMedium = entry.getOlapTable().getPartitionInfo()
                    .getDataProperty(physicalPartition.getParentId()).getStorageMedium();

            for (Tablet tablet : oldIndex.getTablets()) {
                if (oldTabletIds.contains(tablet.getId())) {
                    continue;
                }
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartition.getId(), newIndexId, 0,
                        storageMedium, true);
                newIndex.addTablet(tablet, tabletMeta);
            }

            for (Tablet tablet : dynamicTablets.getNewTablets()) {
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, physicalPartition.getId(), newIndexId, 0,
                        storageMedium, true);
                newIndex.addTablet(tablet, tabletMeta);
            }

            newIndex.setVirtualBuckets(dynamicTablets.calcNewVirtualBuckets(oldIndex.getVirtualBuckets()));

            // TODO:
        });
    }

    private void removeMaterializedIndexes() {

    }
}
