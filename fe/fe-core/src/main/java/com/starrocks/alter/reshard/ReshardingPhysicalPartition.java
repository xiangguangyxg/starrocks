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

package com.starrocks.alter.reshard;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.Future;

/*
 * ReshardingPhysicalPartition saves the context during tablet splitting or merging for a physical partition
 */
public class ReshardingPhysicalPartition {
    private static final Logger LOG = LogManager.getLogger(ReshardingPhysicalPartition.class);

    @SerializedName(value = "physicalPartitionId")
    protected final long physicalPartitionId;

    @SerializedName(value = "reshardingMaterializedIndexes")
    protected final Map<Long, ReshardingMaterializedIndex> reshardingMaterializedIndexes;

    @SerializedName(value = "commitVersion")
    protected long commitVersion;

    protected Future<Boolean> publishFuture;

    public ReshardingPhysicalPartition(long physicalPartitionId,
            Map<Long, ReshardingMaterializedIndex> reshardingMaterializedIndexes) {
        this.physicalPartitionId = physicalPartitionId;
        this.reshardingMaterializedIndexes = reshardingMaterializedIndexes;
    }

    public long getPhysicalPartitionId() {
        return physicalPartitionId;
    }

    public Map<Long, ReshardingMaterializedIndex> getReshardingMaterializedIndexes() {
        return reshardingMaterializedIndexes;
    }

    public void setCommitVersion(long commitVersion) {
        this.commitVersion = commitVersion;
    }

    public long getCommitVersion() {
        return commitVersion;
    }

    public void setPublishFuture(Future<Boolean> publishFuture) {
        this.publishFuture = publishFuture;
    }

    public static enum PublishState {
        PUBLISH_NOT_STARTED, // Publish not started
        PUBLISH_IN_PROGRESS, // Publish in progress
        PUBLISH_SUCCESS, // Publish success
        PUBLISH_FAILED, // Publish failed
    }

    public PublishState getPublishState() {
        if (publishFuture == null) {
            return PublishState.PUBLISH_NOT_STARTED;
        }

        if (!publishFuture.isDone()) {
            return PublishState.PUBLISH_IN_PROGRESS;
        }

        try {
            return publishFuture.get() ? PublishState.PUBLISH_SUCCESS : PublishState.PUBLISH_FAILED;
        } catch (InterruptedException e) {
            LOG.warn("Interrupted to publish future get. ", e);
            Thread.currentThread().interrupt();
            return PublishState.PUBLISH_IN_PROGRESS;
        } catch (Exception e) {
            LOG.warn("Failed to publish future get. ", e);
            return PublishState.PUBLISH_FAILED;
        }
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingMaterializedIndex reshardingMaterializedIndex : reshardingMaterializedIndexes.values()) {
            parallelTablets += reshardingMaterializedIndex.getParallelTablets();
        }
        return parallelTablets;
    }
}
