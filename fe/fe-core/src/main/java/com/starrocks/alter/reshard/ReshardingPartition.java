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

import java.util.Map;

/*
 * ReshardingPartition saves the context during tablet splitting or merging for a partition
 */
public class ReshardingPartition {

    @SerializedName(value = "partitionId")
    protected final long partitionId;

    @SerializedName(value = "reshardingPhysicalPartitions")
    protected final Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions;

    public ReshardingPartition(long partitionId, Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) {
        this.partitionId = partitionId;
        this.reshardingPhysicalPartitions = reshardingPhysicalPartitions;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public Map<Long, ReshardingPhysicalPartition> getReshardingPhysicalPartitions() {
        return reshardingPhysicalPartitions;
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            parallelTablets += reshardingPhysicalPartition.getParallelTablets();
        }
        return parallelTablets;
    }
}
