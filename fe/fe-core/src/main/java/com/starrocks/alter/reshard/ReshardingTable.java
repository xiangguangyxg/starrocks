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
 * ReshardingTable saves the context during tablet splitting or merging for a table
 */
public class ReshardingTable {

    @SerializedName(value = "tableId")
    protected final long tableId;

    @SerializedName(value = "reshardingPartitions")
    protected final Map<Long, ReshardingPartition> reshardingPartitions;

    public ReshardingTable(long tableId, Map<Long, ReshardingPartition> reshardingPartitions) {
        this.tableId = tableId;
        this.reshardingPartitions = reshardingPartitions;
    }

    public long getTableId() {
        return tableId;
    }

    public Map<Long, ReshardingPartition> getReshardingPartitions() {
        return reshardingPartitions;
    }

    public long getParallelTablets() {
        long parallelTablets = 0;
        for (ReshardingPartition reshardingPartition : reshardingPartitions.values()) {
            parallelTablets += reshardingPartition.getParallelTablets();
        }
        return parallelTablets;
    }
}
