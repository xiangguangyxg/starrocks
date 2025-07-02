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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SplitTabletClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SplitTabletJob extends DynamicTabletJob {
    private static final Logger LOG = LogManager.getLogger(SplitTabletJob.class);

    public SplitTabletJob(Database db, OlapTable table, SplitTabletClause splitTabletClause) {
        super(GlobalStateMgr.getCurrentState().getNextId(), JobType.SPLIT_TABLET, db.getId(), table.getId(), null);
    }

    protected void runPendingJob() {

    }

    protected void runPreparingJob() {

    }

    protected void runRunningJob() {

    }

    protected void runCleaningJob() {

    }

    protected void runAbortingJob() {

    }

    protected boolean canAbort() {
        return false;
    }

    public long getParallelTablets() {
        return 0;
    }

    public void replay() {

    }
}
