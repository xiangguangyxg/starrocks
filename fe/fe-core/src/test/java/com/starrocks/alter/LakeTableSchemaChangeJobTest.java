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

package com.starrocks.alter;

import com.google.api.client.util.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.Utils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.utframe.MockedWarehouseManager;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class LakeTableSchemaChangeJobTest {
    private static final int NUM_BUCKETS = 4;
    private static ConnectContext connectContext;
    private static final String DB_NAME = "db_lake_schema_change_test";
    private static Database db;
    private LakeTableSchemaChangeJob schemaChangeJob;
    private LakeTable table;

    public LakeTableSchemaChangeJobTest() {
    }

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    private static LakeTable createTable(ConnectContext connectContext, String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(createTableStmt.getDbName());
        return (LakeTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), createTableStmt.getTableName());
    }

    private static void alterTable(ConnectContext connectContext, String sql) throws Exception {
        AlterTableStmt stmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, stmt);
    }

    private LakeTableSchemaChangeJob getAlterJob(Table table) {
        AlterJobMgr alterJobMgr = GlobalStateMgr.getCurrentState().getAlterJobMgr();
        List<AlterJobV2> jobs = alterJobMgr.getSchemaChangeHandler().getUnfinishedAlterJobV2ByTableId(table.getId());
        alterJobMgr.getSchemaChangeHandler().clearJobs();
        Assertions.assertEquals(1, jobs.size());
        AlterJobV2 alterJob = jobs.get(0);
        Assertions.assertTrue(alterJob instanceof LakeTableSchemaChangeJob);
        return (LakeTableSchemaChangeJob) alterJob;
    }

    @BeforeEach
    public void before() throws Exception {
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        connectContext.setDatabase(DB_NAME);
        db = GlobalStateMgr.getServingState().getLocalMetastore().getDb(DB_NAME);
        table = createTable(connectContext, "CREATE TABLE t0(c0 INT) duplicate key(c0) distributed by hash(c0) buckets "
                    + NUM_BUCKETS);
        Config.enable_fast_schema_evolution_in_share_data_mode = false;
        alterTable(connectContext, "ALTER TABLE t0 ADD COLUMN c1 BIGINT AS c0 + 2");
        schemaChangeJob = getAlterJob(table);
        Config.enable_fast_schema_evolution_in_share_data_mode = true;
    }

    @AfterEach
    public void after() throws Exception {
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropDb(connectContext, DB_NAME, true);
    }

    @Test
    public void testCancelPendingJob() throws IOException {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
        // test cancel again
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testDropTableBeforeCancel() {
        db.dropTable(table.getName());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testPendingJobNoAliveBackend() {
        MockedWarehouseManager mockedWarehouseManager = new MockedWarehouseManager();
        new MockUp<GlobalStateMgr>() {
            @Mock
            public WarehouseManager getWarehouseMgr() {
                return mockedWarehouseManager;
            }
        };

        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return null;
            }
        };
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }
        };

        mockedWarehouseManager.setComputeNodesAssignedToTablet(null);
        Exception exception = Assertions.assertThrows(AlterCancelException.class, () ->
                schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("No alive backend"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testTableDroppedInPending() {
        new MockUp<Utils>() {
            @Mock
            public Long chooseNodeId(LakeTablet tablet) {
                return 1L;
            }
        };

        db.dropTable(table.getName());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("Table does not exist"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());
        exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("Database does not exist"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testCreateTabletFailed() {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds, AtomicBoolean waitingCreatingReplica,
                                             AtomicBoolean isCancelling) throws AlterCancelException {
                throw new AlterCancelException("Create tablet failed");
            }
        };

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains("Create tablet failed"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testCreateTabletSuccess() throws AlterCancelException {
        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testPreviousTxnNotFinished() throws AlterCancelException {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return false;
            }
        };

        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testThrowAnalysisExceptionWhileWaitingTxn() throws AlterCancelException {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                throw new AnalysisException("isPreviousLoadFinished exception");
            }
        };

        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runWaitingTxnJob());
        Assertions.assertTrue(exception.getMessage().contains("sPreviousLoadFinished exception"));
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testTableNotExistWhileWaitingTxn() throws AlterCancelException {
        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        db.dropTable(table.getName());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runWaitingTxnJob());
        Assertions.assertTrue(exception.getMessage().contains("Table does not exist."));
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());
        exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runWaitingTxnJob());
        Assertions.assertTrue(exception.getMessage().contains("Database does not exist"));
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().put(db.getId(), db);
        db.registerTableUnlocked(table);
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testTableDroppedBeforeRewriting() throws AlterCancelException {
        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        db.dropTable(table.getName());
        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runRunningJob());
        Assertions.assertTrue(exception.getMessage().contains("Table or database does not exist"));

        db.registerTableUnlocked(table);
        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());

        exception = Assertions.assertThrows(AlterCancelException.class, () ->
                schemaChangeJob.runRunningJob());
        Assertions.assertTrue(exception.getMessage().contains("Table or database does not exist"));

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().put(db.getId(), db);
        db.registerTableUnlocked(table);
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testAlterTabletFailed() throws AlterCancelException {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().stream().findFirst().get().failed();
                batchTask.getAllTasks().stream().findFirst().get().failed();
                batchTask.getAllTasks().stream().findFirst().get().failed();
            }
        };

        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () -> schemaChangeJob.runRunningJob());
        Assertions.assertTrue(exception.getMessage().contains("schema change task failed after try three times"));

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assertions.assertEquals(0, partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testAlterTabletSuccess() throws AlterCancelException {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());
        Assertions.assertTrue(schemaChangeJob.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);
        Collection<Partition> partitions = table.getPartitions();
        Assertions.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);
        Assertions.assertEquals(3, partition.getDefaultPhysicalPartition().getNextVersion());
        List<MaterializedIndex> shadowIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(1, shadowIndexes.size());

        // Does not support cancel job in FINISHED_REWRITING state.
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // LakeTablet alter job will not mark tablet force delete into TabletInvertedIndex
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getForceDeleteTablets().isEmpty());

        // Drop the table, now it's ok to cancel the job
        db.dropTable(table.getName());
        schemaChangeJob.cancel("table does not exist anymore");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testAlterTabletSuccessEnablePartitionAgg() throws Exception {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        LakeTable table1 = createTable(connectContext, 
                    "CREATE TABLE t1(c0 INT) duplicate key(c0) distributed by hash(c0) buckets 3 " +
                                "PROPERTIES('file_bundling'='true')");
        
        Config.enable_fast_schema_evolution_in_share_data_mode = false;
        alterTable(connectContext, "ALTER TABLE t1 ADD COLUMN c1 BIGINT AS c0 + 2");
        LakeTableSchemaChangeJob schemaChangeJob1 = getAlterJob(table1);
        
        schemaChangeJob1.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob1.getJobState());

        schemaChangeJob1.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob1.getJobState());

        schemaChangeJob1.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob1.getJobState());
        Assertions.assertTrue(schemaChangeJob1.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);
        Collection<Partition> partitions = table1.getPartitions();
        Assertions.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);
        Assertions.assertEquals(3, partition.getDefaultPhysicalPartition().getNextVersion());
        List<MaterializedIndex> shadowIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(1, shadowIndexes.size());

        // Does not support cancel job in FINISHED_REWRITING state.
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob1.getJobState());

        while (schemaChangeJob1.getJobState() != AlterJobV2.JobState.FINISHED) {
            schemaChangeJob1.runFinishedRewritingJob();
            Thread.sleep(100);
        }
    }

    @Test
    public void testPublishVersion() throws AlterCancelException, InterruptedException {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }
        };

        schemaChangeJob.runPendingJob();
        Assertions.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.runWaitingTxnJob();
        Assertions.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        Collection<Partition> partitions = table.getPartitions();
        Assertions.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assertions.assertNotNull(partition);

        Assertions.assertEquals(1, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(2, partition.getDefaultPhysicalPartition().getNextVersion());
        // Disable send publish version
        partition.getDefaultPhysicalPartition().setNextVersion(3);

        schemaChangeJob.runRunningJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        List<MaterializedIndex> shadowIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(1, shadowIndexes.size());

        // The partition's visible version has not catch up with the commit version of this schema change job now.
        schemaChangeJob.runFinishedRewritingJob();
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Reset partition's next version
        partition.getDefaultPhysicalPartition().setVisibleVersion(2, System.currentTimeMillis());

        // Drop table
        db.dropTable(table.getName());

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () ->
                schemaChangeJob.runFinishedRewritingJob());
        Assertions.assertTrue(exception.getMessage().contains("Table does not exist"));
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Add table back to database
        db.registerTableUnlocked(table);

        // Make publish version success
        new MockUp<AlterJobV2>() {
            @Mock
            public boolean publishVersion() {
                return true;
            }
        };

        while (schemaChangeJob.getJobState() != AlterJobV2.JobState.FINISHED) {
            schemaChangeJob.runFinishedRewritingJob();
            Thread.sleep(100);
        }
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, schemaChangeJob.getJobState());
        Assertions.assertTrue(schemaChangeJob.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);

        Assertions.assertEquals(2, table.getBaseSchema().size());
        Assertions.assertEquals("c0", table.getBaseSchema().get(0).getName());
        Assertions.assertEquals("c1", table.getBaseSchema().get(1).getName());

        Assertions.assertSame(partition, table.getPartitions().stream().findFirst().get());
        Assertions.assertEquals(3, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assertions.assertEquals(4, partition.getDefaultPhysicalPartition().getNextVersion());

        shadowIndexes = partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assertions.assertEquals(0, shadowIndexes.size());

        List<MaterializedIndex> normalIndexes =
                    partition.getDefaultPhysicalPartition().getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
        Assertions.assertEquals(1, normalIndexes.size());
        MaterializedIndex normalIndex = normalIndexes.get(0);

        // Does not support cancel job in FINISHED state.
        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.FINISHED, schemaChangeJob.getJobState());
    }

    @Test
    public void testTransactionRaceCondition() throws AlterCancelException {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public long peekNextTransactionId() {
                return 10103L; // !!!! <-------- 10103 != 10101 + 1
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                return true;
            }
        };

        Exception exception = Assertions.assertThrows(AlterCancelException.class, () ->
                schemaChangeJob.runPendingJob());
        Assertions.assertTrue(exception.getMessage().contains(
                    "concurrent transaction detected while adding shadow index, please re-run the alter table command"));
        Assertions.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assertions.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assertions.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testShow() {
        UtFrameUtils.mockInitWarehouseEnv();

        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        LakeTableSchemaChangeJob alterJobV2 =
                    new LakeTableSchemaChangeJob(12345L, db.getId(), table.getId(), table.getName(), 10);
        alterJobV2.addIndexSchema(1L, 2L, "a", (short) 1, Lists.newArrayList());

        schemaChangeHandler.addAlterJobV2(alterJobV2);
        System.out.println(schemaChangeHandler.getAlterJobInfosByDb(db));

        SchemaChangeHandler schemaChangeHandler2 = new SchemaChangeHandler();
        alterJobV2 = new LakeTableSchemaChangeJob(12345L, db.getId(), table.getId(), table.getName(), 10);
        alterJobV2.addIndexSchema(1L, 2L, "a", (short) 1, Lists.newArrayList());
        schemaChangeHandler2.addAlterJobV2(alterJobV2);
        System.out.println(schemaChangeHandler2.getAlterJobInfosByDb(db));
    }

    @Test
    public void testCancelPendingJobWithFlag() throws Exception {
        schemaChangeJob.setIsCancelling(true);
        schemaChangeJob.runPendingJob();
        schemaChangeJob.setIsCancelling(false);

        schemaChangeJob.setWaitingCreatingReplica(true);
        schemaChangeJob.cancel("");
        schemaChangeJob.setWaitingCreatingReplica(false);
    }
}
