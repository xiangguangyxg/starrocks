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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/Coordinator.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.common.Config;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.ThriftServer;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.AuditStatisticsUtil;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.connector.exception.GlobalDictNotMatchException;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.datacache.DataCacheSelectMetrics;
import com.starrocks.metric.MetricRepo;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.qe.scheduler.Deployer;
import com.starrocks.qe.scheduler.QueryRuntimeProfile;
import com.starrocks.qe.scheduler.dag.AllAtOnceExecutionSchedule;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.ExecutionSchedule;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.qe.scheduler.dag.PhasedExecutionSchedule;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.LoadPlanner;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryType;
import com.starrocks.thrift.TReportAuditStatisticsParams;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TRuntimeFilterDestination;
import com.starrocks.thrift.TRuntimeFilterProberParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DefaultCoordinator extends Coordinator {
    private static final Logger LOG = LogManager.getLogger(DefaultCoordinator.class);

    private static final int DEFAULT_PROFILE_TIMEOUT_SECOND = 2;

    private final JobSpec jobSpec;
    private final ExecutionDAG executionDAG;

    private final ConnectContext connectContext;

    private final CoordinatorPreprocessor coordinatorPreprocessor;

    /**
     * Protects all the fields below.
     */
    private final Lock lock = new ReentrantLock();

    /**
     * Overall status of the entire query.
     * <p> Set to the first reported fragment error status or to CANCELLED, if {@link #cancel(String cancelledMessage)} is called.
     */
    private Status queryStatus = new Status();

    private PQueryStatistics auditStatistics;

    private final QueryRuntimeProfile queryProfile;

    private ResultReceiver receiver;
    private int numReceivedRows = 0;

    /**
     * True indicates the query is done returning all results.
     * <p> It is possible that the coordinator still needs to wait for cleanup on remote fragments (e.g. queries with limit)
     * Once this is set to true, errors from remote fragments are ignored.
     */
    private boolean returnedAllResults;

    private boolean thriftServerHighLoad;

    private LogicalSlot slot = null;

    private ShortCircuitExecutor shortCircuitExecutor = null;
    private boolean isShortCircuit = false;
    private boolean isBinaryRow = false;

    private long estimatedMemCost;
    private ExecutionSchedule scheduler;

    public static class Factory implements Coordinator.Factory {

        @Override
        public DefaultCoordinator createQueryScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                       List<ScanNode> scanNodes,
                                                       TDescriptorTable descTable) {
            JobSpec jobSpec =
                    JobSpec.Factory.fromQuerySpec(context, fragments, scanNodes, descTable, TQueryType.SELECT);
            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createInsertScheduler(ConnectContext context, List<PlanFragment> fragments,
                                                        List<ScanNode> scanNodes,
                                                        TDescriptorTable descTable) {
            JobSpec jobSpec = JobSpec.Factory.fromQuerySpec(context, fragments, scanNodes, descTable, TQueryType.LOAD);
            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createBrokerLoadScheduler(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();
            JobSpec jobSpec = JobSpec.Factory.fromBrokerLoadJobSpec(loadPlanner);

            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createStreamLoadScheduler(LoadPlanner loadPlanner) {
            ConnectContext context = loadPlanner.getContext();
            JobSpec jobSpec = JobSpec.Factory.fromStreamLoadJobSpec(loadPlanner);

            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createSyncStreamLoadScheduler(StreamLoadPlanner planner, TNetworkAddress address) {
            JobSpec jobSpec = JobSpec.Factory.fromSyncStreamLoadSpec(planner);
            return new DefaultCoordinator(jobSpec, planner, address);
        }

        @Override
        public DefaultCoordinator createBrokerExportScheduler(Long jobId, TUniqueId queryId, DescriptorTable descTable,
                                                              List<PlanFragment> fragments, List<ScanNode> scanNodes,
                                                              String timezone,
                                                              long startTime, Map<String, String> sessionVariables,
                                                              long execMemLimit, ComputeResource computeResource) {
            ConnectContext context = new ConnectContext();
            context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
            context.getSessionVariable().setEnablePipelineEngine(true);
            context.getSessionVariable().setPipelineDop(0);
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setCurrentWarehouseId(computeResource.getWarehouseId());
            context.setCurrentComputeResource(computeResource);

            JobSpec jobSpec = JobSpec.Factory.fromBrokerExportSpec(context, jobId, queryId, descTable,
                    fragments, scanNodes, timezone,
                    startTime, sessionVariables, execMemLimit);

            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createRefreshDictionaryCacheScheduler(ConnectContext context, TUniqueId queryId,
                                                                        DescriptorTable descTable,
                                                                        List<PlanFragment> fragments,
                                                                        List<ScanNode> scanNodes) {

            JobSpec jobSpec = JobSpec.Factory.fromRefreshDictionaryCacheSpec(context, queryId, descTable, fragments,
                    scanNodes);
            return new DefaultCoordinator(context, jobSpec);
        }

        @Override
        public DefaultCoordinator createNonPipelineBrokerLoadScheduler(Long jobId, TUniqueId queryId,
                                                                       DescriptorTable descTable,
                                                                       List<PlanFragment> fragments,
                                                                       List<ScanNode> scanNodes,
                                                                       String timezone,
                                                                       long startTime,
                                                                       Map<String, String> sessionVariables,
                                                                       ConnectContext context, long execMemLimit,
                                                                       long warehouseId) {
            JobSpec jobSpec = JobSpec.Factory.fromNonPipelineBrokerLoadJobSpec(context, jobId, queryId, descTable,
                    fragments, scanNodes, timezone,
                    startTime, sessionVariables, execMemLimit, warehouseId);

            return new DefaultCoordinator(context, jobSpec);
        }
    }

    /**
     * Only used for sync stream load profile, and only init relative data structure.
     */
    public DefaultCoordinator(JobSpec jobSpec, StreamLoadPlanner planner, TNetworkAddress address) {
        this.connectContext = planner.getConnectContext();
        this.jobSpec = jobSpec;
        this.executionDAG = ExecutionDAG.build(jobSpec);

        TUniqueId queryId = jobSpec.getQueryId();

        LOG.info("Execution Profile: {}", DebugUtil.printId(queryId));

        FragmentInstanceExecState execState = FragmentInstanceExecState.createFakeExecution(queryId, address);
        executionDAG.addExecution(execState);

        this.queryProfile = new QueryRuntimeProfile(connectContext, jobSpec, false);
        queryProfile.initFragmentProfiles(1);
        queryProfile.attachInstances(Collections.singletonList(queryId));
        queryProfile.attachExecutionProfiles(executionDAG.getExecutions());

        this.coordinatorPreprocessor = new CoordinatorPreprocessor(connectContext, jobSpec, false);
        this.scheduler = new AllAtOnceExecutionSchedule();
    }

    DefaultCoordinator(ConnectContext context, JobSpec jobSpec) {
        this.connectContext = context;
        this.jobSpec = jobSpec;
        this.returnedAllResults = false;

        final boolean enablePhasedScheduler = context.getSessionVariable().enablePhasedScheduler();
        this.coordinatorPreprocessor = new CoordinatorPreprocessor(context, jobSpec, enablePhasedScheduler);
        this.executionDAG = coordinatorPreprocessor.getExecutionDAG();

        List<PlanFragment> fragments = jobSpec.getFragments();
        List<ScanNode> scanNodes = jobSpec.getScanNodes();
        TDescriptorTable descTable = jobSpec.getDescTable();

        if (connectContext.getCommand() == MysqlCommand.COM_STMT_EXECUTE) {
            isBinaryRow = true;
        }

        shortCircuitExecutor =
                ShortCircuitExecutor.create(context, fragments, scanNodes, descTable, isBinaryRow,
                        jobSpec.isNeedReport(),
                        jobSpec.getPlanProtocol(), coordinatorPreprocessor.getWorkerProvider());

        if (null != shortCircuitExecutor) {
            isShortCircuit = true;
        }
        if (enablePhasedScheduler) {
            scheduler = new PhasedExecutionSchedule(connectContext);
        } else {
            scheduler = new AllAtOnceExecutionSchedule();
        }

        this.queryProfile =
                new QueryRuntimeProfile(connectContext, jobSpec,
                        isShortCircuit);
    }

    @Override
    public LogicalSlot getSlot() {
        return slot;
    }

    public void setSlot(LogicalSlot slot) {
        this.slot = slot;
    }

    @Override
    public long getLoadJobId() {
        return jobSpec.getLoadJobId();
    }

    @Override
    public void setLoadJobId(Long jobId) {
        jobSpec.setLoadJobId(jobId);
    }

    @Override
    public TUniqueId getQueryId() {
        return jobSpec.getQueryId();
    }

    @Override
    public void setQueryId(TUniqueId queryId) {
        jobSpec.setQueryId(queryId);
    }

    @Override
    public void setLoadJobType(TLoadJobType type) {
        jobSpec.setLoadJobType(type);
    }

    @Override
    public TLoadJobType getLoadJobType() {
        return jobSpec.getLoadJobType();
    }

    @Override
    public Status getExecStatus() {
        return queryStatus;
    }

    public QueryRuntimeProfile getQueryRuntimeProfile() {
        return queryProfile;
    }

    @Override
    public RuntimeProfile getQueryProfile() {
        return queryProfile.getQueryProfile();
    }

    @Override
    public List<String> getDeltaUrls() {
        return queryProfile.getDeltaUrls();
    }

    @Override
    public Map<String, String> getLoadCounters() {
        return queryProfile.getLoadCounters();
    }

    @Override
    public String getTrackingUrl() {
        return queryProfile.getTrackingUrl();
    }

    @Override
    public List<String> getRejectedRecordPaths() {
        return queryProfile.getRejectedRecordPaths();
    }

    @Override
    public long getStartTimeMs() {
        return jobSpec.getStartTimeMs();
    }

    public JobSpec getJobSpec() {
        return jobSpec;
    }

    @Override
    public void setTimeoutSecond(int timeoutSecond) {
        jobSpec.setQueryTimeout(timeoutSecond);
    }

    @Override
    public void setPredictedCost(long memBytes) {
        this.estimatedMemCost = memBytes;
    }

    public long getPredictedCost() {
        return estimatedMemCost;
    }

    @Override
    public void clearExportStatus() {
        lock.lock();
        try {
            executionDAG.resetExecutions();
            queryStatus.setStatus(new Status());
            queryProfile.clearExportStatus();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<TTabletCommitInfo> getCommitInfos() {
        return queryProfile.getCommitInfos();
    }

    @Override
    public List<TTabletFailInfo> getFailInfos() {
        return queryProfile.getFailInfos();
    }

    @Override
    public List<TSinkCommitInfo> getSinkCommitInfos() {
        return queryProfile.getSinkCommitInfos();
    }

    @Override
    public List<String> getExportFiles() {
        return queryProfile.getExportFiles();
    }

    @Override
    public void setTopProfileSupplier(Supplier<RuntimeProfile> topProfileSupplier) {
        queryProfile.setTopProfileSupplier(topProfileSupplier);
    }

    @Override
    public void setExecPlan(ExecPlan execPlan) {
        queryProfile.setExecPlan(execPlan);
    }

    @Override
    public boolean isUsingBackend(Long backendID) {
        return coordinatorPreprocessor.getWorkerProvider().isWorkerSelected(backendID);
    }

    @Override
    public boolean isShortCircuit() {
        return isShortCircuit;
    }

    private void lock() {
        lock.lock();
    }

    private void unlock() {
        lock.unlock();
    }

    public ExecutionDAG getExecutionDAG() {
        return executionDAG;
    }

    // Initiate asynchronous execState of query. Returns as soon as all plan fragments
    // have started executing at their respective backends.
    // 'Request' must contain at least a coordinator plan fragment (ie, can't
    // be for a query like 'SELECT 1').
    // A call to Exec() must precede all other member function calls.
    public void prepareExec() throws StarRocksException {
        if (LOG.isDebugEnabled()) {
            if (!jobSpec.getScanNodes().isEmpty()) {
                LOG.debug("debug: in Coordinator::exec. query id: {}, planNode: {}",
                        DebugUtil.printId(jobSpec.getQueryId()),
                        jobSpec.getScanNodes().get(0).treeToThrift());
            }
            if (!jobSpec.getFragments().isEmpty()) {
                LOG.debug("debug: in Coordinator::exec. query id: {}, fragment: {}",
                        DebugUtil.printId(jobSpec.getQueryId()),
                        jobSpec.getFragments().get(0).toThrift());
            }
            LOG.debug("debug: in Coordinator::exec. query id: {}, desc table: {}",
                    DebugUtil.printId(jobSpec.getQueryId()), jobSpec.getDescTable());
        }

        if (slot != null && slot.getPipelineDop() > 0 &&
                slot.getPipelineDop() != jobSpec.getQueryOptions().getPipeline_dop()) {
            jobSpec.getFragments().forEach(fragment -> fragment.limitMaxPipelineDop(slot.getPipelineDop()));
        }

        if (connectContext != null) {
            if (connectContext.getSessionVariable().isEnableConnectorIncrementalScanRanges()) {
                jobSpec.setIncrementalScanRanges(true);
            }
        }

        coordinatorPreprocessor.prepareExec();

        prepareResultSink();

        prepareProfile();
    }

    @Override
    public void onReleaseSlots() {
        if (slot != null) {
            jobSpec.getSlotProvider().cancelSlotRequirement(slot);
            jobSpec.getSlotProvider().releaseSlot(slot);
        }
    }

    @Override
    public void onFinished() {
        onReleaseSlots();
        // for async profile, if Be doesn't report profile in time, we upload the most complete profile
        // into profile Manager here. IN other case, queryProfile.finishAllInstances just do nothing here
        queryProfile.finishAllInstances(Status.OK);
    }

    public CoordinatorPreprocessor getPrepareInfo() {
        return coordinatorPreprocessor;
    }

    public List<PlanFragment> getFragments() {
        return jobSpec.getFragments();
    }

    public boolean isLoadType() {
        return jobSpec.isLoadType();
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return jobSpec.getScanNodes();
    }

    @Override
    public void startScheduling(ScheduleOption option) throws StarRocksException, InterruptedException, RpcException {
        try (Timer timer = Tracers.watchScope(Tracers.Module.SCHEDULER, "Pending")) {
            QueryQueueManager.getInstance().maybeWait(connectContext, this);
        }

        if (isShortCircuit) {
            execShortCircuit();
            return;
        }

        try (Timer timer = Tracers.watchScope(Tracers.Module.SCHEDULER, "Prepare")) {
            prepareExec();
        }

        try (Timer timer = Tracers.watchScope(Tracers.Module.SCHEDULER, "Deploy")) {
            deliverExecFragments(option);
        }

        // Prevent `explain scheduler` from waiting until the profile timeout.
        if (!option.doDeploy) {
            queryProfile.finishAllInstances(Status.OK);
        }
    }

    @Override
    public Status scheduleNextTurn(TUniqueId fragmentInstanceId) {
        try {
            scheduler.tryScheduleNextTurn(fragmentInstanceId);
        } catch (Exception e) {
            LOG.warn("schedule fragment:{} next internal error:", DebugUtil.printId(fragmentInstanceId), e);
            cancel(PPlanFragmentCancelReason.INTERNAL_ERROR, e.getMessage());
            return Status.internalError(e.getMessage());
        }
        return Status.OK;
    }

    @Override
    public String getSchedulerExplain() {
        String predict = Config.enable_query_cost_prediction ?
                "predicted memory cost: " + getPredictedCost() + "\n" : "";
        return predict +
                executionDAG.getFragmentsInPreorder().stream()
                        .map(ExecutionFragment::getExplainString)
                        .collect(Collectors.joining("\n"));
    }

    private void prepareProfile() {
        this.queryProfile.initFragmentProfiles(executionDAG.getFragmentsInCreatedOrder().size());

        ExecutionFragment rootExecFragment = executionDAG.getRootFragment();
        boolean isLoadType = !(rootExecFragment.getPlanFragment().getSink() instanceof ResultSink);
        if (isLoadType) {
            // for non-pipeline engine, enable_profile is renamed from is_report_success,
            // which is not only for report profile but also for report success.
            // for pipeline engine, enable_profile is only for report profile.
            // when enable_profile is true by default, the runtime profile of pipeline engine may use a lot of memory.
            // so we only set enable_profile to true when use non-pipeline engine.
            if (!jobSpec.isEnablePipeline()) {
                jobSpec.getQueryOptions().setEnable_profile(true);
            }
            if (jobSpec.isBrokerLoad() && jobSpec.getQueryOptions().getBig_query_profile_threshold() == 0) {
                jobSpec.getQueryOptions()
                        .setBig_query_profile_threshold(Config.default_big_load_profile_threshold_second * 1000);
            }
            // runtime load profile does not need to report too frequently
            if (jobSpec.getQueryOptions().getRuntime_profile_report_interval() < 30) {
                jobSpec.getQueryOptions().setRuntime_profile_report_interval(30);
            }
            List<Long> relatedBackendIds = coordinatorPreprocessor.getWorkerProvider().getSelectedWorkerIds();
            GlobalStateMgr.getCurrentState().getLoadMgr().initJobProgress(
                    jobSpec.getLoadJobId(), jobSpec.getQueryId(), executionDAG.getInstanceIds(), relatedBackendIds);
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(jobSpec.getQueryId()),
                    coordinatorPreprocessor.getWorkerProvider().getSelectedWorkerIds());
        }

        queryProfile.attachInstances(executionDAG.getInstanceIds());
    }

    private void prepareResultSink() {
        ExecutionFragment rootExecFragment = executionDAG.getRootFragment();
        long workerId = rootExecFragment.getInstances().get(0).getWorkerId();
        ComputeNode worker = coordinatorPreprocessor.getWorkerProvider().getWorkerById(workerId);
        // Select top fragment as global runtime filter merge address
        setGlobalRuntimeFilterParams(rootExecFragment, worker.getBrpcIpAddress());
        boolean isLoadType = !(rootExecFragment.getPlanFragment().getSink() instanceof ResultSink);
        if (isLoadType) {
            return;
        }

        TNetworkAddress execBeAddr = worker.getAddress();
        receiver = new ResultReceiver(
                rootExecFragment.getInstances().get(0).getInstanceId(),
                workerId,
                worker.getBrpcAddress(),
                jobSpec.getQueryOptions().query_timeout * 1000);

        if (LOG.isDebugEnabled()) {
            LOG.debug("dispatch query job: {} to {}", DebugUtil.printId(jobSpec.getQueryId()), execBeAddr);
        }

        // set the broker address for OUTFILE sink
        ResultSink resultSink = (ResultSink) rootExecFragment.getPlanFragment().getSink();
        if (isBinaryRow) {
            resultSink.setBinaryRow(true);
        }
        if (resultSink.isOutputFileSink() && resultSink.needBroker()) {
            FsBroker broker = GlobalStateMgr.getCurrentState().getBrokerMgr().getBroker(resultSink.getBrokerName(),
                    execBeAddr.getHostname());
            resultSink.setBrokerAddr(broker.ip, broker.port);
            LOG.info("OUTFILE through broker: {}:{}", broker.ip, broker.port);
        }
    }

    private void deliverExecFragments(ScheduleOption option) throws RpcException, StarRocksException {
        lock();
        try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeployLockInternalTime")) {
            Deployer deployer =
                    new Deployer(connectContext, jobSpec, executionDAG, coordinatorPreprocessor.getCoordAddress(),
                            this::handleErrorExecution, option.doDeploy);
            scheduler.prepareSchedule(this, deployer, executionDAG);
            this.scheduler.schedule(option);
            MetricRepo.HISTO_DEPLOY_PLAN_FRAGMENTS_LATENCY.update(ignored.getTotalTime());
            queryProfile.attachExecutionProfiles(executionDAG.getExecutions());
        } finally {
            unlock();
        }
    }

    @Override
    public List<DeployState> assignIncrementalScanRangesToDeployStates(Deployer deployer, List<DeployState> deployStates)
            throws StarRocksException {
        List<DeployState> updatedStates = new ArrayList<>();
        if (!jobSpec.isIncrementalScanRanges()) {
            return updatedStates;
        }
        for (DeployState state : deployStates) {

            Set<PlanFragmentId> planFragmentIds = new HashSet<>();
            for (List<FragmentInstanceExecState> fragmentInstanceExecStates : state.getThreeStageExecutionsToDeploy()) {
                for (FragmentInstanceExecState execState : fragmentInstanceExecStates) {
                    planFragmentIds.add(execState.getFragmentId());
                }
            }

            Set<PlanFragmentId> updatedPlanFragmentIds = new HashSet<>();
            for (PlanFragmentId fragmentId : planFragmentIds) {
                boolean hasMoreScanRanges = false;
                ExecutionFragment fragment = executionDAG.getFragment(fragmentId);
                for (ScanNode scanNode : fragment.getScanNodes()) {
                    if (scanNode.hasMoreScanRanges()) {
                        hasMoreScanRanges = true;
                    }
                }
                if (hasMoreScanRanges) {
                    coordinatorPreprocessor.assignIncrementalScanRangesToFragmentInstances(fragment);
                    updatedPlanFragmentIds.add(fragmentId);
                }
            }

            if (updatedPlanFragmentIds.isEmpty()) {
                continue;
            }

            DeployState newState = new DeployState();
            updatedStates.add(newState);
            int index = 0;
            for (List<FragmentInstanceExecState> fragmentInstanceExecStates : state.getThreeStageExecutionsToDeploy()) {
                List<FragmentInstanceExecState> res = newState.getThreeStageExecutionsToDeploy().get(index);
                index += 1;
                for (FragmentInstanceExecState execState : fragmentInstanceExecStates) {
                    if (!updatedPlanFragmentIds.contains(execState.getFragmentId())) {
                        continue;
                    }
                    FragmentInstance instance = execState.getFragmentInstance();
                    TExecPlanFragmentParams request = deployer.createIncrementalScanRangesRequest(instance);
                    execState.setRequestToDeploy(request);
                    res.add(execState);
                }
            }
        }
        return updatedStates;
    }

    private boolean isInternalCancelError(String errMsg) {
        return errMsg.equals(FeConstants.LIMIT_REACH_ERROR) || errMsg.equals(FeConstants.QUERY_FINISHED_ERROR);
    }

    private void handleErrorExecution(Status status, FragmentInstanceExecState execution, Throwable failure)
            throws StarRocksException, RpcException {
        switch (Objects.requireNonNull(status.getErrorCode())) {
            case TIMEOUT:
                cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
                throw new StarRocksException("query timeout. backend id: " + execution.getWorker().getId());
            case THRIFT_RPC_ERROR:
                cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
                SimpleScheduler.addToBlocklist(execution.getWorker().getId());
                throw new RpcException(
                        String.format("rpc failed with %s: %s", execution.getWorker().getHost(), status.getErrorMsg()),
                        failure);
            case CANCELLED:
                if (isInternalCancelError(status.getErrorMsg())) {
                    // ignore the internal cancel error message
                    break;
                }
                // fallthrough
            default:
                cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
                dealStatusToTryRetry(status);
        }
    }

    // choose at most num FInstances on difference BEs
    private List<FragmentInstance> pickupFInstancesOnDifferentHosts(List<FragmentInstance> instances, int num) {
        if (instances.size() <= num) {
            return instances;
        }

        Map<Long, List<FragmentInstance>> workerId2instances = Maps.newHashMap();
        for (FragmentInstance instance : instances) {
            workerId2instances.putIfAbsent(instance.getWorkerId(), Lists.newLinkedList());
            workerId2instances.get(instance.getWorkerId()).add(instance);
        }
        List<FragmentInstance> picked = Lists.newArrayList();
        while (picked.size() < num) {
            for (List<FragmentInstance> instancesPerHost : workerId2instances.values()) {
                if (instancesPerHost.isEmpty()) {
                    continue;
                }
                picked.add(instancesPerHost.remove(0));
            }
        }
        return picked;
    }

    private List<TRuntimeFilterDestination> mergeGRFProbers(List<TRuntimeFilterProberParams> probers) {
        Map<TNetworkAddress, List<TUniqueId>> host2probers = Maps.newHashMap();
        for (TRuntimeFilterProberParams prober : probers) {
            host2probers.putIfAbsent(prober.fragment_instance_address, Lists.newArrayList());
            host2probers.get(prober.fragment_instance_address).add(prober.fragment_instance_id);
        }
        return host2probers.entrySet().stream().map(
                e -> new TRuntimeFilterDestination().setAddress(e.getKey()).setFinstance_ids(e.getValue())
        ).collect(Collectors.toList());
    }

    private void setGlobalRuntimeFilterParams(ExecutionFragment topParams,
                                              TNetworkAddress mergeHost) {

        Map<Integer, List<TRuntimeFilterProberParams>> broadcastGRFProbersMap = Maps.newHashMap();
        List<RuntimeFilterDescription> broadcastGRFList = Lists.newArrayList();
        Map<Integer, List<TRuntimeFilterProberParams>> idToProbePrams = new HashMap<>();

        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            PlanFragment fragment = execFragment.getPlanFragment();
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getProbeRuntimeFilters().entrySet()) {
                List<TRuntimeFilterProberParams> probeParamList = Lists.newArrayList();
                for (final FragmentInstance instance : execFragment.getInstances()) {
                    TRuntimeFilterProberParams probeParam = new TRuntimeFilterProberParams();
                    probeParam.setFragment_instance_id(instance.getInstanceId());
                    probeParam.setFragment_instance_address(
                            coordinatorPreprocessor.getBrpcIpAddress(instance.getWorkerId()));
                    probeParamList.add(probeParam);
                }
                if (jobSpec.isEnablePipeline() && kv.getValue().isBroadcastJoin() &&
                        kv.getValue().isHasRemoteTargets()) {
                    broadcastGRFProbersMap.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).addAll(probeParamList);
                } else {
                    idToProbePrams.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).addAll(probeParamList);
                }
            }

            Set<TUniqueId> broadcastGRfSenders =
                    pickupFInstancesOnDifferentHosts(execFragment.getInstances(), 3).stream().
                            map(FragmentInstance::getInstanceId).collect(Collectors.toSet());
            for (Map.Entry<Integer, RuntimeFilterDescription> kv : fragment.getBuildRuntimeFilters().entrySet()) {
                int rid = kv.getKey();
                RuntimeFilterDescription rf = kv.getValue();
                if (rf.isBroadCastJoinInSkew()) {
                    // runtime filter coordinator need to know this rid is generated by skew join optimization
                    // when merge runtime filter instance, it should wait not only shuffle join's rf but also boradcast rf
                    topParams.getRuntimeFilterParams()
                            .addToSkew_join_runtime_filters(rf.getSkew_shuffle_filter_id());
                    rf.setBroadcastGRFSenders(broadcastGRfSenders);
                } else if (rf.isHasRemoteTargets()) {
                    if (rf.isBroadcastJoin()) {
                        // for broadcast join, we send at most 3 copy to probers, the first arrival wins.
                        topParams.getRuntimeFilterParams().putToRuntime_filter_builder_number(rid, 1);
                        if (jobSpec.isEnablePipeline()) {
                            rf.setBroadcastGRFSenders(broadcastGRfSenders);
                            broadcastGRFList.add(rf);
                        } else {
                            rf.setSenderFragmentInstanceId(execFragment.getInstances().get(0).getInstanceId());
                        }
                    } else {
                        topParams.getRuntimeFilterParams()
                                .putToRuntime_filter_builder_number(rid, execFragment.getInstances().size());
                    }
                }
            }
            fragment.setRuntimeFilterMergeNodeAddresses(fragment.getPlanRoot(), mergeHost);
        }
        topParams.getRuntimeFilterParams().setId_to_prober_params(idToProbePrams);

        broadcastGRFList.forEach(rf -> rf.setBroadcastGRFDestinations(
                mergeGRFProbers(broadcastGRFProbersMap.get(rf.getFilterId()))));

        if (connectContext != null) {
            SessionVariable sessionVariable = connectContext.getSessionVariable();
            topParams.getRuntimeFilterParams().setRuntime_filter_max_size(
                    sessionVariable.getGlobalRuntimeFilterBuildMaxSize());
        }
    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        return coordinatorPreprocessor.getChannelIdToBEHTTPMap();
    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        return coordinatorPreprocessor.getChannelIdToBEPortMap();
    }

    private void updateStatus(Status status, TUniqueId instanceId) {
        lock.lock();
        try {
            // The query is done and we are just waiting for remote fragments to clean up.
            // Ignore their cancelled updates.
            if (returnedAllResults && status.isCancelled()) {
                return;
            }
            // nothing to update
            if (status.ok()) {
                return;
            }

            // don't override an error status; also, cancellation has already started
            if (!queryStatus.ok()) {
                return;
            }

            queryStatus.setStatus(status);
            LOG.warn(
                    "one instance report fail throw updateStatus(), need cancel. job id: {}, query id: {}, instance id: {}",
                    jobSpec.getLoadJobId(), DebugUtil.printId(jobSpec.getQueryId()),
                    instanceId != null ? DebugUtil.printId(instanceId) : "NaN");
            cancelInternal(PPlanFragmentCancelReason.INTERNAL_ERROR);
        } finally {
            lock.unlock();
        }
    }

    private void dealStatusToTryRetry(Status status) throws RpcException, StarRocksException {
        if (!status.ok()) {
            if (Strings.isNullOrEmpty(status.getErrorMsg())) {
                status.rewriteErrorMsg();
            }

            if (status.isRemoteFileNotFound()) {
                throw new RemoteFileNotFoundException(status.getErrorMsg());
            }

            if (status.isGlobalDictNotMatch()) {
                throw new GlobalDictNotMatchException(status.getErrorMsg());
            }

            if (status.isRpcError()) {
                throw new RpcException("unknown", status.getErrorMsg());
            } else {
                String errMsg = status.getErrorMsg();
                LOG.warn("query {} failed: {}", connectContext.queryId, errMsg);

                // hide host info
                int hostIndex = errMsg.indexOf("host");
                if (hostIndex != -1) {
                    errMsg = errMsg.substring(0, hostIndex);
                }
                InternalErrorCode ec = InternalErrorCode.INTERNAL_ERR;
                if (status.isCancelled() &&
                        status.getErrorMsg().equals(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR)) {
                    ec = InternalErrorCode.CANCEL_NODE_NOT_ALIVE_ERR;
                } else if (status.isTimeout()) {
                    ErrorReport.reportTimeoutException(
                            ErrorCode.ERR_TIMEOUT, "Query", jobSpec.getQueryOptions().query_timeout,
                            String.format("please increase the '%s' session variable and retry",
                                    SessionVariable.QUERY_TIMEOUT));
                }
                throw new StarRocksException(ec, errMsg);
            }
        }
    }

    @Override
    public RowBatch getNext() throws Exception {
        if (isShortCircuit) {
            return shortCircuitExecutor.getNext();
        }
        if (receiver == null) {
            throw new StarRocksException("There is no receiver.");
        }

        RowBatch resultBatch;
        Status status = new Status();

        resultBatch = receiver.getNext(status);
        if (!status.ok()) {
            connectContext.setErrorCodeOnce(status.getErrorCodeString());
            LOG.warn("get next fail, need cancel. status {}, query id: {}", status,
                    DebugUtil.printId(jobSpec.getQueryId()));
        }
        updateStatus(status, null /* no instance id */);

        Status copyStatus;
        lock();
        try {
            copyStatus = new Status(queryStatus);
        } finally {
            unlock();
        }
        dealStatusToTryRetry(copyStatus);

        if (resultBatch.isEos()) {
            this.returnedAllResults = true;

            // if this query is a block query do not cancel.
            long numLimitRows = executionDAG.getRootFragment().getPlanFragment().getPlanRoot().getLimit();
            boolean hasLimit = numLimitRows > 0;
            if (!jobSpec.isBlockQuery() && executionDAG.getInstanceIds().size() > 1) {
                if (hasLimit && numReceivedRows >= numLimitRows) {
                    LOG.debug("no block query, return num >= limit rows, need cancel");
                    cancelInternal(PPlanFragmentCancelReason.LIMIT_REACH);
                } else {
                    cancelInternal(PPlanFragmentCancelReason.QUERY_FINISHED);
                }
            }
        } else {
            numReceivedRows += resultBatch.getBatch().getRowsSize();
        }

        return resultBatch;
    }

    /**
     * Cancel execState of query. This includes the execState of the local plan fragment,
     * if any, as well as all plan fragments on remote nodes.
     */
    @Override
    public void cancel(PPlanFragmentCancelReason reason, String message) {
        lock();
        try {
            if (!queryStatus.ok()) {
                // we can't cancel twice
                return;
            } else {
                queryStatus.setStatus(Status.CANCELLED);
                queryStatus.setErrorMsg(message);
            }
            LOG.info("cancel query {} because {}", connectContext.queryId, message);
            cancelInternal(reason);
        } finally {
            try {
                // Disable count down profileDoneSignal for collect all backend's profile
                // but if backend has crashed, we need count down profileDoneSignal since it will not report by itself
                if (message.equals(FeConstants.BACKEND_NODE_NOT_FOUND_ERROR)) {
                    queryProfile.finishAllInstances(Status.OK);
                    LOG.info("count down profileDoneSignal since backend has crashed, query id: {}",
                            DebugUtil.printId(jobSpec.getQueryId()));
                }
            } finally {
                unlock();
            }
        }
    }

    private boolean isInternalCancel(PPlanFragmentCancelReason cancelReason) {
        return cancelReason.equals(PPlanFragmentCancelReason.LIMIT_REACH) ||
                cancelReason.equals(PPlanFragmentCancelReason.QUERY_FINISHED);
    }

    private void cancelInternal(PPlanFragmentCancelReason cancelReason) {
        jobSpec.getSlotProvider().cancelSlotRequirement(slot);
        if (!isInternalCancel(cancelReason) && StringUtils.isEmpty(connectContext.getState().getErrorMessage())) {
            connectContext.getState().setError(cancelReason.toString());
        }
        if (null != receiver) {
            receiver.cancel();
        }
        if (isPhasedSchedule()) {
            cancelRemoteQueryContext(cancelReason);
        } else {
            cancelRemoteFragmentsAsync(cancelReason);
        }
        if (!isInternalCancel(cancelReason)) {
            // count down to zero to notify all objects waiting for this
            if (!connectContext.isProfileEnabled()) {
                queryProfile.finishAllInstances(Status.OK);
            }
        }
    }

    private boolean isPhasedSchedule() {
        return scheduler instanceof PhasedExecutionSchedule;
    }

    // For phased schedule execution, we cancel the query context. (BE will cancel the relevant fragment internally)
    private void cancelRemoteQueryContext(PPlanFragmentCancelReason cancelReason) {
        executionDAG.cancelQueryContext(cancelReason);
    }

    private void cancelRemoteFragmentsAsync(PPlanFragmentCancelReason cancelReason) {
        scheduler.cancel();
        for (FragmentInstanceExecState execState : executionDAG.getExecutions()) {
            // If the execState fails to be cancelled, and it has been finished or not been deployed,
            // count down the profileDoneSignal of this execState immediately,
            // because the profile report will not arrive anymore for the finished or non-deployed execState.
            if (!execState.cancelFragmentInstance(cancelReason) &&
                    (!execState.hasBeenDeployed() || execState.isFinished())) {
                queryProfile.finishInstance(execState.getInstanceId());
            }
        }

        executionDAG.getInstances().stream()
                .filter(instance -> executionDAG.getExecution(instance.getIndexInJob()) == null)
                .forEach(instance -> queryProfile.finishInstance(instance.getInstanceId()));
    }

    @Override
    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        FragmentInstanceExecState execState = executionDAG.getExecution(params.getBackend_num());
        if (execState == null) {
            LOG.warn("unknown backend number: {}, valid backend numbers: {}", params.getBackend_num(),
                    executionDAG.getExecutionIndexesInJob());
            return;
        }

        // NOTE:
        // The exec status would affect query schedule, so it must be updated no matter what exceptions happen.
        // Otherwise, the query might hang until timeout
        if (!execState.updateExecStatus(params)) {
            LOG.info("duplicate report fragment exec status, query id: {}, instance id: {}, backend id: {}, " +
                            "status: {}, exec state: {}",
                    DebugUtil.printId(jobSpec.getQueryId()),
                    DebugUtil.printId(params.getFragment_instance_id()),
                    params.getBackend_id(), params.status, execState.getState());
            return;
        }

        final String instanceId = DebugUtil.printId(params.getFragment_instance_id());
        final boolean isDone = params.isDone();
        // Create a CompletableFuture chain for handling updates
        final CompletableFuture<Void> future = CompletableFuture.completedFuture(null)
                .thenRun(() -> {
                    try {
                        updateRuntimeProfile(params, execState, instanceId);
                    } catch (Throwable e) {
                        LOG.warn("update runtime profile failed {}", instanceId, e);
                    }
                })
                .thenRun(() -> {
                    // update load info if it's a isDone rpc
                    if (isDone && execState.isFinished()) {
                        updateFinishInstance(params, execState, instanceId);
                    }
                })
                .handle((result, ex) -> {
                    // all block are independent, continue the execution no matter what exception happen
                    if (ex != null) {
                        LOG.warn("Error occurred during fragment exec status update {}: {}", instanceId,
                                ex.getMessage());
                    }
                    return null; // Return null to continue the chain
                });

        try {
            future.get();
        } catch (Exception e) {
            LOG.warn("Error occurred during updateFragmentExecStatus {}", instanceId, e);
        }
    }

    /**
     * Update runtime profile and load profile no matter the input params is a finish or runtime profile
     */
    private void updateRuntimeProfile(TReportExecStatusParams params,
                                      FragmentInstanceExecState execState,
                                      String instanceId) {
        // update profile
        try {
            queryProfile.updateProfile(execState, params);
            execState.updateRunningProfile(params);
        } catch (Throwable e) {
            LOG.warn("update profile failed {}", instanceId, e);
        }

        // update load profile
        try {
            lock();
            queryProfile.updateLoadChannelProfile(params);
        } catch (Throwable e) {
            LOG.warn("update load channel profile failed {}", instanceId, e);
        } finally {
            unlock();
        }

        // update job progress
        try {
            updateJobProgress(params);
        } catch (Throwable e) {
            LOG.warn("update job progress failed {}", instanceId, e);
        }

        // update status
        Status status = new Status(params.status);
        if (!(returnedAllResults && status.isCancelled()) && !status.ok()) {
            ConnectContext ctx = connectContext;
            if (ctx != null) {
                ctx.setErrorCodeOnce(status.getErrorCodeString());
            }
            LOG.warn("exec state report failed status={}, query_id={}, instance_id={}, backend_id={}",
                    status, DebugUtil.printId(jobSpec.getQueryId()),
                    DebugUtil.printId(params.getFragment_instance_id()),
                    params.getBackend_id());
            updateStatus(status, params.getFragment_instance_id());
        }
    }

    /**
     * Update the instance only if the instance is finished.
     */
    private void updateFinishInstance(TReportExecStatusParams params,
                                      FragmentInstanceExecState execState,
                                      String instanceId) {
        // For DML jobs, finishInstance is ensured to be called only after instance finished and should not be
        // called repeatedly. Otherwise, it will cause commit with wrong commit info.
        if (jobSpec.isLoadType() && execState.getState().isFinished() && queryProfile.isFinished()) {
            throw new RuntimeException(String.format("updateFinishInstance called after fragment is finished:%s, query_id:%s",
                    instanceId, DebugUtil.printId(params.getQuery_id())));
        }
        try {
            lock();
            queryProfile.updateLoadInformation(execState, params);
        } catch (Throwable e) {
            LOG.warn("update load information failed {}", instanceId, e);
        } finally {
            unlock();
        }

        // NOTE: it's critical for query execution, and must be put after the profile update
        queryProfile.finishInstance(params.getFragment_instance_id());
    }

    @Override
    public synchronized void updateAuditStatistics(TReportAuditStatisticsParams params) {
        PQueryStatistics newAuditStatistics = AuditStatisticsUtil.toProtobuf(params.audit_statistics);
        if (auditStatistics == null) {
            auditStatistics = newAuditStatistics;
        } else {
            AuditStatisticsUtil.mergeProtobuf(newAuditStatistics, auditStatistics);
        }
    }

    private void updateJobProgress(TReportExecStatusParams params) {
        if (params.isSetLoad_type()) {
            TLoadJobType loadJobType = params.getLoad_type();
            if (loadJobType == TLoadJobType.BROKER ||
                    loadJobType == TLoadJobType.INSERT_QUERY ||
                    loadJobType == TLoadJobType.INSERT_VALUES) {
                if (params.isSetSink_load_bytes() && params.isSetSource_load_rows()
                        && params.isSetSource_load_bytes()) {
                    GlobalStateMgr.getCurrentState().getLoadMgr().updateJobPrgress(
                            jobSpec.getLoadJobId(), params);
                }
            }
        } else {
            if (params.isSetSink_load_bytes() && params.isSetSource_load_rows()
                    && params.isSetSource_load_bytes()) {
                GlobalStateMgr.getCurrentState().getLoadMgr().updateJobPrgress(
                        jobSpec.getLoadJobId(), params);
            }
        }
    }

    public void collectProfileSync() {
        if (executionDAG.getExecutions().isEmpty()) {
            return;
        }

        // wait for all backends
        if (jobSpec.isNeedReport()) {
            int timeout;
            // connectContext can be null for broker export task coordinator
            if (connectContext != null) {
                timeout = connectContext.getSessionVariable().getProfileTimeout();
            } else {
                timeout = DEFAULT_PROFILE_TIMEOUT_SECOND;
            }

            // Waiting for other fragment instances to finish execState
            // Ideally, it should wait indefinitely, but out of defense, set timeout
            boolean isFinished = queryProfile.waitForProfileFinished(timeout, TimeUnit.SECONDS);
            if (!isFinished) {
                LOG.warn("failed to get profile within {} seconds", timeout);
            }
        }

        lock();
        try {
            queryProfile.finalizeProfile();
        } finally {
            unlock();
        }
    }

    @Override
    public boolean tryProcessProfileAsync(Consumer<Boolean> task) {
        if (executionDAG.getExecutions().isEmpty() && (!isShortCircuit)) {
            return false;
        }
        if (!jobSpec.isNeedReport()) {
            return false;
        }
        boolean enableAsyncProfile = true;
        if (connectContext != null && connectContext.getSessionVariable() != null) {
            enableAsyncProfile = connectContext.getSessionVariable().isEnableAsyncProfile();
        }
        TUniqueId queryId = null;
        if (connectContext != null) {
            queryId = connectContext.getExecutionId();
        }

        if (!enableAsyncProfile || !queryProfile.addListener(task)) {
            if (enableAsyncProfile) {
                LOG.info("Profile task is full, execute in sync mode, query id = {}", DebugUtil.printId(queryId));
            }
            collectProfileSync();
            task.accept(false);
            return false;
        }
        return true;
    }

    /**
     * Waiting the coordinator finish executing.
     * return false if waiting timeout.
     * return true otherwise.
     * NOTICE: return true does not mean that coordinator executed success,
     * the caller should check queryStatus for result.
     * <p>
     * We divide the entire waiting process into multiple rounds,
     * with a maximum of 5 seconds per round. And after each round of waiting,
     * check the status of the BE. If the BE status is abnormal, the wait is ended
     * and the result is returned. Otherwise, continue to the next round of waiting.
     * This method mainly avoids the problem that the Coordinator waits for a long time
     * after some BE can no long return the result due to some exception, such as BE is down.
     */
    @Override
    public boolean join(int timeoutS) {
        final long fixedMaxWaitTime = 5;

        long leftTimeoutS = timeoutS;
        boolean awaitRes = false;
        while (leftTimeoutS > 0) {
            long waitTime = Math.min(leftTimeoutS, fixedMaxWaitTime);
            awaitRes = queryProfile.waitForProfileFinished(waitTime, TimeUnit.SECONDS);
            if (awaitRes) {
                return true;
            }

            if (!checkBackendState()) {
                return true;
            }

            if (ThriftServer.getExecutor() != null
                    && ThriftServer.getExecutor().getPoolSize() >= Config.thrift_server_max_worker_threads) {
                thriftServerHighLoad = true;
            }

            leftTimeoutS -= waitTime;
        }

        if (!awaitRes) {
            LOG.warn("failed to get profile within {} seconds", timeoutS);
        }
        return false;
    }

    // build execution profile  from every BE's report
    @Override
    public RuntimeProfile buildQueryProfile(boolean needMerge) {
        if (isShortCircuit) {
            return shortCircuitExecutor.buildQueryProfile(needMerge);
        }
        return queryProfile.buildQueryProfile(needMerge);
    }

    /**
     * Check the state of backends in needCheckBackendExecStates.
     * return true if all of them are OK. Otherwise, return false.
     */
    @Override
    public boolean checkBackendState() {
        for (FragmentInstanceExecState execState : executionDAG.getNeedCheckExecutions()) {
            if (!execState.isBackendStateHealthy()) {
                queryStatus = new Status(TStatusCode.INTERNAL_ERROR,
                        "backend " + execState.getWorker().getId() + " is down");
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isDone() {
        return queryProfile.isFinished();
    }

    @Override
    public boolean isEnableLoadProfile() {
        return connectContext != null && connectContext.getSessionVariable().isEnableLoadProfile();
    }

    /**
     * Get information of each fragment instance.
     *
     * @return the fragment instance information list, consistent with the fragment index of {@code EXPLAIN}.
     */
    @Override
    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        return executionDAG.getFragmentInstanceInfos();
    }

    @Override
    public DataCacheSelectMetrics getDataCacheSelectMetrics() {
        return queryProfile.getDataCacheSelectMetrics();
    }

    @Override
    public PQueryStatistics getAuditStatistics() {
        return auditStatistics;
    }

    @Override
    public boolean isThriftServerHighLoad() {
        return this.thriftServerHighLoad;
    }

    @Override
    public boolean isProfileAlreadyReported() {
        return this.queryProfile.isProfileAlreadyReported();
    }

    @Override
    public String getWarehouseName() {
        if (connectContext == null) {
            return "";
        }
        return connectContext.getSessionVariable().getWarehouseName();
    }

    @Override
    public long getCurrentWarehouseId() {
        return connectContext.getCurrentWarehouseId();
    }

    @Override
    public String getResourceGroupName() {
        return jobSpec.getResourceGroup() == null ? ResourceGroup.DEFAULT_RESOURCE_GROUP_NAME :
                jobSpec.getResourceGroup().getName();
    }

    private void execShortCircuit() throws StarRocksException {
        shortCircuitExecutor.exec();
    }

    public ResultReceiver getReceiver() {
        return receiver;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }
}
