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

package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.QueryDebugOptions;
import com.starrocks.sql.optimizer.LogicalPlanPrinter;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import kotlin.text.Charsets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class PlanTestNoneDBBase {
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.show_execution_groups = false;
        // disable checking tablets
        Config.tablet_sched_max_scheduling_tablets = -1;
        Config.alter_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        connectContext.getSessionVariable().setCboEqBaseType(SessionVariableConstants.VARCHAR);
        connectContext.getSessionVariable().setUseCorrelatedPredicateEstimate(false);
        FeConstants.enablePruneEmptyOutputScan = false;
        FeConstants.showJoinLocalShuffleInExplain = false;
        FeConstants.showFragmentCost = false;
        FeConstants.setLengthForVarchar = false;
    }

    @BeforeEach
    public void setUp() {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
    }

    public static void assertContains(String text, String... pattern) {
        if (isIgnoreExplicitColRefIds()) {
            String ignoreExpect = normalizeLogicalPlan(text);
            for (String actual : pattern) {
                String ignoreActual = normalizeLogicalPlan(actual);
                Assertions.assertTrue(ignoreExpect.contains(ignoreActual), text);
            }
        }  else {
            for (String s : pattern) {
                Assertions.assertTrue(text.contains(s), text);
            }
        }
    }

    public static void assertContainsAny(String text, String... pattern) {
        boolean contains = false;
        for (String s : pattern) {
            contains |= text.contains(s);
        }
        if (!contains) {
            Assertions.fail(text);
        }
    }

    private static final String NORMAL_PLAN_PREDICATE_PREFIX = "PREDICATES:";
    private static final String LOWER_NORMAL_PLAN_PREDICATE_PREFIX = "predicates:";
    private static final String LOGICAL_PLAN_SCAN_PREFIX = "SCAN ";
    private static final String LOGICAL_PLAN_PREDICATE_PREFIX = " predicate";

    private static String normalizeLogicalPlanPredicate(String predicate) {
        if (predicate.startsWith(LOGICAL_PLAN_SCAN_PREFIX) && predicate.contains(LOGICAL_PLAN_PREDICATE_PREFIX)) {
            String[] splitArray = predicate.split(LOGICAL_PLAN_PREDICATE_PREFIX);
            Preconditions.checkArgument(splitArray.length == 2);
            String first = splitArray[0];
            String second = splitArray[1];
            String predicates = second.substring(1, second.length() - 2);
            StringBuilder sb = new StringBuilder();
            sb.append(first);
            sb.append(LOGICAL_PLAN_PREDICATE_PREFIX + "[");
            // FIXME: This is only used for normalize not for the final result.
            String sorted = Arrays.stream(predicates.split(" AND "))
                    .map(p -> Arrays.stream(p.split(" OR ")).sorted().collect(Collectors.joining(" OR ")))
                    .sorted()
                    .collect(Collectors.joining(" AND "));
            sb.append(sorted);
            sb.append("])");
            return sb.toString();
        } else if (predicate.contains("PREDICATES: ") && predicate.contains(" IN ")) {
            // normalize in predicate values' order
            String[] splitArray = predicate.split(" IN ");
            if (splitArray.length != 2) {
                return predicate;
            }
            String first = splitArray[0];
            String second = splitArray[1];
            String predicates = second.substring(1, second.length() - 1);
            String sorted = Arrays.stream(predicates.split(", ")).sorted().collect(Collectors.joining(","));
            StringBuilder sb = new StringBuilder();
            sb.append(first);
            sb.append(" IN (");
            sb.append(sorted);
            sb.append(")");
            return sb.toString();
        } else {
            return predicate;
        }
    }

    private static String normalizeLogicalPlan(String plan) {
        return Stream.of(plan.split("\n"))
                .filter(s -> !s.contains("tabletList"))
                .map(str -> str.replaceAll("\\d+:", "col\\$:").trim())
                .map(str -> str.replaceAll("\\[\\d+]", "[col\\$]").trim())
                .map(str -> str.replaceAll("\\[\\d+, \\d+]", "[col\\$, col\\$]").trim())
                .map(str -> str.replaceAll("\\[\\d+, \\d+, \\d+]", "[col\\$, col\\$, col\\$]").trim())
                .map(str -> normalizeLogicalPlanPredicate(str))
                .collect(Collectors.joining("\n"));
    }

    private static String normalizeNormalPlanSeparator(String predicate, String sep) {
        String[] predicates = predicate.split(sep);
        Preconditions.checkArgument(predicates.length == 2);
        return predicates[0] + sep + Arrays.stream(predicates[1].split(",")).sorted().collect(Collectors.joining(","));
    }

    public static String normalizeNormalPlanPredicate(String predicate) {
        if (predicate.contains(NORMAL_PLAN_PREDICATE_PREFIX)) {
            return normalizeNormalPlanSeparator(predicate, NORMAL_PLAN_PREDICATE_PREFIX);
        } else if (predicate.contains(LOWER_NORMAL_PLAN_PREDICATE_PREFIX)) {
            return normalizeNormalPlanSeparator(predicate, LOWER_NORMAL_PLAN_PREDICATE_PREFIX);
        } else {
            return predicate;
        }
    }

    public static String normalizeNormalPlan(String plan) {
        return Stream.of(plan.split("\n")).filter(s -> !s.contains("tabletList"))
                .map(str -> str.replaceAll("\\d+: ", "col\\$: ").trim())
                .map(str -> normalizeNormalPlanPredicate(str))
                .collect(Collectors.joining("\n"));
    }

    public static void assertContainsIgnoreColRefs(String text, String... pattern) {
        String normT = normalizeNormalPlan(text);
        for (String s : pattern) {
            // If pattern contains multi lines, only check line by line.
            String normS = normalizeNormalPlan(s);
            for (String line : normS.split("\n")) {
                Assertions.assertTrue(normT.contains(line), text);
            }
        }
    }

    public static void assertContainsCTEReuse(String sql) throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(100000);
        String plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
        assertContains(plan, "  MultiCastDataSinks");
    }

    public static void assertMatches(String text, String pattern) {
        Pattern regex = Pattern.compile(pattern);
        Assertions.assertTrue(regex.matcher(text).find(), text);
    }

    public static void assertNotMatches(String text, String pattern) {
        Pattern regex = Pattern.compile(pattern);
        Assertions.assertFalse(regex.matcher(text).find(), text);
    }

    public static void assertContains(String text, List<String> patterns) {
        for (String s : patterns) {
            Assertions.assertTrue(text.contains(s), s + "\n" + text);
        }
    }

    public void assertCContains(String text, String... pattern) {
        for (String s : pattern) {
            Assertions.assertTrue(text.contains(s), text);
        }
    }

    public static void assertNotContains(String text, String pattern) {
        Assertions.assertFalse(text.contains(pattern), text);
    }

    public static void assertNotContains(String text, String... pattern) {
        for (String s : pattern) {
            Assertions.assertFalse(text.contains(s), text);
        }
    }

    public static void setTableStatistics(OlapTable table, long rowCount) {
        for (Partition partition : table.getAllPartitions()) {
            partition.getDefaultPhysicalPartition().getBaseIndex().setRowCount(rowCount);
        }
    }

    public static void setPartitionStatistics(OlapTable table, String partitionName, long rowCount) {
        for (Partition partition : table.getAllPartitions()) {
            if (partition.getName().equals(partitionName)) {
                partition.getDefaultPhysicalPartition().getBaseIndex().setRowCount(rowCount);
            }
        }
    }

    public ExecPlan getExecPlan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
    }

    public String getFragmentPlan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.NORMAL);
    }

    public String getFragmentPlan(String sql, String traceModule) throws Exception {
        Pair<String, Pair<ExecPlan, String>> result =
                UtFrameUtils.getFragmentPlanWithTrace(connectContext, sql, traceModule);
        Pair<ExecPlan, String> execPlanWithQuery = result.second;
        String traceLog = execPlanWithQuery.second;
        if (!Strings.isNullOrEmpty(traceLog)) {
            System.out.println(traceLog);
        }
        return execPlanWithQuery.first.getExplainString(TExplainLevel.NORMAL);
    }

    public String getLogicalFragmentPlan(String sql) throws Exception {
        return LogicalPlanPrinter.print(UtFrameUtils.getPlanAndFragment(
                connectContext, sql).second.getPhysicalPlan());
    }

    public String getVerboseExplain(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.VERBOSE);
    }

    public String getCostExplain(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.
                getExplainString(TExplainLevel.COSTS);
    }

    public String getDumpString(String sql) throws Exception {
        UtFrameUtils.getPlanAndFragment(connectContext, sql);
        return GsonUtils.GSON.toJson(connectContext.getDumpInfo());
    }

    public String getThriftPlan(String sql) throws Exception {
        return UtFrameUtils.getPlanThriftString(connectContext, sql);
    }

    public String getDescTbl(String sql) throws Exception {
        return UtFrameUtils.getThriftDescTbl(connectContext, sql);
    }

    public static int getPlanCount(String sql) throws Exception {
        connectContext.getSessionVariable().setUseNthExecPlan(1);
        int planCount = UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getPlanCount();
        connectContext.getSessionVariable().setUseNthExecPlan(0);
        return planCount;
    }

    public String getSQLFile(String filename) {
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + filename + ".sql");

        String sql;
        try (BufferedReader re = new BufferedReader(new FileReader(file))) {
            sql = re.lines().collect(Collectors.joining());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return sql;
    }

    public void runFileUnitTest(String sqlBase, String filename, boolean debug) {
        List<Throwable> errorCollector = Lists.newArrayList();
        String path = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath();
        File file = new File(path + "/" + filename + ".sql");

        String mode = "";
        String tempStr;
        int nth = StringUtils.isBlank(sqlBase) ? 0 : -1;

        StringBuilder sql = new StringBuilder(sqlBase);
        StringBuilder result = new StringBuilder();
        StringBuilder fragment = new StringBuilder();
        StringBuilder comment = new StringBuilder();
        StringBuilder fragmentStatistics = new StringBuilder();
        StringBuilder dumpInfoString = new StringBuilder();
        StringBuilder planEnumerate = new StringBuilder();
        StringBuilder exceptString = new StringBuilder();
        StringBuilder schedulerString = new StringBuilder();

        boolean isDebug = debug;
        boolean isComment = false;
        boolean hasResult = false;
        boolean hasFragment = false;
        boolean hasFragmentStatistics = false;
        boolean isDump = false;
        boolean isEnumerate = false;
        boolean hasScheduler = false;
        int planCount = -1;

        File debugFile = new File(file.getPath() + ".debug");
        BufferedWriter writer = null;

        if (isDebug) {
            try {
                FileUtils.write(debugFile, "", StandardCharsets.UTF_8);
                writer = new BufferedWriter(new FileWriter(debugFile, true));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("DEBUG MODE!");
            System.out.println("DEBUG FILE: " + debugFile.getPath());
        }

        Pattern regex = Pattern.compile("\\[plan-(\\d+)]");
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while ((tempStr = reader.readLine()) != null) {
                if (tempStr.startsWith("/*")) {
                    isComment = true;
                    comment.append(tempStr).append("\n");
                }
                if (tempStr.endsWith("*/")) {
                    isComment = false;
                    comment.append(tempStr).append("\n");
                    continue;
                }

                if (isComment || tempStr.startsWith("//")) {
                    comment.append(tempStr);
                    continue;
                }

                Matcher m = regex.matcher(tempStr);
                if (m.find()) {
                    isEnumerate = true;
                    planEnumerate = new StringBuilder();
                    mode = "enum";
                    nth = Integer.parseInt(m.group(1));
                    connectContext.getSessionVariable().setUseNthExecPlan(nth);
                    continue;
                }

                switch (tempStr) {
                    case "[debug]":
                        isDebug = true;
                        // will create new file
                        if (null == writer) {
                            writer = new BufferedWriter(new FileWriter(debugFile, true));
                            System.out.println("DEBUG MODE!");
                        }
                        continue;
                    case "[planCount]":
                        mode = "planCount";
                        continue;
                    case "[sql]":
                        sql = new StringBuilder();
                        mode = "sql";
                        nth = 0;
                        continue;
                    case "[result]":
                        result = new StringBuilder();
                        mode = "result";
                        hasResult = true;
                        continue;
                    case "[fragment]":
                        fragment = new StringBuilder();
                        mode = "fragment";
                        hasFragment = true;
                        continue;
                    case "[fragment statistics]":
                        fragmentStatistics = new StringBuilder();
                        mode = "fragment statistics";
                        hasFragmentStatistics = true;
                        continue;
                    case "[dump]":
                        dumpInfoString = new StringBuilder();
                        mode = "dump";
                        isDump = true;
                        continue;
                    case "[except]":
                        exceptString = new StringBuilder();
                        mode = "except";
                        continue;
                    case "[scheduler]":
                        schedulerString = new StringBuilder();
                        hasScheduler = true;
                        mode = "scheduler";
                        continue;
                    case "[end]":
                        if (executeSqlByMode(sql, nth, comment, exceptString,
                                hasResult, result,
                                hasFragment, fragment,
                                hasFragmentStatistics, fragmentStatistics,
                                isDump, dumpInfoString,
                                hasScheduler, schedulerString,
                                isEnumerate, planCount, planEnumerate,
                                isDebug, writer, errorCollector)) {
                            continue;
                        }

                        hasResult = false;
                        hasFragment = false;
                        hasFragmentStatistics = false;
                        isDump = false;
                        hasScheduler = false;
                        comment = new StringBuilder();
                        continue;
                }

                switch (mode) {
                    case "sql":
                        sql.append(tempStr).append("\n");
                        break;
                    case "planCount":
                        planCount = Integer.parseInt(tempStr);
                        break;
                    case "result":
                        result.append(tempStr).append("\n");
                        break;
                    case "fragment":
                        fragment.append(tempStr.trim()).append("\n");
                        break;
                    case "fragment statistics":
                        fragmentStatistics.append(tempStr.trim()).append("\n");
                        break;
                    case "dump":
                        dumpInfoString.append(tempStr).append("\n");
                        break;
                    case "enum":
                        planEnumerate.append(tempStr).append("\n");
                        break;
                    case "except":
                        exceptString.append(tempStr);
                        break;
                    case "scheduler":
                        schedulerString.append(tempStr).append("\n");
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println(sql);
            e.printStackTrace();
            Assertions.fail();
        }

        if (CollectionUtils.isNotEmpty(errorCollector)) {
            StringJoiner joiner = new StringJoiner("\n");
            errorCollector.stream().forEach(e -> joiner.add(e.getMessage()));
            Assertions.fail(joiner.toString());
        }
    }

    public void runFileUnitTest(String filename, boolean debug) {
        runFileUnitTest("", filename, debug);
    }

    public void runFileUnitTest(String filename) {
        runFileUnitTest(filename, false);
    }

    public void runFileUnitTest(String sql, String resultFile) {
        runFileUnitTest(sql, resultFile, false);
    }

    private boolean executeSqlByMode(StringBuilder sql, int nth, StringBuilder comment,
                                     StringBuilder exceptString,
                                     boolean hasResult, StringBuilder result,
                                     boolean hasFragment, StringBuilder fragment,
                                     boolean hasFragmentStatistics, StringBuilder fragmentStatistics,
                                     boolean isDump, StringBuilder dumpInfoString,
                                     boolean hasScheduler, StringBuilder schedulerString,
                                     boolean isEnumerate, int planCount, StringBuilder planEnumerate,
                                     boolean isDebug, BufferedWriter debugWriter,
                                     List<Throwable> errorCollector) throws Exception {
        Pair<String, Pair<ExecPlan, String>> pair = null;
        QueryDebugOptions debugOptions = connectContext.getSessionVariable().getQueryDebugOptions();
        String logModule = debugOptions.isEnableQueryTraceLog() ? "MV" : "";
        try {
            pair = UtFrameUtils.getFragmentPlanWithTrace(connectContext, sql.toString(), logModule);
        } catch (Exception ex) {
            if (!exceptString.toString().isEmpty()) {
                Assertions.assertEquals(exceptString.toString(), ex.getMessage());
                return true;
            }
            ex.printStackTrace();
            Assertions.fail("Planning failed, message: " + ex.getMessage() + ", sql: " + sql);
        }

        try {
            String fra = null;
            String statistic = null;
            String dumpStr = null;
            String actualSchedulerPlan = null;

            ExecPlan execPlan = pair.second.first;
            if (debugOptions.isEnableQueryTraceLog()) {
                System.out.println(pair.second.second);
            }
            if (hasResult && !isDebug) {
                checkWithIgnoreTabletList(result.toString().trim(), pair.first.trim());
            }
            if (hasFragment) {
                fra = execPlan.getExplainString(TExplainLevel.NORMAL);
                if (!isDebug) {
                    fra = format(fra);
                    checkWithIgnoreTabletList(fragment.toString().trim(), fra.trim());
                }
            }
            if (hasFragmentStatistics) {
                statistic = format(execPlan.getExplainString(TExplainLevel.COSTS));
                if (!isDebug) {
                    checkWithIgnoreTabletList(fragmentStatistics.toString().trim(), statistic.trim());
                }
            }
            if (isDump) {
                dumpStr = Stream.of(toPrettyFormat(getDumpString(sql.toString())).split("\n"))
                        .filter(s -> !s.contains("\"session_variables\""))
                        .collect(Collectors.joining("\n"));
                if (!isDebug) {
                    Assertions.assertEquals(dumpInfoString.toString().trim(), dumpStr.trim());
                }
            }
            if (hasScheduler) {
                try {
                    actualSchedulerPlan =
                            UtFrameUtils.getPlanAndStartScheduling(connectContext, sql.toString()).first;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    if (!exceptString.toString().isEmpty()) {
                        Assertions.assertEquals(exceptString.toString(), ex.getMessage());
                        return true;
                    }
                    Assertions.fail("Scheduling failed, message: " + ex.getMessage() + ", sql: " + sql);
                }

                if (!isDebug) {
                    checkSchedulerPlan(schedulerString.toString(), actualSchedulerPlan);
                }
            }
            if (isDebug) {
                debugSQL(debugWriter, hasResult, hasFragment, isDump, hasFragmentStatistics, hasScheduler, nth,
                        sql.toString(), pair.first, fra, dumpStr, statistic, comment.toString(),
                        actualSchedulerPlan);
            }
            if (isEnumerate) {
                Assertions.assertEquals(planCount, execPlan.getPlanCount(), "plan count mismatch");
                checkWithIgnoreTabletList(planEnumerate.toString().trim(), pair.first.trim());
                connectContext.getSessionVariable().setUseNthExecPlan(0);
            }
        } catch (Error error) {
            StringBuilder message = new StringBuilder();
            message.append(nth).append(" plan ").append("\n").append(sql).append("\n").append(error.getMessage());
            errorCollector.add(new Throwable(message.toString(), error));
        }
        return false;
    }

    public static String format(String result) {
        StringBuilder sb = new StringBuilder();
        Arrays.stream(result.split("\n")).forEach(d -> sb.append(d.trim()).append("\n"));
        return sb.toString().trim();
    }

    private void debugSQL(BufferedWriter writer, boolean hasResult, boolean hasFragment, boolean hasDump,
                          boolean hasStatistics, boolean hasScheduler, int nthPlan, String sql, String plan,
                          String fragment,
                          String dump,
                          String statistic,
                          String comment,
                          String actualSchedulerPlan) {
        try {
            if (!comment.trim().isEmpty()) {
                writer.append(comment).append("\n");
            }
            if (nthPlan == 0) {
                writer.append("[sql]\n");
                writer.append(sql.trim());
            }

            if (hasResult) {
                writer.append("\n[result]\n");
                writer.append(plan);
            }
            if (nthPlan > 0) {
                writer.append("\n[plan-").append(String.valueOf(nthPlan)).append("]\n");
                writer.append(plan);
            }

            if (hasScheduler) {
                writer.append("\n[scheduler]\n");
                writer.append(actualSchedulerPlan);
            }

            if (hasFragment) {
                writer.append("\n[fragment]\n");
                writer.append(fragment.trim());
            }

            if (hasStatistics) {
                writer.append("\n[fragment statistics]\n");
                writer.append(statistic.trim());
            }

            if (hasDump) {
                writer.append("\n[dump]\n");
                writer.append(dump.trim());
            }

            writer.append("\n[end]\n\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String toPrettyFormat(String json) {
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(jsonObject);
    }

    /**
     * Whether ignore explicit column ref ids when checking the expected plans.
     */
    public static boolean isIgnoreExplicitColRefIds() {
        return false;
    }

    private void checkSchedulerPlan(String expect, String actual) {
        String[] expectedLines = expect.trim().split("\n");
        String[] actualLines = actual.trim().split("\n");

        int ei = 0;
        int ai = 0;

        while (ei < expectedLines.length && ai < actualLines.length) {
            String eline = expectedLines[ei];
            String aline = actualLines[ai];
            ei++;
            ai++;
            Assertions.assertEquals(eline, aline, actual);
            if ("INSTANCES".equals(eline.trim())) {
                // The instances of the fragment may be in random order,
                // so we need to extract each instance and check if they have exactly the same elements in any order.
                Map<Long, String> eInstances = Maps.newHashMap();
                Map<Long, String> aInstances = Maps.newHashMap();
                ei = extractInstancesFromSchedulerPlan(expectedLines, ei, eInstances);
                ai = extractInstancesFromSchedulerPlan(actualLines, ai, aInstances);
                assertThat(aInstances).withFailMessage("actual=[" + actual + "], expect=[" + expect + "]")
                        .containsExactlyInAnyOrderEntriesOf(eInstances);
            }
        }
        Assertions.assertEquals(ei, ai);
    }

    private static int extractInstancesFromSchedulerPlan(String[] lines, int startIndex, Map<Long, String> instances) {
        int i = startIndex;
        long beId = -1;
        for (; i < lines.length; i++) {
            String line = lines[i];
            String trimLine = line.trim();
            if (trimLine.isEmpty()) { // The profile Fragment is coming to the end.
                break;
            } else if (trimLine.startsWith("INSTANCE(")) { // Start a new instance.
                if (beId != -1) {
                    instances.put(beId / 10, beId / 10 + "");
                    beId = -1;
                }
            } else { // Still in this instance.
                Pattern beIdPattern = Pattern.compile("^\\s*BE: (\\d+)$");
                Matcher matcher = beIdPattern.matcher(line);

                if (matcher.find()) {
                    beId = Long.parseLong(matcher.group(1));
                }
            }
        }

        if (beId != -1) {
            // ignore comparing the BE id
            instances.put(beId / 10, beId / 10 + "");
        }

        return i;
    }

    private void checkWithIgnoreTabletList(String expect, String actual) {
        if (isIgnoreExplicitColRefIds()) {
            String ignoreExpect = normalizeLogicalPlan(expect);
            String ignoreActual = normalizeLogicalPlan(actual);
            Assertions.assertEquals(ignoreExpect, ignoreActual, actual);
        } else {
            expect = Stream.of(expect.split("\n")).
                    filter(s -> !s.contains("tabletList")).collect(Collectors.joining("\n"));
            expect = Stream.of(expect.split("\n")).filter(s -> !s.contains("tabletList"))
                    .collect(Collectors.joining("\n"));
            actual = Stream.of(actual.split("\n")).filter(s -> !s.contains("tabletList"))
                    .collect(Collectors.joining("\n"));
            Assertions.assertEquals(expect, actual);
        }
    }

    protected void assertPlanContains(String sql, String... explain) throws Exception {
        String explainString = getFragmentPlan(sql);

        for (String expected : explain) {
            Assertions.assertTrue(StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected),
                    "expected is:\n" + expected + "\n but plan is \n" + explainString);
        }
    }

    protected void assertLogicalPlanContains(String sql, String... explain) throws Exception {
        String explainString = getLogicalFragmentPlan(sql);

        for (String expected : explain) {
            Assertions.assertTrue(StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected),
                    "expected is: " + expected + " but plan is \n" + explainString);
        }
    }

    protected void assertVerbosePlanContains(String sql, String... explain) throws Exception {
        String explainString = getVerboseExplain(sql);

        for (String expected : explain) {
            Assertions.assertTrue(StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected),
                    "expected is: " + expected + " but plan is \n" + explainString);
        }
    }

    protected void assertVerbosePlanNotContains(String sql, String... explain) throws Exception {
        String explainString = getVerboseExplain(sql);

        for (String expected : explain) {
            Assertions.assertFalse(StringUtils.containsIgnoreCase(explainString.toLowerCase(), expected),
                    "expected is: " + expected + " but plan is \n" + explainString);
        }
    }

    protected void assertExceptionMsgContains(String sql, String message) {
        try {
            getFragmentPlan(sql);
            throw new Error();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains(message), e.getMessage());
        }
    }

    public Table getTable(String t) {
        GlobalStateMgr globalStateMgr = starRocksAssert.getCtx().getGlobalStateMgr();
        return globalStateMgr.getLocalMetastore().getDb("test").getTable(t);
    }

    public OlapTable getOlapTable(String t) {
        return (OlapTable) getTable(t);
    }

    public static List<Arguments> zipSqlAndPlan(List<String> sqls, List<String> plans) {
        Preconditions.checkState(sqls.size() == plans.size(), "sqls and plans should have same size");
        List<Arguments> arguments = Lists.newArrayList();
        for (int i = 0; i < sqls.size(); i++) {
            arguments.add(Arguments.of(sqls.get(i), plans.get(i)));
        }
        return arguments;
    }

    protected static void createTables(String dirName, List<String> fileNames) {
        getSqlList(dirName, fileNames).forEach(createTblSql -> {
            System.out.println("create table sql:" + createTblSql);
            try {
                starRocksAssert.withTable(createTblSql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected static void createMaterializedViews(String dirName, List<String> fileNames) {
        getSqlList(dirName, fileNames).forEach(sql -> {
            System.out.println("create mv sql:" + sql);
            try {
                starRocksAssert.withMaterializedView(sql);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected static List<String> getSqlList(String dirName, List<String> fileNames) {
        ClassLoader loader = PlanTestBase.class.getClassLoader();
        List<String> createTableSqlList = fileNames.stream().map(n -> {
            System.out.println("file name:" + n);
            try {
                return CharStreams.toString(
                        new InputStreamReader(
                                Objects.requireNonNull(loader.getResourceAsStream(dirName + n + ".sql")),
                                Charsets.UTF_8));
            } catch (Throwable e) {
                return null;
            }
        }).collect(Collectors.toList());
        Assertions.assertFalse(createTableSqlList.contains(null));
        return createTableSqlList;
    }

    public static String getFileContent(String fileName) throws Exception {
        ClassLoader loader = PlanTestNoneDBBase.class.getClassLoader();
        System.out.println("file name:" + fileName);
        String content = "";
        try {
            content = CharStreams.toString(
                    new InputStreamReader(
                            Objects.requireNonNull(loader.getResourceAsStream(fileName)),
                            Charsets.UTF_8));
        } catch (Throwable e) {
            throw e;
        }
        return content;
    }

    protected static void executeSqlFile(String fileName) throws Exception {
        String sql = getFileContent(fileName);
        List<StatementBase> statements = SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode());
        for (StatementBase stmt : statements) {
            StmtExecutor stmtExecutor = StmtExecutor.newInternalExecutor(connectContext, stmt);
            stmtExecutor.execute();
            Assertions.assertEquals("", connectContext.getState().getErrorMessage());
        }
    }
}
