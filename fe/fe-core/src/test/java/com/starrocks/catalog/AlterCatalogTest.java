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
package com.starrocks.catalog;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.persist.AlterCatalogLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AlterCatalogTest {
    public static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @AfterAll
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testAlter(@Mocked RangerBasePlugin rangerPlugin) throws Exception {

        new Expectations() {
            {
                rangerPlugin.init();
                minTimes = 0;
            }
        };

        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "alter catalog hive0 set (\"type\"  =  \"hudi\");",
                    connectContext), connectContext);
            Assertions.fail();
        } catch (AnalysisException e) {
        }

        try {
            DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                    "alter catalog hive0 set (\"hive.metastore.uris\"  =  \"xx\");",
                    connectContext), connectContext);
            Assertions.fail();
        } catch (DdlException e) {
        }

        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.service.name\"  =  \"hive0\");",
                connectContext), connectContext);
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"enable_cache_list_names\"  =  \"true\");",
                connectContext), connectContext);

        Map<String, Catalog> catalogMap = connectContext.getGlobalStateMgr().getCatalogMgr().getCatalogs();
        Catalog catalog = catalogMap.get("hive0");
        Map<String, String> properties = catalog.getConfig();
        Assertions.assertEquals("hive0", properties.get("ranger.plugin.hive.service.name"));
        Assertions.assertEquals("true", properties.get("enable_cache_list_names"));
    }

    @Test
    public void testReplay(@Mocked RangerBasePlugin rangerPlugin) throws Exception {
        new Expectations() {
            {
                rangerPlugin.init();
                minTimes = 0;
            }
        };

        Map<String, String> properties = new HashMap<>();
        properties.put("ranger.plugin.hive.service.name", "hive0");
        properties.put("enable_cache_list_names", "true");
        AlterCatalogLog log = new AlterCatalogLog("hive0", properties);
        GlobalStateMgr.getCurrentState().getCatalogMgr().replayAlterCatalog(log);

        Map<String, Catalog> catalogMap = connectContext.getGlobalStateMgr().getCatalogMgr().getCatalogs();
        Catalog catalog = catalogMap.get("hive0");
        properties = catalog.getConfig();
        Assertions.assertEquals("hive0", properties.get("ranger.plugin.hive.service.name"));
        Assertions.assertEquals("true", properties.get("enable_cache_list_names"));
    }

    @Test
    public void testPriv() throws Exception {
        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(
                "alter catalog hive0 set (\"ranger.plugin.hive.service.name\"  =  \"hive0\");",
                connectContext);
        Authorizer.check(stmt, connectContext);

        connectContext.setCurrentUserIdentity(new UserIdentity("u1", "%"));
        try {
            Authorizer.check(stmt, connectContext);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Access denied"));
        }
    }
}
