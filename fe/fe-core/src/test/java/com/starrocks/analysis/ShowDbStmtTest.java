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

package com.starrocks.analysis;

import com.starrocks.catalog.Database;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.Preconditions;

public class ShowDbStmtTest {
    private ConnectContext ctx;
    private GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void setUp() throws Exception {
        ctx = new ConnectContext(null);
        ctx.setCommand(MysqlCommand.COM_SLEEP);


        // mock database
        Database db = new Database();
        new Expectations(db) {
            {
                GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), anyString);
                minTimes = 0;
            }
        };

        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setCurrentUserIdentity(new UserIdentity("testUser", "%"));
        ctx.setQualifiedUser("testCluster:testUser");

        new Expectations(ctx) {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };
    }

    @Test
    public void testNormal() throws Exception {
        ShowDbStmt stmt = new ShowDbStmt(null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertNull(stmt.getPattern());
        Assertions.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assertions.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());

        stmt = new ShowDbStmt("abc");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assertions.assertEquals("abc", stmt.getPattern());
        Assertions.assertEquals(1, stmt.getMetaData().getColumnCount());
        Assertions.assertEquals("Database", stmt.getMetaData().getColumn(0).getName());

        stmt = new ShowDbStmt(null, null, null);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        ShowResultSetMetaData metaData = resultSet.getMetaData();
        Assertions.assertEquals(metaData.getColumn(0).getName(), "Database");

        String sql = "show databases where `database` = 't1'";
        stmt = (ShowDbStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Preconditions.notNull(stmt.toSelectStmt().getOrigStmt(), "stmt's original stmt should not be null");

    }

    @Test
    public void testShowSchemas() throws Exception {
        ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
        String showSQL = "show schemas";
        ShowDbStmt showDbStmt = (ShowDbStmt) UtFrameUtils.parseStmtWithNewParser(showSQL, ctx);
    }

}