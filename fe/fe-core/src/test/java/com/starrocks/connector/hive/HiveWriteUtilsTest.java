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

package com.starrocks.connector.hive;

import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_HIVE_TABLE;

public class HiveWriteUtilsTest {
    @Test
    public void testIsS3Url() {
        Assertions.assertTrue(HiveWriteUtils.isS3Url("obs://"));
    }

    @Test
    public void checkLocationProp() {
        Map<String, String> conf = new HashMap<>();
        conf.put("external_location", "xxx");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Can't create non-managed Hive table. Only supports creating hive table under Database location. " +
                        "You could execute command without external_location properties",
                () -> HiveWriteUtils.checkLocationProperties(conf));
    }

    @Test
    public void testPathExists() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to check path",
                () -> HiveWriteUtils.pathExists(path, new Configuration()));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        Assertions.assertFalse(HiveWriteUtils.pathExists(path, new Configuration()));
    }

    @Test
    public void testIsDirectory() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed checking path",
                () -> HiveWriteUtils.isDirectory(path, new Configuration()));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        Assertions.assertFalse(HiveWriteUtils.isDirectory(path, new Configuration()));
    }

    @Test
    public void testCreateDirectory() {
        Path path = new Path("hdfs://127.0.0.1:9000/user/hive/warehouse/db");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to create directory",
                () -> HiveWriteUtils.createDirectory(path, new Configuration()));

        new MockUp<FileSystem>() {
            @Mock
            public FileSystem get(URI uri, Configuration conf) {
                return new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
            }
        };
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to create directory",
                () -> HiveWriteUtils.createDirectory(path, new Configuration()));
    }

    @Test
    public void testFileCreateByQuery() {
        Assertions.assertFalse(HiveWriteUtils.fileCreatedByQuery("000000_0", "aaaa-bbbb"));
    }
}
