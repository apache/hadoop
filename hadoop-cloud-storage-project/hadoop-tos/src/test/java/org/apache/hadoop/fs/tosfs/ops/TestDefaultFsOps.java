/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.ops;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.tosfs.RawFileStatus;
import org.apache.hadoop.fs.tosfs.RawFileSystem;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.common.ThreadPools;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.object.ObjectStorage;
import org.apache.hadoop.fs.tosfs.object.ObjectStorageFactory;
import org.apache.hadoop.fs.tosfs.util.TestUtility;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.tosfs.object.tos.TOS.TOS_SCHEME;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class TestDefaultFsOps extends TestBaseFsOps {
  private static ExecutorService threadPool;

  static Stream<Arguments> provideArguments() {
    // Case1: direct rename.
    List<Arguments> values = new ArrayList<>();
    Configuration directRenameConf = new Configuration();
    directRenameConf.setBoolean(ConfKeys.FS_OBJECT_RENAME_ENABLED.key("tos"), true);
    directRenameConf.setBoolean(ConfKeys.FS_ASYNC_CREATE_MISSED_PARENT.key("tos"), false);

    ObjectStorage storage0 =
        ObjectStorageFactory.createWithPrefix(String.format("tos-%s/", UUIDUtils.random()),
            TOS_SCHEME, TestUtility.bucket(), directRenameConf);
    values.add(Arguments.of(
        storage0,
        new DefaultFsOps(storage0, directRenameConf, threadPool, obj -> {
          long modifiedTime = RawFileSystem.dateToLong(obj.mtime());
          String path =
              String.format("%s://%s/%s", storage0.scheme(), storage0.bucket().name(), obj.key());
          return new RawFileStatus(obj.size(), obj.isDir(), 0, modifiedTime, new Path(path), "fake",
              obj.checksum());
        })));

    // Case2: copied rename.
    Configuration copiedRenameConf = new Configuration();
    copiedRenameConf.setLong(ConfKeys.FS_MULTIPART_COPY_THRESHOLD.key("tos"), 1L << 20);
    copiedRenameConf.setBoolean(ConfKeys.FS_ASYNC_CREATE_MISSED_PARENT.key("tos"), false);

    ObjectStorage storage1 =
        ObjectStorageFactory.createWithPrefix(String.format("tos-%s/", UUIDUtils.random()),
            TOS_SCHEME, TestUtility.bucket(), copiedRenameConf);
    values.add(Arguments.of(
        storage1,
        new DefaultFsOps(storage1, copiedRenameConf, threadPool, obj -> {
          long modifiedTime = RawFileSystem.dateToLong(obj.mtime());
          String path =
              String.format("%s://%s/%s", storage1.scheme(), storage1.bucket().name(), obj.key());
          return new RawFileStatus(obj.size(), obj.isDir(), 0, modifiedTime, new Path(path), "fake",
              obj.checksum());
        })));

    return values.stream();
  }

  @BeforeAll
  public static void beforeClass() {
    assumeTrue(TestEnv.checkTestEnabled());
    threadPool = ThreadPools.newWorkerPool("TestDefaultFsHelper-pool");
  }

  @AfterAll
  public static void afterClass() {
    if (!TestEnv.checkTestEnabled()) {
      return;
    }

    if (!threadPool.isShutdown()) {
      threadPool.shutdown();
    }
  }
}
