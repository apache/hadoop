/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.federation.router.async;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

import static org.apache.hadoop.hdfs.protocol.SnapshotDiffReport.DiffType.MODIFY;
import static org.apache.hadoop.hdfs.server.federation.router.async.utils.AsyncUtil.syncReturn;
import static org.junit.Assert.assertEquals;

/**
 * Used to test the functionality of {@link RouterAsyncSnapshot}.
 */
public class TestRouterAsyncSnapshot extends RouterAsyncProtocolTestBase {
  private final String testFile = "/testdir/testSnapshot.file";
  private FileSystem routerFs;
  private RouterAsyncSnapshot asyncSnapshot;

  @Before
  public void setup() throws IOException {
    routerFs = getRouterFs();
    asyncSnapshot = new RouterAsyncSnapshot(getRouterAsyncRpcServer());
    FSDataOutputStream fsDataOutputStream = routerFs.create(
        new Path(testFile), true);
    fsDataOutputStream.write(new byte[1024]);
    fsDataOutputStream.close();
  }

  @Test
  public void testRouterAsyncSnapshot() throws Exception {
    asyncSnapshot.allowSnapshot("/testdir");
    syncReturn(null);
    asyncSnapshot.createSnapshot("/testdir", "testdirSnapshot");
    String snapshotName = syncReturn(String.class);
    assertEquals("/testdir/.snapshot/testdirSnapshot", snapshotName);
    asyncSnapshot.getSnapshottableDirListing();
    SnapshottableDirectoryStatus[] snapshottableDirectoryStatuses =
        syncReturn(SnapshottableDirectoryStatus[].class);
    assertEquals(1, snapshottableDirectoryStatuses.length);
    asyncSnapshot.getSnapshotListing("/testdir");
    SnapshotStatus[] snapshotStatuses = syncReturn(SnapshotStatus[].class);
    assertEquals(1, snapshotStatuses.length);

    FSDataOutputStream fsDataOutputStream = routerFs.append(
        new Path("/testdir/testSnapshot.file"), true);
    fsDataOutputStream.write(new byte[1024]);
    fsDataOutputStream.close();

    asyncSnapshot.createSnapshot("/testdir", "testdirSnapshot1");
    snapshotName = syncReturn(String.class);
    assertEquals("/testdir/.snapshot/testdirSnapshot1", snapshotName);

    asyncSnapshot.getSnapshotDiffReport("/testdir",
        "testdirSnapshot", "testdirSnapshot1");
    SnapshotDiffReport snapshotDiffReport = syncReturn(SnapshotDiffReport.class);
    assertEquals(MODIFY, snapshotDiffReport.getDiffList().get(0).getType());

    asyncSnapshot.getSnapshotDiffReportListing("/testdir",
        "testdirSnapshot", "testdirSnapshot1", new byte[]{}, -1);
    SnapshotDiffReportListing snapshotDiffReportListing =
        syncReturn(SnapshotDiffReportListing.class);
    assertEquals(1, snapshotDiffReportListing.getModifyList().size());

    LambdaTestUtils.intercept(SnapshotException.class, () -> {
      asyncSnapshot.disallowSnapshot("/testdir");
      syncReturn(null);
    });

    asyncSnapshot.renameSnapshot("/testdir",
        "testdirSnapshot1", "testdirSnapshot2");
    syncReturn(null);

    LambdaTestUtils.intercept(SnapshotException.class,
        "Cannot delete snapshot testdirSnapshot1 from path /testdir",
        () -> {
        asyncSnapshot.deleteSnapshot("/testdir", "testdirSnapshot1");
        syncReturn(null);
      });

    asyncSnapshot.deleteSnapshot("/testdir", "testdirSnapshot2");
    syncReturn(null);

    asyncSnapshot.deleteSnapshot("/testdir", "testdirSnapshot");
    syncReturn(null);

    asyncSnapshot.disallowSnapshot("/testdir");
    syncReturn(null);
  }
}