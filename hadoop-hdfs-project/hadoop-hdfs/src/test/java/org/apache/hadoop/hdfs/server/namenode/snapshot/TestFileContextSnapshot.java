/**
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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileContextSnapshot {

  private static final short REPLICATION = 3;
  private static final int BLOCKSIZE = 1024;
  private static final long SEED = 0;
  private Configuration conf;
  private MiniDFSCluster cluster;
  private FileContext fileContext;
  private DistributedFileSystem dfs;

  private final String snapshotRoot = "/snapshot";
  private final Path filePath = new Path(snapshotRoot, "file1");
  private Path snapRootPath;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fileContext = FileContext.getFileContext(conf);
    dfs = (DistributedFileSystem) cluster.getFileSystem();
    snapRootPath = new Path(snapshotRoot);
    dfs.mkdirs(snapRootPath);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 60000)
  public void testCreateAndDeleteSnapshot() throws Exception {
    DFSTestUtil.createFile(dfs, filePath, BLOCKSIZE, REPLICATION, SEED);
    // disallow snapshot on dir
    dfs.disallowSnapshot(snapRootPath);
    try {
      fileContext.createSnapshot(snapRootPath, "s1");
    } catch (SnapshotException e) {
      GenericTestUtils.assertExceptionContains(
          "Directory is not a snapshottable directory: " + snapRootPath, e);
    }

    // allow snapshot on dir
    dfs.allowSnapshot(snapRootPath);
    Path ssPath = fileContext.createSnapshot(snapRootPath, "s1");
    assertTrue("Failed to create snapshot", dfs.exists(ssPath));
    fileContext.deleteSnapshot(snapRootPath, "s1");
    assertFalse("Failed to delete snapshot", dfs.exists(ssPath));
  }

  /**
   * Test FileStatus of snapshot file before/after rename
   */
  @Test(timeout = 60000)
  public void testRenameSnapshot() throws Exception {
    DFSTestUtil.createFile(dfs, filePath, BLOCKSIZE, REPLICATION, SEED);
    dfs.allowSnapshot(snapRootPath);
    // Create snapshot for sub1
    Path snapPath1 = fileContext.createSnapshot(snapRootPath, "s1");
    Path ssPath = new Path(snapPath1, filePath.getName());
    assertTrue("Failed to create snapshot", dfs.exists(ssPath));
    FileStatus statusBeforeRename = dfs.getFileStatus(ssPath);

    // Rename the snapshot
    fileContext.renameSnapshot(snapRootPath, "s1", "s2");
    // <sub1>/.snapshot/s1/file1 should no longer exist
    assertFalse("Old snapshot still exists after rename!", dfs.exists(ssPath));
    Path snapshotRoot = SnapshotTestHelper.getSnapshotRoot(snapRootPath, "s2");
    ssPath = new Path(snapshotRoot, filePath.getName());

    // Instead, <sub1>/.snapshot/s2/file1 should exist
    assertTrue("Snapshot doesn't exists!", dfs.exists(ssPath));
    FileStatus statusAfterRename = dfs.getFileStatus(ssPath);

    // FileStatus of the snapshot should not change except the path
    assertFalse("Filestatus of the snapshot matches",
        statusBeforeRename.equals(statusAfterRename));
    statusBeforeRename.setPath(statusAfterRename.getPath());
    assertEquals("FileStatus of the snapshot mismatches!",
        statusBeforeRename.toString(), statusAfterRename.toString());
  }
}