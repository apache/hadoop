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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotTestHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Make sure we correctly update the quota usage for truncate.
 * We need to cover the following cases:
 * 1. No snapshot, truncate to 0
 * 2. No snapshot, truncate at block boundary
 * 3. No snapshot, not on block boundary
 * 4~6. With snapshot, all the current blocks are included in latest
 *      snapshots, repeat 1~3
 * 7~9. With snapshot, blocks in the latest snapshot and blocks in the current
 *      file diverged, repeat 1~3
 */
public class TestTruncateQuotaUpdate {
  private static final int BLOCKSIZE = 1024;
  private static final short REPLICATION = 4;
  private static final long DISKQUOTA = BLOCKSIZE * 20;
  static final long seed = 0L;
  private static final Path dir = new Path("/TestTruncateQuotaUpdate");
  private static final Path file = new Path(dir, "file");

  private MiniDFSCluster cluster;
  private FSDirectory fsdir;
  private DistributedFileSystem dfs;

  @Before
  public void setUp() throws Exception {
    final Configuration conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION)
        .build();
    cluster.waitActive();

    fsdir = cluster.getNamesystem().getFSDirectory();
    dfs = cluster.getFileSystem();

    dfs.mkdirs(dir);
    dfs.setQuota(dir, Long.MAX_VALUE - 1, DISKQUOTA);
    dfs.setQuotaByStorageType(dir, StorageType.DISK, DISKQUOTA);
    dfs.setStoragePolicy(dir, HdfsConstants.HOT_STORAGE_POLICY_NAME);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testTruncateQuotaUpdate() throws Exception {

  }

  public interface TruncateCase {
    public void prepare() throws Exception;
    public void run() throws Exception;
  }

  private void testTruncate(long newLength, long expectedDiff,
      long expectedUsage) throws Exception {
    // before doing the real truncation, make sure the computation is correct
    final INodesInPath iip = fsdir.getINodesInPath4Write(file.toString());
    final INodeFile fileNode = iip.getLastINode().asFile();
    fileNode.recordModification(iip.getLatestSnapshotId(), true);
    final long diff = fileNode.computeQuotaDeltaForTruncate(newLength);
    Assert.assertEquals(expectedDiff, diff);

    // do the real truncation
    dfs.truncate(file, newLength);
    // wait for truncate to finish
    TestFileTruncate.checkBlockRecovery(file, dfs);
    final INodeDirectory dirNode = fsdir.getINode4Write(dir.toString())
        .asDirectory();
    final long spaceUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getStorageSpace();
    final long diskUsed = dirNode.getDirectoryWithQuotaFeature()
        .getSpaceConsumed().getTypeSpaces().get(StorageType.DISK);
    Assert.assertEquals(expectedUsage, spaceUsed);
    Assert.assertEquals(expectedUsage, diskUsed);
  }

  /**
   * case 1~3
   */
  private class TruncateWithoutSnapshot implements TruncateCase {
    @Override
    public void prepare() throws Exception {
      // original file size: 2.5 block
      DFSTestUtil.createFile(dfs, file, BLOCKSIZE * 2 + BLOCKSIZE / 2,
          REPLICATION, 0L);
    }

    @Override
    public void run() throws Exception {
      // case 1: first truncate to 1.5 blocks
      long newLength = BLOCKSIZE + BLOCKSIZE / 2;
      // we truncate 1 blocks, but not on the boundary, thus the diff should
      // be -block + (block - 0.5 block) = -0.5 block
      long diff = -BLOCKSIZE / 2;
      // the new quota usage should be BLOCKSIZE * 1.5 * replication
      long usage = (BLOCKSIZE + BLOCKSIZE / 2) * REPLICATION;
      testTruncate(newLength, diff, usage);

      // case 2: truncate to 1 block
      newLength = BLOCKSIZE;
      // the diff should be -0.5 block since this is not on boundary
      diff = -BLOCKSIZE / 2;
      // after truncation the quota usage should be BLOCKSIZE * replication
      usage = BLOCKSIZE * REPLICATION;
      testTruncate(newLength, diff, usage);

      // case 3: truncate to 0
      testTruncate(0, -BLOCKSIZE, 0);
    }
  }

  /**
   * case 4~6
   */
  private class TruncateWithSnapshot implements TruncateCase {
    @Override
    public void prepare() throws Exception {
      DFSTestUtil.createFile(dfs, file, BLOCKSIZE * 2 + BLOCKSIZE / 2,
          REPLICATION, 0L);
      SnapshotTestHelper.createSnapshot(dfs, dir, "s1");
    }

    @Override
    public void run() throws Exception {
      // case 4: truncate to 1.5 blocks
      long newLength = BLOCKSIZE + BLOCKSIZE / 2;
      // all the blocks are in snapshot. truncate need to allocate a new block
      // diff should be +BLOCKSIZE
      long diff = BLOCKSIZE;
      // the new quota usage should be BLOCKSIZE * 3 * replication
      long usage = BLOCKSIZE * 3 * REPLICATION;
      testTruncate(newLength, diff, usage);

      // case 5: truncate to 1 block
      newLength = BLOCKSIZE;
      // the block for truncation is not in snapshot, diff should be -0.5 block
      diff = -BLOCKSIZE / 2;
      // after truncation the quota usage should be 2.5 block * repl
      usage = (BLOCKSIZE * 2 + BLOCKSIZE / 2) * REPLICATION;
      testTruncate(newLength, diff, usage);

      // case 6: truncate to 0
      testTruncate(0, 0, usage);
    }
  }

  /**
   * case 7~9
   */
  private class TruncateWithSnapshot2 implements TruncateCase {
    @Override
    public void prepare() throws Exception {
      // original size: 2.5 blocks
      DFSTestUtil.createFile(dfs, file, BLOCKSIZE * 2 + BLOCKSIZE / 2,
          REPLICATION, 0L);
      SnapshotTestHelper.createSnapshot(dfs, dir, "s1");

      // truncate to 1.5 block
      dfs.truncate(file, BLOCKSIZE + BLOCKSIZE / 2);
      TestFileTruncate.checkBlockRecovery(file, dfs);

      // append another 1 BLOCK
      DFSTestUtil.appendFile(dfs, file, BLOCKSIZE);
    }

    @Override
    public void run() throws Exception {
      // case 8: truncate to 2 blocks
      long newLength = BLOCKSIZE * 2;
      // the original 2.5 blocks are in snapshot. the block truncated is not
      // in snapshot. diff should be -0.5 block
      long diff = -BLOCKSIZE / 2;
      // the new quota usage should be BLOCKSIZE * 3.5 * replication
      long usage = (BLOCKSIZE * 3 + BLOCKSIZE / 2) * REPLICATION;
      testTruncate(newLength, diff, usage);

      // case 7: truncate to 1.5 block
      newLength = BLOCKSIZE  + BLOCKSIZE / 2;
      // the block for truncation is not in snapshot, diff should be
      // -0.5 block + (block - 0.5block) = 0
      diff = 0;
      // after truncation the quota usage should be 3 block * repl
      usage = (BLOCKSIZE * 3) * REPLICATION;
      testTruncate(newLength, diff, usage);

      // case 9: truncate to 0
      testTruncate(0, -BLOCKSIZE / 2,
          (BLOCKSIZE * 2 + BLOCKSIZE / 2) * REPLICATION);
    }
  }

  private void testTruncateQuotaUpdate(TruncateCase t) throws Exception {
    t.prepare();
    t.run();
  }

  @Test
  public void testQuotaNoSnapshot() throws Exception {
    testTruncateQuotaUpdate(new TruncateWithoutSnapshot());
  }

  @Test
  public void testQuotaWithSnapshot() throws Exception {
    testTruncateQuotaUpdate(new TruncateWithSnapshot());
  }

  @Test
  public void testQuotaWithSnapshot2() throws Exception {
    testTruncateQuotaUpdate(new TruncateWithSnapshot2());
  }
}
