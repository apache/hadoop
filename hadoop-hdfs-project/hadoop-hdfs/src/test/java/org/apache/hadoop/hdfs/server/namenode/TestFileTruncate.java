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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.test.LambdaTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.SafeModeAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.FsDatasetTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.event.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileTruncate {
  static {
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, Level.TRACE);
    GenericTestUtils.setLogLevel(FSEditLogLoader.LOG, Level.TRACE);
  }
  static final Logger LOG = LoggerFactory.getLogger(TestFileTruncate.class);
  static final int BLOCK_SIZE = 4;
  static final short REPLICATION = 3;
  static final int DATANODE_NUM = 3;
  static final int SUCCESS_ATTEMPTS = 300;
  static final int RECOVERY_ATTEMPTS = 600;
  static final long SLEEP = 100L;

  static final long LOW_SOFTLIMIT = 100L;
  static final long LOW_HARDLIMIT = 200L;
  static final int SHORT_HEARTBEAT = 1;

  static Configuration conf;
  static MiniDFSCluster cluster;
  static DistributedFileSystem fs;

 private Path parent;

  @Before
  public void setUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, SHORT_HEARTBEAT);
    conf.setLong(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY, 1);
    cluster = new MiniDFSCluster.Builder(conf)
        .format(true)
        .numDataNodes(DATANODE_NUM)
        .waitSafeMode(true)
        .build();
    fs = cluster.getFileSystem();
    parent = new Path("/test");
  }

  @After
  public void tearDown() throws IOException {
    if(fs != null) {
      fs.close();
      fs = null;
    }
    if(cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * Truncate files of different sizes byte by byte.
   */
  @Test
  public void testBasicTruncate() throws IOException {
    int startingFileSize = 3 * BLOCK_SIZE;

    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    for (int fileLength = startingFileSize; fileLength > 0;
                                            fileLength -= BLOCK_SIZE - 1) {
      for (int toTruncate = 0; toTruncate <= fileLength; toTruncate++) {
        final Path p = new Path(parent, "testBasicTruncate" + fileLength);
        writeContents(contents, fileLength, p);

        int newLength = fileLength - toTruncate;
        assertTrue("DFS supports truncate",
            fs.hasPathCapability(p, CommonPathCapabilities.FS_TRUNCATE));
        boolean isReady = fs.truncate(p, newLength);
        LOG.info("fileLength=" + fileLength + ", newLength=" + newLength
            + ", toTruncate=" + toTruncate + ", isReady=" + isReady);

        assertEquals("File must be closed for zero truncate"
            + " or truncating at the block boundary",
            isReady, toTruncate == 0 || newLength % BLOCK_SIZE == 0);
        if (!isReady) {
          checkBlockRecovery(p);
        }

        ContentSummary cs = fs.getContentSummary(parent);
        assertEquals("Bad disk space usage",
            cs.getSpaceConsumed(), newLength * REPLICATION);
        // validate the file content
        checkFullFile(p, newLength, contents);
      }
    }
    fs.delete(parent, true);
  }

  /** Truncate the same file multiple times until its size is zero. */
  @Test
  public void testMultipleTruncate() throws IOException {
    Path dir = new Path("/testMultipleTruncate");
    fs.mkdirs(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[100 * BLOCK_SIZE];
    ThreadLocalRandom.current().nextBytes(data);
    writeContents(data, data.length, p);

    for(int n = data.length; n > 0; ) {
      final int newLength = ThreadLocalRandom.current().nextInt(n);
      assertTrue("DFS supports truncate",
          fs.hasPathCapability(p, CommonPathCapabilities.FS_TRUNCATE));
      final boolean isReady = fs.truncate(p, newLength);
      LOG.info("newLength=" + newLength + ", isReady=" + isReady);
      assertEquals("File must be closed for truncating at the block boundary",
          isReady, newLength % BLOCK_SIZE == 0);
      assertEquals("Truncate is not idempotent",
          isReady, fs.truncate(p, newLength));
      if (!isReady) {
        checkBlockRecovery(p);
      }
      checkFullFile(p, newLength, data);
      n = newLength;
    }

    fs.delete(dir, true);
  }

  /** Truncate the same file multiple times until its size is zero. */
  @Test
  public void testSnapshotTruncateThenDeleteSnapshot() throws IOException {
    Path dir = new Path("/testSnapshotTruncateThenDeleteSnapshot");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[BLOCK_SIZE];
    ThreadLocalRandom.current().nextBytes(data);
    writeContents(data, data.length, p);
    final String snapshot = "s0";
    fs.createSnapshot(dir, snapshot);
    Block lastBlock = getLocatedBlocks(p).getLastLocatedBlock()
        .getBlock().getLocalBlock();
    final int newLength = data.length - 1;
    assert newLength % BLOCK_SIZE != 0 :
        " newLength must not be multiple of BLOCK_SIZE";
    assertTrue("DFS supports truncate",
        fs.hasPathCapability(p, CommonPathCapabilities.FS_TRUNCATE));
    final boolean isReady = fs.truncate(p, newLength);
    LOG.info("newLength=" + newLength + ", isReady=" + isReady);
    assertEquals("File must be closed for truncating at the block boundary",
        isReady, newLength % BLOCK_SIZE == 0);
    fs.deleteSnapshot(dir, snapshot);
    if (!isReady) {
      checkBlockRecovery(p);
    }
    checkFullFile(p, newLength, data);
    assertBlockNotPresent(lastBlock);
    fs.delete(dir, true);
  }

  /**
   * Test truncate twice together on a file.
   */
  @Test(timeout=90000)
  public void testTruncateTwiceTogether() throws Exception {

    Path dir = new Path("/testTruncateTwiceTogether");
    fs.mkdirs(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[100 * BLOCK_SIZE];
    ThreadLocalRandom.current().nextBytes(data);
    writeContents(data, data.length, p);

    DataNodeFaultInjector originInjector = DataNodeFaultInjector.get();
    DataNodeFaultInjector injector = new DataNodeFaultInjector() {
      @Override
      public void delay() {
        try {
          // Bigger than soft lease period.
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          // Ignore
        }
      }
    };
    // Delay to recovery.
    DataNodeFaultInjector.set(injector);

    // Truncate by using different client name.
    Thread t = new Thread(() -> {
      String hdfsCacheDisableKey = "fs.hdfs.impl.disable.cache";
      boolean originCacheDisable =
          conf.getBoolean(hdfsCacheDisableKey, false);
      try {
        conf.setBoolean(hdfsCacheDisableKey, true);
        FileSystem fs1 = FileSystem.get(conf);
        fs1.truncate(p, data.length-1);
        } catch (IOException e) {
          // ignore
        } finally{
          conf.setBoolean(hdfsCacheDisableKey, originCacheDisable);
        }
      });
    t.start();
    t.join();
    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(LOW_SOFTLIMIT, LOW_HARDLIMIT);

    LambdaTestUtils.intercept(RemoteException.class,
        "/testTruncateTwiceTogether/file is being truncated",
        () -> fs.truncate(p, data.length - 2));

    // wait for block recovery
    checkBlockRecovery(p);
    assertFileLength(p, data.length - 1);

    DataNodeFaultInjector.set(originInjector);
    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(HdfsConstants.LEASE_SOFTLIMIT_PERIOD,
            conf.getLong(DFSConfigKeys.DFS_LEASE_HARDLIMIT_KEY,
                DFSConfigKeys.DFS_LEASE_HARDLIMIT_DEFAULT) * 1000);
    fs.delete(dir, true);
  }

  /**
   * Truncate files and then run other operations such as
   * rename, set replication, set permission, etc.
   */
  @Test
  public void testTruncateWithOtherOperations() throws IOException {
    Path dir = new Path("/testTruncateOtherOperations");
    fs.mkdirs(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[2 * BLOCK_SIZE];

    ThreadLocalRandom.current().nextBytes(data);
    writeContents(data, data.length, p);

    final int newLength = data.length - 1;
    boolean isReady = fs.truncate(p, newLength);
    assertFalse(isReady);

    fs.setReplication(p, (short)(REPLICATION - 1));
    fs.setPermission(p, FsPermission.createImmutable((short)0444));

    final Path q = new Path(dir, "newFile");
    fs.rename(p, q);

    checkBlockRecovery(q);
    checkFullFile(q, newLength, data);

    cluster.restartNameNode();
    checkFullFile(q, newLength, data);

    fs.delete(dir, true);
  }

  @Test
  public void testSnapshotWithAppendTruncate()
      throws IOException, InterruptedException {
    testSnapshotWithAppendTruncate(0, 1, 2);
    testSnapshotWithAppendTruncate(0, 2, 1);
    testSnapshotWithAppendTruncate(1, 0, 2);
    testSnapshotWithAppendTruncate(1, 2, 0);
    testSnapshotWithAppendTruncate(2, 0, 1);
    testSnapshotWithAppendTruncate(2, 1, 0);
  }

  /**
   * Create three snapshots with appended and truncated file.
   * Delete snapshots in the specified order and verify that
   * remaining snapshots are still readable.
   */
  void testSnapshotWithAppendTruncate(int... deleteOrder)
      throws IOException, InterruptedException {
    FSDirectory fsDir = cluster.getNamesystem().getFSDirectory();
    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    fs.allowSnapshot(parent);
    String truncateFile = "testSnapshotWithAppendTruncate";
    final Path src = new Path(parent, truncateFile);
    int[] length = new int[4];
    length[0] = 2 * BLOCK_SIZE + BLOCK_SIZE / 2;
    DFSTestUtil.createFile(fs, src, 64, length[0], BLOCK_SIZE, REPLICATION, 0L);
    Block firstBlk = getLocatedBlocks(src).get(0).getBlock().getLocalBlock();
    Path[] snapshotFiles = new Path[4];

    // Diskspace consumed should be 10 bytes * 3. [blk 1,2,3]
    ContentSummary contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(30L));

    // Add file to snapshot and append
    String[] ss = new String[] {"ss0", "ss1", "ss2", "ss3"};
    Path snapshotDir = fs.createSnapshot(parent, ss[0]);
    snapshotFiles[0] = new Path(snapshotDir, truncateFile);
    length[1] = length[2] = length[0] + BLOCK_SIZE + 1;
    DFSTestUtil.appendFile(fs, src, BLOCK_SIZE + 1);
    Block lastBlk = getLocatedBlocks(src).getLastLocatedBlock()
                                         .getBlock().getLocalBlock();

    // Diskspace consumed should be 15 bytes * 3. [blk 1,2,3,4]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(45L));

    // Create another snapshot without changes
    snapshotDir = fs.createSnapshot(parent, ss[1]);
    snapshotFiles[1] = new Path(snapshotDir, truncateFile);

    // Create another snapshot and append
    snapshotDir = fs.createSnapshot(parent, ss[2]);
    snapshotFiles[2] = new Path(snapshotDir, truncateFile);
    DFSTestUtil.appendFile(fs, src, BLOCK_SIZE -1 + BLOCK_SIZE / 2);
    Block appendedBlk = getLocatedBlocks(src).getLastLocatedBlock()
                                             .getBlock().getLocalBlock();

    // Diskspace consumed should be 20 bytes * 3. [blk 1,2,3,4,5]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(60L));

    // Truncate to block boundary
    int newLength = length[0] + BLOCK_SIZE / 2;
    boolean isReady = fs.truncate(src, newLength);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem(0).getBlockManager());
    assertTrue("Recovery is not expected.", isReady);
    assertFileLength(snapshotFiles[2], length[2]);
    assertFileLength(snapshotFiles[1], length[1]);
    assertFileLength(snapshotFiles[0], length[0]);
    assertBlockNotPresent(appendedBlk);
    // Diskspace consumed should be 16 bytes * 3. [blk 1,2,3 SS:4]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(48L));
    // Truncate full block again
    newLength = length[0] - BLOCK_SIZE / 2;
    isReady = fs.truncate(src, newLength);
    assertTrue("Recovery is not expected.", isReady);
    assertFileLength(snapshotFiles[2], length[2]);
    assertFileLength(snapshotFiles[1], length[1]);
    assertFileLength(snapshotFiles[0], length[0]);
    // Diskspace consumed should be 16 bytes * 3. [blk 1,2 SS:3,4]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(48L));
    // Truncate half of the last block
    newLength -= BLOCK_SIZE / 2;
    isReady = fs.truncate(src, newLength);
    assertFalse("Recovery is expected.", isReady);
    checkBlockRecovery(src);
    assertFileLength(snapshotFiles[2], length[2]);
    assertFileLength(snapshotFiles[1], length[1]);
    assertFileLength(snapshotFiles[0], length[0]);
    Block replacedBlk = getLocatedBlocks(src).getLastLocatedBlock()
        .getBlock().getLocalBlock();
    // Diskspace consumed should be 16 bytes * 3. [blk 1,6 SS:2,3,4]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(54L));
    snapshotDir = fs.createSnapshot(parent, ss[3]);
    snapshotFiles[3] = new Path(snapshotDir, truncateFile);
    length[3] = newLength;
    // Delete file. Should still be able to read snapshots
    int numINodes = fsDir.getInodeMapSize();
    isReady = fs.delete(src, false);
    assertTrue("Delete failed.", isReady);
    assertFileLength(snapshotFiles[3], length[3]);
    assertFileLength(snapshotFiles[2], length[2]);
    assertFileLength(snapshotFiles[1], length[1]);
    assertFileLength(snapshotFiles[0], length[0]);
    assertEquals("Number of INodes should not change",
        numINodes, fsDir.getInodeMapSize());
    fs.deleteSnapshot(parent, ss[3]);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem(0).getBlockManager());
    assertBlockExists(firstBlk);
    assertBlockExists(lastBlk);
    assertBlockNotPresent(replacedBlk);
    // Diskspace consumed should be 16 bytes * 3. [SS:1,2,3,4]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(48L));
    // delete snapshots in the specified order
    fs.deleteSnapshot(parent, ss[deleteOrder[0]]);
    assertFileLength(snapshotFiles[deleteOrder[1]], length[deleteOrder[1]]);
    assertFileLength(snapshotFiles[deleteOrder[2]], length[deleteOrder[2]]);
    assertBlockExists(firstBlk);
    assertBlockExists(lastBlk);
    assertEquals("Number of INodes should not change",
        numINodes, fsDir.getInodeMapSize());
    // Diskspace consumed should be 16 bytes * 3. [SS:1,2,3,4]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(48L));
    fs.deleteSnapshot(parent, ss[deleteOrder[1]]);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem(0).getBlockManager());
    assertFileLength(snapshotFiles[deleteOrder[2]], length[deleteOrder[2]]);
    assertBlockExists(firstBlk);
    contentSummary = fs.getContentSummary(parent);
    if(fs.exists(snapshotFiles[0])) {
      // Diskspace consumed should be 0 bytes * 3. [SS:1,2,3]
      assertBlockNotPresent(lastBlk);
      assertThat(contentSummary.getSpaceConsumed(), is(36L));
    } else {
      // Diskspace consumed should be 48 bytes * 3. [SS:1,2,3,4]
      assertThat(contentSummary.getSpaceConsumed(), is(48L));
    }
    assertEquals("Number of INodes should not change",
        numINodes, fsDir .getInodeMapSize());
    fs.deleteSnapshot(parent, ss[deleteOrder[2]]);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem(0).getBlockManager());
    assertBlockNotPresent(firstBlk);
    assertBlockNotPresent(lastBlk);
    // Diskspace consumed should be 0 bytes * 3. []
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(0L));
    assertNotEquals("Number of INodes should change",
        numINodes, fsDir.getInodeMapSize());
  }

  /**
   * Create three snapshots with file truncated 3 times.
   * Delete snapshots in the specified order and verify that
   * remaining snapshots are still readable.
   */
  @Test
  public void testSnapshotWithTruncates()
      throws IOException, InterruptedException {
    testSnapshotWithTruncates(0, 1, 2);
    testSnapshotWithTruncates(0, 2, 1);
    testSnapshotWithTruncates(1, 0, 2);
    testSnapshotWithTruncates(1, 2, 0);
    testSnapshotWithTruncates(2, 0, 1);
    testSnapshotWithTruncates(2, 1, 0);
  }

  void testSnapshotWithTruncates(int... deleteOrder)
      throws IOException, InterruptedException {
    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    fs.allowSnapshot(parent);
    String truncateFile = "testSnapshotWithTruncates";
    final Path src = new Path(parent, truncateFile);
    int[] length = new int[3];
    length[0] = 3 * BLOCK_SIZE;
    DFSTestUtil.createFile(fs, src, 64, length[0], BLOCK_SIZE, REPLICATION, 0L);
    Block firstBlk = getLocatedBlocks(src).get(0).getBlock().getLocalBlock();
    Block lastBlk = getLocatedBlocks(src).getLastLocatedBlock()
                                         .getBlock().getLocalBlock();
    Path[] snapshotFiles = new Path[3];

    // Diskspace consumed should be 12 bytes * 3. [blk 1,2,3]
    ContentSummary contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(36L));

    // Add file to snapshot and append
    String[] ss = new String[] {"ss0", "ss1", "ss2"};
    Path snapshotDir = fs.createSnapshot(parent, ss[0]);
    snapshotFiles[0] = new Path(snapshotDir, truncateFile);
    length[1] = 2 * BLOCK_SIZE;
    boolean isReady = fs.truncate(src, 2 * BLOCK_SIZE);
    assertTrue("Recovery is not expected.", isReady);

    // Diskspace consumed should be 12 bytes * 3. [blk 1,2 SS:3]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(36L));
    snapshotDir = fs.createSnapshot(parent, ss[1]);
    snapshotFiles[1] = new Path(snapshotDir, truncateFile);

    // Create another snapshot with truncate
    length[2] = BLOCK_SIZE + BLOCK_SIZE / 2;
    isReady = fs.truncate(src, BLOCK_SIZE + BLOCK_SIZE / 2);
    assertFalse("Recovery is expected.", isReady);
    checkBlockRecovery(src);
    snapshotDir = fs.createSnapshot(parent, ss[2]);
    snapshotFiles[2] = new Path(snapshotDir, truncateFile);
    assertFileLength(snapshotFiles[0], length[0]);
    assertBlockExists(lastBlk);

    // Diskspace consumed should be 14 bytes * 3. [blk 1,4 SS:2,3]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(42L));

    fs.deleteSnapshot(parent, ss[deleteOrder[0]]);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem(0).getBlockManager());
    assertFileLength(snapshotFiles[deleteOrder[1]], length[deleteOrder[1]]);
    assertFileLength(snapshotFiles[deleteOrder[2]], length[deleteOrder[2]]);
    assertFileLength(src, length[2]);
    assertBlockExists(firstBlk);

    contentSummary = fs.getContentSummary(parent);
    if(fs.exists(snapshotFiles[0])) {
      // Diskspace consumed should be 14 bytes * 3. [blk 1,4 SS:2,3]
      assertThat(contentSummary.getSpaceConsumed(), is(42L));
      assertBlockExists(lastBlk);
    } else {
      // Diskspace consumed should be 10 bytes * 3. [blk 1,4 SS:2]
      assertThat(contentSummary.getSpaceConsumed(), is(30L));
      assertBlockNotPresent(lastBlk);
    }

    fs.deleteSnapshot(parent, ss[deleteOrder[1]]);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem(0).getBlockManager());
    assertFileLength(snapshotFiles[deleteOrder[2]], length[deleteOrder[2]]);
    assertFileLength(src, length[2]);
    assertBlockExists(firstBlk);

    contentSummary = fs.getContentSummary(parent);
    if(fs.exists(snapshotFiles[0])) {
      // Diskspace consumed should be 14 bytes * 3. [blk 1,4 SS:2,3]
      assertThat(contentSummary.getSpaceConsumed(), is(42L));
      assertBlockExists(lastBlk);
    } else if(fs.exists(snapshotFiles[1])) {
      // Diskspace consumed should be 10 bytes * 3. [blk 1,4 SS:2]
      assertThat(contentSummary.getSpaceConsumed(), is(30L));
      assertBlockNotPresent(lastBlk);
    } else {
      // Diskspace consumed should be 6 bytes * 3. [blk 1,4 SS:]
      assertThat(contentSummary.getSpaceConsumed(), is(18L));
      assertBlockNotPresent(lastBlk);
    }

    fs.deleteSnapshot(parent, ss[deleteOrder[2]]);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem(0).getBlockManager());
    assertFileLength(src, length[2]);
    assertBlockExists(firstBlk);

    // Diskspace consumed should be 6 bytes * 3. [blk 1,4 SS:]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(18L));
    assertThat(contentSummary.getLength(), is(6L));

    fs.delete(src, false);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem().getBlockManager());
    assertBlockNotPresent(firstBlk);

    // Diskspace consumed should be 0 bytes * 3. []
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(0L));
  }

  /**
   * Failure / recovery test for truncate.
   * In this failure the DNs fail to recover the blocks and the NN triggers
   * lease recovery.
   * File stays in RecoveryInProgress until DataNodes report recovery.
   */
  @Test
  public void testTruncateFailure() throws IOException {
    int startingFileSize = 2 * BLOCK_SIZE + BLOCK_SIZE / 2;
    int toTruncate = 1;

    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path dir = new Path("/dir");
    final Path p = new Path(dir, "testTruncateFailure");
    {
      FSDataOutputStream out = fs.create(p, false, BLOCK_SIZE, REPLICATION,
          BLOCK_SIZE);
      out.write(contents, 0, startingFileSize);
      try {
        fs.truncate(p, 0);
        fail("Truncate must fail on open file.");
      } catch (IOException expected) {
        GenericTestUtils.assertExceptionContains(
            "Failed to TRUNCATE_FILE", expected);
      } finally {
        out.close();
      }
    }

    {
      FSDataOutputStream out = fs.append(p);
      try {
        fs.truncate(p, 0);
        fail("Truncate must fail for append.");
      } catch (IOException expected) {
        GenericTestUtils.assertExceptionContains(
            "Failed to TRUNCATE_FILE", expected);
      } finally {
        out.close();
      }
    }

    try {
      fs.truncate(p, -1);
      fail("Truncate must fail for a negative new length.");
    } catch (HadoopIllegalArgumentException expected) {
      GenericTestUtils.assertExceptionContains(
          "Cannot truncate to a negative file size", expected);
    }

    try {
      fs.truncate(p, startingFileSize + 1);
      fail("Truncate must fail for a larger new length.");
    } catch (Exception expected) {
      GenericTestUtils.assertExceptionContains(
          "Cannot truncate to a larger file size", expected);
    }

    try {
      fs.truncate(dir, 0);
      fail("Truncate must fail for a directory.");
    } catch (Exception expected) {
      GenericTestUtils.assertExceptionContains(
          "Path is not a file", expected);
    }

    try {
      fs.truncate(new Path(dir, "non-existing"), 0);
      fail("Truncate must fail for a non-existing file.");
    } catch (Exception expected) {
      GenericTestUtils.assertExceptionContains(
          "File does not exist", expected);
    }

    
    fs.setPermission(p, FsPermission.createImmutable((short)0664));
    {
      final UserGroupInformation fooUgi = 
          UserGroupInformation.createUserForTesting("foo", new String[]{"foo"});
      try {
        final FileSystem foofs = DFSTestUtil.getFileSystemAs(fooUgi, conf);
        foofs.truncate(p, 0);
        fail("Truncate must fail for no WRITE permission.");
      } catch (Exception expected) {
        GenericTestUtils.assertExceptionContains(
            "Permission denied", expected);
      }
    }

    cluster.shutdownDataNodes();
    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(LOW_SOFTLIMIT, LOW_HARDLIMIT);

    int newLength = startingFileSize - toTruncate;
    boolean isReady = fs.truncate(p, newLength);
    assertThat("truncate should have triggered block recovery.",
        isReady, is(false));

    {
      try {
        fs.truncate(p, 0);
        fail("Truncate must fail since a truncate is already in progress.");
      } catch (IOException expected) {
        GenericTestUtils.assertExceptionContains(
            "Failed to TRUNCATE_FILE", expected);
      }
    }

    boolean recoveryTriggered = false;
    for(int i = 0; i < RECOVERY_ATTEMPTS; i++) {
      String leaseHolder =
          NameNodeAdapter.getLeaseHolderForPath(cluster.getNameNode(),
          p.toUri().getPath());
      if(leaseHolder.startsWith(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) {
        recoveryTriggered = true;
        break;
      }
      try { Thread.sleep(SLEEP); } catch (InterruptedException ignored) {}
    }
    assertThat("lease recovery should have occurred in ~" +
        SLEEP * RECOVERY_ATTEMPTS + " ms.", recoveryTriggered, is(true));
    cluster.startDataNodes(conf, DATANODE_NUM, true,
        StartupOption.REGULAR, null);
    cluster.waitActive();

    checkBlockRecovery(p);

    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(HdfsConstants.LEASE_SOFTLIMIT_PERIOD,
            conf.getLong(DFSConfigKeys.DFS_LEASE_HARDLIMIT_KEY,
                DFSConfigKeys.DFS_LEASE_HARDLIMIT_DEFAULT) * 1000);

    checkFullFile(p, newLength, contents);
    fs.delete(p, false);
  }

  /**
   * The last block is truncated at mid. (non copy-on-truncate)
   * dn0 is shutdown before truncate and restart after truncate successful.
   */
  @Test(timeout=60000)
  public void testTruncateWithDataNodesRestart() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path p = new Path(parent, "testTruncateWithDataNodesRestart");

    writeContents(contents, startingFileSize, p);
    LocatedBlock oldBlock = getLocatedBlocks(p).getLastLocatedBlock();

    int dn = 0;
    int toTruncateLength = 1;
    int newLength = startingFileSize - toTruncateLength;
    cluster.getDataNodes().get(dn).shutdown();
    truncateAndRestartDN(p, dn, newLength);
    checkBlockRecovery(p);

    LocatedBlock newBlock = getLocatedBlocks(p).getLastLocatedBlock();
    /*
     * For non copy-on-truncate, the truncated block id is the same, but the 
     * GS should increase.
     * The truncated block will be replicated to dn0 after it restarts.
     */
    assertEquals(newBlock.getBlock().getBlockId(), 
        oldBlock.getBlock().getBlockId());
    assertEquals(newBlock.getBlock().getGenerationStamp(),
        oldBlock.getBlock().getGenerationStamp() + 1);

    Thread.sleep(2000);
    // trigger the second time BR to delete the corrupted replica if there's one
    cluster.triggerBlockReports();
    // Wait replicas come to 3
    DFSTestUtil.waitReplication(fs, p, REPLICATION);
    // Old replica is disregarded and replaced with the truncated one
    FsDatasetTestUtils utils = cluster.getFsDatasetTestUtils(dn);
    assertEquals(utils.getStoredDataLength(newBlock.getBlock()),
        newBlock.getBlockSize());
    assertEquals(utils.getStoredGenerationStamp(newBlock.getBlock()),
        newBlock.getBlock().getGenerationStamp());

    // Validate the file
    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));
    checkFullFile(p, newLength, contents);

    fs.delete(parent, true);
  }

  /**
   * The last block is truncated at mid. (copy-on-truncate)
   * dn1 is shutdown before truncate and restart after truncate successful.
   */
  @Test(timeout=60000)
  public void testCopyOnTruncateWithDataNodesRestart() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path p = new Path(parent, "testCopyOnTruncateWithDataNodesRestart");

    writeContents(contents, startingFileSize, p);
    LocatedBlock oldBlock = getLocatedBlocks(p).getLastLocatedBlock();
    fs.allowSnapshot(parent);
    fs.createSnapshot(parent, "ss0");

    int dn = 1;
    int toTruncateLength = 1;
    int newLength = startingFileSize - toTruncateLength;
    cluster.getDataNodes().get(dn).shutdown();
    truncateAndRestartDN(p, dn, newLength);
    checkBlockRecovery(p);

    LocatedBlock newBlock = getLocatedBlocks(p).getLastLocatedBlock();
    /*
     * For copy-on-truncate, new block is made with new block id and new GS.
     * The replicas of the new block is 2, then it will be replicated to dn1.
     */
    assertNotEquals(newBlock.getBlock().getBlockId(), 
        oldBlock.getBlock().getBlockId());
    assertEquals(newBlock.getBlock().getGenerationStamp(),
        oldBlock.getBlock().getGenerationStamp() + 1);

    // Wait replicas come to 3
    DFSTestUtil.waitReplication(fs, p, REPLICATION);
    FsDatasetTestUtils utils = cluster.getFsDatasetTestUtils(dn);
    // New block is replicated to dn1
    assertEquals(utils.getStoredDataLength(newBlock.getBlock()),
        newBlock.getBlockSize());
    // Old replica exists too since there is snapshot
    assertEquals(utils.getStoredDataLength(oldBlock.getBlock()),
        oldBlock.getBlockSize());
    assertEquals(utils.getStoredGenerationStamp(oldBlock.getBlock()),
        oldBlock.getBlock().getGenerationStamp());

    // Validate the file
    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));
    checkFullFile(p, newLength, contents);

    fs.deleteSnapshot(parent, "ss0");
    fs.delete(parent, true);
  }

  /**
   * The last block is truncated at mid. (non copy-on-truncate)
   * dn0, dn1 are restarted immediately after truncate.
   */
  @Test(timeout=60000)
  public void testTruncateWithDataNodesRestartImmediately() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path p = new Path(parent, "testTruncateWithDataNodesRestartImmediately");

    writeContents(contents, startingFileSize, p);
    LocatedBlock oldBlock = getLocatedBlocks(p).getLastLocatedBlock();

    int dn0 = 0;
    int dn1 = 1;
    int toTruncateLength = 1;
    int newLength = startingFileSize - toTruncateLength;
    boolean isReady = fs.truncate(p, newLength);
    assertFalse(isReady);

    cluster.restartDataNode(dn0, false, true);
    cluster.restartDataNode(dn1, false, true);
    cluster.waitActive();
    checkBlockRecovery(p);

    LocatedBlock newBlock = getLocatedBlocks(p).getLastLocatedBlock();
    /*
     * For non copy-on-truncate, the truncated block id is the same, but the 
     * GS should increase.
     */
    assertEquals(newBlock.getBlock().getBlockId(), 
        oldBlock.getBlock().getBlockId());
    assertEquals(newBlock.getBlock().getGenerationStamp(),
        oldBlock.getBlock().getGenerationStamp() + 1);

    Thread.sleep(2000);
    // trigger the second time BR to delete the corrupted replica if there's one
    cluster.triggerBlockReports();
    // Wait replicas come to 3
    DFSTestUtil.waitReplication(fs, p, REPLICATION);
    // Old replica is disregarded and replaced with the truncated one on dn0
    FsDatasetTestUtils utils = cluster.getFsDatasetTestUtils(dn0);
    assertEquals(utils.getStoredDataLength(newBlock.getBlock()),
        newBlock.getBlockSize());
    assertEquals(utils.getStoredGenerationStamp(newBlock.getBlock()),
        newBlock.getBlock().getGenerationStamp());

    // Old replica is disregarded and replaced with the truncated one on dn1
    utils = cluster.getFsDatasetTestUtils(dn1);
    assertEquals(utils.getStoredDataLength(newBlock.getBlock()),
        newBlock.getBlockSize());
    assertEquals(utils.getStoredGenerationStamp(newBlock.getBlock()),
        newBlock.getBlock().getGenerationStamp());

    // Validate the file
    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));
    checkFullFile(p, newLength, contents);

    fs.delete(parent, true);
  }

  /**
   * The last block is truncated at mid. (non copy-on-truncate)
   * shutdown the datanodes immediately after truncate.
   */
  @Test(timeout=60000)
  public void testTruncateWithDataNodesShutdownImmediately() throws Exception {
    int startingFileSize = 3 * BLOCK_SIZE;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    final Path p = new Path(parent, "testTruncateWithDataNodesShutdownImmediately");

    writeContents(contents, startingFileSize, p);

    int toTruncateLength = 1;
    int newLength = startingFileSize - toTruncateLength;
    boolean isReady = fs.truncate(p, newLength);
    assertFalse(isReady);

    cluster.shutdownDataNodes();
    cluster.setDataNodesDead();
    try {
      for(int i = 0; i < SUCCESS_ATTEMPTS && cluster.isDataNodeUp(); i++) {
        Thread.sleep(SLEEP);
      }
      assertFalse("All DataNodes should be down.", cluster.isDataNodeUp());
      LocatedBlocks blocks = getLocatedBlocks(p);
      assertTrue(blocks.isUnderConstruction());
    } finally {
      cluster.startDataNodes(conf, DATANODE_NUM, true,
          StartupOption.REGULAR, null);
      cluster.waitActive();
    }
    checkBlockRecovery(p);

    fs.delete(parent, true);
  }

  /**
   * EditLogOp load test for Truncate.
   */
  @Test
  public void testTruncateEditLogLoad() throws IOException {
    // purge previously accumulated edits
    fs.setSafeMode(SafeModeAction.ENTER);
    fs.saveNamespace();
    fs.setSafeMode(SafeModeAction.LEAVE);

    int startingFileSize = 2 * BLOCK_SIZE + BLOCK_SIZE / 2;
    int toTruncate = 1;
    final String s = "/testTruncateEditLogLoad";
    final Path p = new Path(s);
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    writeContents(contents, startingFileSize, p);

    int newLength = startingFileSize - toTruncate;
    boolean isReady = fs.truncate(p, newLength);
    assertThat("truncate should have triggered block recovery.",
        isReady, is(false));

    cluster.restartNameNode();

    String holder = UserGroupInformation.getCurrentUser().getUserName();
    cluster.getNamesystem().recoverLease(s, holder, "");

    checkBlockRecovery(p);
    checkFullFile(p, newLength, contents);
    fs.delete(p, false);
  }

  /**
   * Upgrade, RollBack, and restart test for Truncate.
   */
  @Test
  public void testUpgradeAndRestart() throws IOException {
    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    fs.allowSnapshot(parent);
    String truncateFile = "testUpgrade";
    final Path p = new Path(parent, truncateFile);
    int startingFileSize = 2 * BLOCK_SIZE;
    int toTruncate = 1;
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    writeContents(contents, startingFileSize, p);

    Path snapshotDir = fs.createSnapshot(parent, "ss0");
    Path snapshotFile = new Path(snapshotDir, truncateFile);

    int newLengthBeforeUpgrade = startingFileSize - toTruncate;
    boolean isReady = fs.truncate(p, newLengthBeforeUpgrade);
    assertThat("truncate should have triggered block recovery.",
        isReady, is(false));

    checkBlockRecovery(p);

    checkFullFile(p, newLengthBeforeUpgrade, contents);
    assertFileLength(snapshotFile, startingFileSize);
    long totalBlockBefore = cluster.getNamesystem().getBlocksTotal();

    restartCluster(StartupOption.UPGRADE);

    assertThat("SafeMode should be OFF",
        cluster.getNamesystem().isInSafeMode(), is(false));
    assertThat("NameNode should be performing upgrade.",
        cluster.getNamesystem().isUpgradeFinalized(), is(false));
    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLengthBeforeUpgrade));

    int newLengthAfterUpgrade = newLengthBeforeUpgrade - toTruncate;
    Block oldBlk = getLocatedBlocks(p).getLastLocatedBlock()
        .getBlock().getLocalBlock();
    isReady = fs.truncate(p, newLengthAfterUpgrade);
    assertThat("truncate should have triggered block recovery.",
        isReady, is(false));
    fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLengthAfterUpgrade));
    assertThat("Should copy on truncate during upgrade",
        getLocatedBlocks(p).getLastLocatedBlock().getBlock()
        .getLocalBlock().getBlockId(), is(not(equalTo(oldBlk.getBlockId()))));

    checkBlockRecovery(p);

    checkFullFile(p, newLengthAfterUpgrade, contents);
    assertThat("Total block count should be unchanged from copy-on-truncate",
        cluster.getNamesystem().getBlocksTotal(), is(totalBlockBefore));

    restartCluster(StartupOption.ROLLBACK);

    assertThat("File does not exist " + p, fs.exists(p), is(true));
    fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLengthBeforeUpgrade));
    checkFullFile(p, newLengthBeforeUpgrade, contents);
    assertThat("Total block count should be unchanged from rolling back",
        cluster.getNamesystem().getBlocksTotal(), is(totalBlockBefore));

    restartCluster(StartupOption.REGULAR);
    assertThat("Total block count should be unchanged from start-up",
        cluster.getNamesystem().getBlocksTotal(), is(totalBlockBefore));
    checkFullFile(p, newLengthBeforeUpgrade, contents);
    assertFileLength(snapshotFile, startingFileSize);

    // empty edits and restart
    fs.setSafeMode(SafeModeAction.ENTER);
    fs.saveNamespace();
    cluster.restartNameNode(true);
    assertThat("Total block count should be unchanged from start-up",
        cluster.getNamesystem().getBlocksTotal(), is(totalBlockBefore));
    checkFullFile(p, newLengthBeforeUpgrade, contents);
    assertFileLength(snapshotFile, startingFileSize);

    fs.deleteSnapshot(parent, "ss0");
    fs.delete(parent, true);
    assertThat("File " + p + " shouldn't exist", fs.exists(p), is(false));
  }

  /**
   * Check truncate recovery.
   */
  @Test
  public void testTruncateRecovery() throws IOException {
    FSNamesystem fsn = cluster.getNamesystem();
    String client = "client";
    String clientMachine = "clientMachine";
    String src = "/test/testTruncateRecovery";
    Path srcPath = new Path(src);

    byte[] contents = AppendTestUtil.initBuffer(BLOCK_SIZE);
    writeContents(contents, BLOCK_SIZE, srcPath);

    INodesInPath iip = fsn.getFSDirectory().getINodesInPath(src, DirOp.WRITE);
    INodeFile file = iip.getLastINode().asFile();
    long initialGenStamp = file.getLastBlock().getGenerationStamp();
    // Test that prepareFileForTruncate sets up in-place truncate.
    fsn.writeLock();
    try {
      Block oldBlock = file.getLastBlock();
      Block truncateBlock = FSDirTruncateOp.prepareFileForTruncate(fsn, iip,
          client, clientMachine, 1, null);
      // In-place truncate uses old block id with new genStamp.
      assertThat(truncateBlock.getBlockId(),
          is(equalTo(oldBlock.getBlockId())));
      assertThat(truncateBlock.getNumBytes(),
          is(oldBlock.getNumBytes()));
      assertThat(truncateBlock.getGenerationStamp(),
          is(fsn.getBlockManager().getBlockIdManager().getGenerationStamp()));
      assertThat(file.getLastBlock().getBlockUCState(),
          is(HdfsServerConstants.BlockUCState.UNDER_RECOVERY));
      long blockRecoveryId = file.getLastBlock().getUnderConstructionFeature()
          .getBlockRecoveryId();
      assertThat(blockRecoveryId, is(initialGenStamp + 1));
      fsn.getEditLog().logTruncate(
          src, client, clientMachine, BLOCK_SIZE-1, Time.now(), truncateBlock);
    } finally {
      fsn.writeUnlock();
    }

    // Re-create file and ensure we are ready to copy on truncate
    writeContents(contents, BLOCK_SIZE, srcPath);
    fs.allowSnapshot(parent);
    fs.createSnapshot(parent, "ss0");
    iip = fsn.getFSDirectory().getINodesInPath(src, DirOp.WRITE);
    file = iip.getLastINode().asFile();
    file.recordModification(iip.getLatestSnapshotId(), true);
    assertThat(file.isBlockInLatestSnapshot(
        (BlockInfoContiguous) file.getLastBlock()), is(true));
    initialGenStamp = file.getLastBlock().getGenerationStamp();
    // Test that prepareFileForTruncate sets up copy-on-write truncate
    fsn.writeLock();
    try {
      Block oldBlock = file.getLastBlock();
      Block truncateBlock = FSDirTruncateOp.prepareFileForTruncate(fsn, iip,
          client, clientMachine, 1, null);
      // Copy-on-write truncate makes new block with new id and genStamp
      assertThat(truncateBlock.getBlockId(),
          is(not(equalTo(oldBlock.getBlockId()))));
      assertThat(truncateBlock.getNumBytes() < oldBlock.getNumBytes(),
          is(true));
      assertThat(truncateBlock.getGenerationStamp(),
          is(fsn.getBlockManager().getBlockIdManager().getGenerationStamp()));
      assertThat(file.getLastBlock().getBlockUCState(),
          is(HdfsServerConstants.BlockUCState.UNDER_RECOVERY));
      long blockRecoveryId = file.getLastBlock().getUnderConstructionFeature()
          .getBlockRecoveryId();
      assertThat(blockRecoveryId, is(initialGenStamp + 1));
      fsn.getEditLog().logTruncate(
          src, client, clientMachine, BLOCK_SIZE-1, Time.now(), truncateBlock);
    } finally {
      fsn.writeUnlock();
    }
    checkBlockRecovery(srcPath);
    fs.deleteSnapshot(parent, "ss0");
    fs.delete(parent, true);
  }

  @Test
  public void testTruncateShellCommand() throws Exception {
    final Path src = new Path("/test/testTruncateShellCommand");
    final int oldLength = 2*BLOCK_SIZE + 1;
    final int newLength = BLOCK_SIZE + 1;

    String[] argv =
        new String[]{"-truncate", String.valueOf(newLength), src.toString()};
    runTruncateShellCommand(src, oldLength, argv);

    // wait for block recovery
    checkBlockRecovery(src);
    assertThat(fs.getFileStatus(src).getLen(), is((long) newLength));
    fs.delete(parent, true);
  }

  @Test
  public void testTruncateShellCommandOnBlockBoundary() throws Exception {
    final Path src = new Path("/test/testTruncateShellCommandOnBoundary");
    final int oldLength = 2 * BLOCK_SIZE;
    final int newLength = BLOCK_SIZE;

    String[] argv =
        new String[]{"-truncate", String.valueOf(newLength), src.toString()};
    runTruncateShellCommand(src, oldLength, argv);

    // shouldn't need to wait for block recovery
    assertThat(fs.getFileStatus(src).getLen(), is((long) newLength));
    fs.delete(parent, true);
  }

  @Test
  public void testTruncateShellCommandWithWaitOption() throws Exception {
    final Path src = new Path("/test/testTruncateShellCommandWithWaitOption");
    final int oldLength = 2 * BLOCK_SIZE + 1;
    final int newLength = BLOCK_SIZE + 1;

    String[] argv = new String[]{"-truncate", "-w", String.valueOf(newLength),
        src.toString()};
    runTruncateShellCommand(src, oldLength, argv);

    // shouldn't need to wait for block recovery
    assertThat(fs.getFileStatus(src).getLen(), is((long) newLength));
    fs.delete(parent, true);
  }

  private void runTruncateShellCommand(Path src, int oldLength,
                                       String[] shellOpts) throws Exception {
    // create file and write data
    writeContents(AppendTestUtil.initBuffer(oldLength), oldLength, src);
    assertThat(fs.getFileStatus(src).getLen(), is((long)oldLength));

    // truncate file using shell
    FsShell shell = null;
    try {
      shell = new FsShell(conf);
      assertThat(ToolRunner.run(shell, shellOpts), is(0));
    } finally {
      if(shell != null) {
        shell.close();
      }
    }
  }

  @Test
  public void testTruncate4Symlink() throws IOException {
    final int fileLength = 3 * BLOCK_SIZE;

    fs.mkdirs(parent);
    final byte[] contents = AppendTestUtil.initBuffer(fileLength);
    final Path file = new Path(parent, "testTruncate4Symlink");
    writeContents(contents, fileLength, file);

    final Path link = new Path(parent, "link");
    fs.createSymlink(file, link, false);

    final int newLength = fileLength/3;
    boolean isReady = fs.truncate(link, newLength);

    assertTrue("Recovery is not expected.", isReady);

    FileStatus fileStatus = fs.getFileStatus(file);
    assertThat(fileStatus.getLen(), is((long) newLength));

    ContentSummary cs = fs.getContentSummary(parent);
    assertEquals("Bad disk space usage",
        cs.getSpaceConsumed(), newLength * REPLICATION);
    // validate the file content
    checkFullFile(file, newLength, contents);

    fs.delete(parent, true);
  }

  /**
   * While rolling upgrade is in-progress the test truncates a file
   * such that copy-on-truncate is triggered, then deletes the file,
   * and makes sure that no blocks involved in truncate are hanging around.
   */
  @Test
  public void testTruncateWithRollingUpgrade() throws Exception {
    final DFSAdmin dfsadmin = new DFSAdmin(cluster.getConfiguration(0));
    DistributedFileSystem dfs = cluster.getFileSystem();
    //start rolling upgrade
    dfs.setSafeMode(SafeModeAction.ENTER);
    int status = dfsadmin.run(new String[]{"-rollingUpgrade", "prepare"});
    assertEquals("could not prepare for rolling upgrade", 0, status);
    dfs.setSafeMode(SafeModeAction.LEAVE);

    Path dir = new Path("/testTruncateWithRollingUpgrade");
    fs.mkdirs(dir);
    final Path p = new Path(dir, "file");
    final byte[] data = new byte[3];
    ThreadLocalRandom.current().nextBytes(data);
    writeContents(data, data.length, p);

    assertEquals("block num should 1", 1,
        cluster.getNamesystem().getFSDirectory().getBlockManager()
            .getTotalBlocks());

    final boolean isReady = fs.truncate(p, 2);
    assertFalse("should be copy-on-truncate", isReady);
    assertEquals("block num should 2", 2,
        cluster.getNamesystem().getFSDirectory().getBlockManager()
            .getTotalBlocks());
    fs.delete(p, true);
    BlockManagerTestUtil.waitForMarkedDeleteQueueIsEmpty(
        cluster.getNamesystem().getBlockManager());
    assertEquals("block num should 0", 0,
        cluster.getNamesystem().getFSDirectory().getBlockManager()
            .getTotalBlocks());
    status = dfsadmin.run(new String[]{"-rollingUpgrade", "finalize"});
    assertEquals("could not finalize rolling upgrade", 0, status);
  }

  static void writeContents(byte[] contents, int fileLength, Path p)
      throws IOException {
    FSDataOutputStream out = fs.create(p, true, BLOCK_SIZE, REPLICATION,
        BLOCK_SIZE);
    out.write(contents, 0, fileLength);
    out.close();
  }

  static void checkBlockRecovery(Path p) throws IOException {
    checkBlockRecovery(p, fs);
  }

  public static void checkBlockRecovery(Path p, DistributedFileSystem dfs)
      throws IOException {
    checkBlockRecovery(p, dfs, SUCCESS_ATTEMPTS, SLEEP);
  }

  public static void checkBlockRecovery(Path p, DistributedFileSystem dfs,
      int attempts, long sleepMs) throws IOException {
    boolean success = false;
    for(int i = 0; i < attempts; i++) {
      LocatedBlocks blocks = getLocatedBlocks(p, dfs);
      boolean noLastBlock = blocks.getLastLocatedBlock() == null;
      if(!blocks.isUnderConstruction() &&
          (noLastBlock || blocks.isLastBlockComplete())) {
        success = true;
        break;
      }
      try { Thread.sleep(sleepMs); } catch (InterruptedException ignored) {}
    }
    assertThat("inode should complete in ~" + sleepMs * attempts + " ms.",
        success, is(true));
  }

  static LocatedBlocks getLocatedBlocks(Path src) throws IOException {
    return getLocatedBlocks(src, fs);
  }

  static LocatedBlocks getLocatedBlocks(Path src, DistributedFileSystem dfs)
      throws IOException {
    return dfs.getClient().getLocatedBlocks(src.toString(), 0, Long.MAX_VALUE);
  }

  static void assertBlockExists(Block blk) {
    assertNotNull("BlocksMap does not contain block: " + blk,
        cluster.getNamesystem().getStoredBlock(blk));
  }

  static void assertBlockNotPresent(Block blk) {
    assertNull("BlocksMap should not contain block: " + blk,
        cluster.getNamesystem().getStoredBlock(blk));
  }

  static void assertFileLength(Path file, long length) throws IOException {
    byte[] data = DFSTestUtil.readFileBuffer(fs, file);
    assertEquals("Wrong data size in snapshot.", length, data.length);
  }

  static void checkFullFile(Path p, int newLength, byte[] contents)
      throws IOException {
    AppendTestUtil.checkFullFile(fs, p, newLength, contents, p.toString());
  }

  static void restartCluster(StartupOption o)
      throws IOException {
    cluster.shutdown();
    if(StartupOption.ROLLBACK == o)
      NameNode.doRollback(conf, false);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODE_NUM)
        .format(false)
        .startupOption(o==StartupOption.ROLLBACK ? StartupOption.REGULAR : o)
        .dnStartupOption(o!=StartupOption.ROLLBACK ? StartupOption.REGULAR : o)
        .build();
    fs = cluster.getFileSystem();
  }

  private void truncateAndRestartDN(Path p, int dn, int newLength)
      throws IOException {
    try {
      boolean isReady = fs.truncate(p, newLength);
      assertFalse(isReady);
    } finally {
      cluster.restartDataNode(dn, false, true);
      cluster.waitActive();
    }
  }

  /**
   * QuotaUsage in Truncate with Snapshot.
   */
  @Test
  public void testQuotaOnTruncateWithSnapshot() throws Exception {
    Path root = new Path("/");
    Path dirPath = new Path(root, "dir");
    assertTrue(fs.mkdirs(dirPath));
    Path filePath = new Path(dirPath, "file");
    DFSTestUtil.createFile(fs, filePath, 10, (short) 3, 0);

    // verify quotausage and content summary after creating snapshot
    fs.allowSnapshot(dirPath);
    fs.createSnapshot(dirPath, "s1");
    assertEquals(fs.getContentSummary(root).getSpaceConsumed(),
        fs.getQuotaUsage(root).getSpaceConsumed());

    // truncating the file size to 5bytes
    boolean blockrecovery = fs.truncate(filePath, 5);
    if (!blockrecovery) {
      checkBlockRecovery(filePath, fs, 300, 100L);
    }

    // verify quotausage and content summary after truncating file which exists
    // in snapshot
    assertEquals(fs.getContentSummary(root).getSpaceConsumed(),
        fs.getQuotaUsage(root).getSpaceConsumed());

    // verify quotausage and content summary after deleting snapshot
    // now the quota of the file shouldn't present in quotausage and content
    // summary
    fs.deleteSnapshot(dirPath, "s1");
    assertEquals(fs.getContentSummary(root).getSpaceConsumed(),
        fs.getQuotaUsage(root).getSpaceConsumed());
  }

  /**
   * Test concat on file which is a reference.
   */
  @Test
  public void testConcatOnInodeRefernce() throws IOException {
    String dir = "/testConcat";
    Path trgDir = new Path(dir);
    fs.mkdirs(new Path(dir), FsPermission.getDirDefault());

    // Create a target file
    Path trg = new Path(dir, "file");
    DFSTestUtil.createFile(fs, trg, 512, (short) 2, 0);

    String dir2 = "/dir2";
    Path srcDir = new Path(dir2);
    // create a source file
    fs.mkdirs(srcDir);
    fs.allowSnapshot(srcDir);
    Path src = new Path(srcDir, "file1");
    DFSTestUtil.createFile(fs, src, 512, (short) 2, 0);

    // make the file as an Inode reference and delete the reference
    fs.createSnapshot(srcDir, "s1");
    fs.rename(src, trgDir);
    fs.deleteSnapshot(srcDir, "s1");
    Path[] srcs = new Path[1];
    srcs[0] = new Path(dir, "file1");
    assertEquals(2, fs.getContentSummary(new Path(dir)).getFileCount());

    // perform concat
    fs.concat(trg, srcs);
    assertEquals(1, fs.getContentSummary(new Path(dir)).getFileCount());
  }

  /**
   * Test Quota space consumed with multiple snapshots.
   */
  @Test
  public void testQuotaSpaceConsumedWithSnapshots() throws IOException {
    Path root = new Path("/");
    Path dir = new Path(root, "dir");
    fs.mkdirs(dir);
    fs.allowSnapshot(dir);

    // create a file
    Path file2 = new Path(dir, "file2");
    DFSTestUtil.createFile(fs, file2, 30, (short) 1, 0);

    // create a snapshot and truncate the file
    fs.createSnapshot(dir, "s1");
    boolean isReady = fs.truncate(file2, 20);
    if (!isReady) {
      checkBlockRecovery(file2);
    }

    // create one more snapshot and truncate the file which exists in previous
    // snapshot
    fs.createSnapshot(dir, "s2");
    isReady = fs.truncate(file2, 10);
    if (!isReady) {
      checkBlockRecovery(file2);
    }

    // delete the snapshots and check quota space consumed usage
    fs.deleteSnapshot(dir, "s1");
    fs.deleteSnapshot(dir, "s2");
    assertEquals(fs.getContentSummary(root).getSpaceConsumed(),
        fs.getQuotaUsage(root).getSpaceConsumed());
    fs.delete(dir, true);
    assertEquals(fs.getContentSummary(root).getSpaceConsumed(),
        fs.getQuotaUsage(root).getSpaceConsumed());

  }

  /**
   * Test truncate on a snapshotted file.
   */
  @Test
  public void testTruncatewithRenameandSnapshot() throws Exception {
    final Path dir = new Path("/dir");
    fs.mkdirs(dir, new FsPermission((short) 0777));
    final Path file = new Path(dir, "file");
    final Path movedFile = new Path("/file");

    // 1. create a file and snapshot for dir which is having a file
    DFSTestUtil.createFile(fs, file, 10, (short) 3, 0);
    fs.allowSnapshot(dir);
    Path snapshotPath = fs.createSnapshot(dir, "s0");
    assertTrue(fs.exists(snapshotPath));

    // 2. move the file
    fs.rename(file, new Path("/"));

    // 3.truncate the moved file
    final boolean isReady = fs.truncate(movedFile, 5);
    if (!isReady) {
      checkBlockRecovery(movedFile);
    }
    FileStatus fileStatus = fs.getFileStatus(movedFile);
    assertEquals(5, fileStatus.getLen());

    // 4. get block locations of file which is in snapshot
    LocatedBlocks locations = fs.getClient().getNamenode()
        .getBlockLocations("/dir/.snapshot/s0/file", 0, 10);
    assertEquals(10, locations.get(0).getBlockSize());
  }
}
