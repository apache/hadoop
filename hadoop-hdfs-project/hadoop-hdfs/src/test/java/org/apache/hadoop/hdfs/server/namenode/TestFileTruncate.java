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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileTruncate {
  static {
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, Level.ALL);
    GenericTestUtils.setLogLevel(FSEditLogLoader.LOG, Level.ALL);
  }
  static final Log LOG = LogFactory.getLog(TestFileTruncate.class);
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

  @BeforeClass
  public static void startUp() throws IOException {
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, SHORT_HEARTBEAT);
    cluster = new MiniDFSCluster.Builder(conf)
        .format(true)
        .numDataNodes(DATANODE_NUM)
        .nameNodePort(NameNode.DEFAULT_PORT)
        .waitSafeMode(true)
        .build();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if(fs != null)      fs.close();
    if(cluster != null) cluster.shutdown();
  }

  /**
   * Truncate files of different sizes byte by byte.
   */
  @Test
  public void testBasicTruncate() throws IOException {
    int startingFileSize = 3 * BLOCK_SIZE;

    Path parent = new Path("/test");
    fs.mkdirs(parent);
    fs.setQuota(parent, 100, 1000);
    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);
    for (int fileLength = startingFileSize; fileLength > 0;
                                            fileLength -= BLOCK_SIZE - 1) {
      for (int toTruncate = 0; toTruncate <= fileLength; toTruncate++) {
        final Path p = new Path(parent, "testBasicTruncate" + fileLength);
        writeContents(contents, fileLength, p);

        int newLength = fileLength - toTruncate;
        boolean isReady = fs.truncate(p, newLength);

        if(!isReady)
          checkBlockRecovery(p);

        FileStatus fileStatus = fs.getFileStatus(p);
        assertThat(fileStatus.getLen(), is((long) newLength));

        ContentSummary cs = fs.getContentSummary(parent);
        assertEquals("Bad disk space usage",
            cs.getSpaceConsumed(), newLength * REPLICATION);
        // validate the file content
        AppendTestUtil.checkFullFile(fs, p, newLength, contents, p.toString());
      }
    }
    fs.delete(parent, true);
  }

  @Test
  public void testSnapshotWithAppendTruncate() throws IOException {
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
  void testSnapshotWithAppendTruncate(int ... deleteOrder) throws IOException {
    FSDirectory fsDir = cluster.getNamesystem().getFSDirectory();
    Path parent = new Path("/test");
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
  public void testSnapshotWithTruncates() throws IOException {
    testSnapshotWithTruncates(0, 1, 2);
    testSnapshotWithTruncates(0, 2, 1);
    testSnapshotWithTruncates(1, 0, 2);
    testSnapshotWithTruncates(1, 2, 0);
    testSnapshotWithTruncates(2, 0, 1);
    testSnapshotWithTruncates(2, 1, 0);
  }

  void testSnapshotWithTruncates(int ... deleteOrder) throws IOException {
    Path parent = new Path("/test");
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
    assertFileLength(src, length[2]);
    assertBlockExists(firstBlk);

    // Diskspace consumed should be 6 bytes * 3. [blk 1,4 SS:]
    contentSummary = fs.getContentSummary(parent);
    assertThat(contentSummary.getSpaceConsumed(), is(18L));
    assertThat(contentSummary.getLength(), is(6L));

    fs.delete(src, false);
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
    final Path p = new Path("/testTruncateFailure");
    FSDataOutputStream out = fs.create(p, false, BLOCK_SIZE, REPLICATION,
        BLOCK_SIZE);
    out.write(contents, 0, startingFileSize);
    try {
      fs.truncate(p, 0);
      fail("Truncate must fail on open file.");
    } catch(IOException expected) {}
    out.close();

    cluster.shutdownDataNodes();
    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(LOW_SOFTLIMIT, LOW_HARDLIMIT);

    int newLength = startingFileSize - toTruncate;
    boolean isReady = fs.truncate(p, newLength);
    assertThat("truncate should have triggered block recovery.",
        isReady, is(false));

    boolean recoveryTriggered = false;
    for(int i = 0; i < RECOVERY_ATTEMPTS; i++) {
      String leaseHolder =
          NameNodeAdapter.getLeaseHolderForPath(cluster.getNameNode(),
          p.toUri().getPath());
      if(leaseHolder.equals(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) {
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
            HdfsConstants.LEASE_HARDLIMIT_PERIOD);

    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));

    checkFullFile(p, newLength, contents);
    fs.delete(p, false);
  }

  /**
   * EditLogOp load test for Truncate.
   */
  @Test
  public void testTruncateEditLogLoad() throws IOException {
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

    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));

    checkFullFile(p, newLength, contents);
    fs.delete(p, false);
  }

  /**
   * Upgrade, RollBack, and restart test for Truncate.
   */
  @Test
  public void testUpgradeAndRestart() throws IOException {
    Path parent = new Path("/test");
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
    fs.setSafeMode(SafeModeAction.SAFEMODE_ENTER);
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
    Path parent = new Path("/test");
    String src = "/test/testTruncateRecovery";
    Path srcPath = new Path(src);

    byte[] contents = AppendTestUtil.initBuffer(BLOCK_SIZE);
    writeContents(contents, BLOCK_SIZE, srcPath);

    INodesInPath iip = fsn.getFSDirectory().getINodesInPath4Write(src, true);
    INodeFile file = iip.getLastINode().asFile();
    long initialGenStamp = file.getLastBlock().getGenerationStamp();
    // Test that prepareFileForTruncate sets up in-place truncate.
    fsn.writeLock();
    try {
      Block oldBlock = file.getLastBlock();
      Block truncateBlock =
          fsn.prepareFileForTruncate(iip, client, clientMachine, 1, null);
      // In-place truncate uses old block id with new genStamp.
      assertThat(truncateBlock.getBlockId(),
          is(equalTo(oldBlock.getBlockId())));
      assertThat(truncateBlock.getNumBytes(),
          is(oldBlock.getNumBytes()));
      assertThat(truncateBlock.getGenerationStamp(),
          is(fsn.getBlockIdManager().getGenerationStampV2()));
      assertThat(file.getLastBlock().getBlockUCState(),
          is(HdfsServerConstants.BlockUCState.UNDER_RECOVERY));
      long blockRecoveryId = ((BlockInfoUnderConstruction) file.getLastBlock())
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
    iip = fsn.getFSDirectory().getINodesInPath(src, true);
    file = iip.getLastINode().asFile();
    file.recordModification(iip.getLatestSnapshotId(), true);
    assertThat(file.isBlockInLatestSnapshot(file.getLastBlock()), is(true));
    initialGenStamp = file.getLastBlock().getGenerationStamp();
    // Test that prepareFileForTruncate sets up copy-on-write truncate
    fsn.writeLock();
    try {
      Block oldBlock = file.getLastBlock();
      Block truncateBlock =
          fsn.prepareFileForTruncate(iip, client, clientMachine, 1, null);
      // Copy-on-write truncate makes new block with new id and genStamp
      assertThat(truncateBlock.getBlockId(),
          is(not(equalTo(oldBlock.getBlockId()))));
      assertThat(truncateBlock.getNumBytes() < oldBlock.getNumBytes(),
          is(true));
      assertThat(truncateBlock.getGenerationStamp(),
          is(fsn.getBlockIdManager().getGenerationStampV2()));
      assertThat(file.getLastBlock().getBlockUCState(),
          is(HdfsServerConstants.BlockUCState.UNDER_RECOVERY));
      long blockRecoveryId = ((BlockInfoUnderConstruction) file.getLastBlock())
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

  static void writeContents(byte[] contents, int fileLength, Path p)
      throws IOException {
    FSDataOutputStream out = fs.create(p, true, BLOCK_SIZE, REPLICATION,
        BLOCK_SIZE);
    out.write(contents, 0, fileLength);
    out.close();
  }

  static void checkBlockRecovery(Path p) throws IOException {
    boolean success = false;
    for(int i = 0; i < SUCCESS_ATTEMPTS; i++) {
      LocatedBlocks blocks = getLocatedBlocks(p);
      boolean noLastBlock = blocks.getLastLocatedBlock() == null;
      if(!blocks.isUnderConstruction() &&
          (noLastBlock || blocks.isLastBlockComplete())) {
        success = true;
        break;
      }
      try { Thread.sleep(SLEEP); } catch (InterruptedException ignored) {}
    }
    assertThat("inode should complete in ~" + SLEEP * SUCCESS_ATTEMPTS + " ms.",
        success, is(true));
  }

  static LocatedBlocks getLocatedBlocks(Path src) throws IOException {
    return fs.getClient().getLocatedBlocks(src.toString(), 0, Long.MAX_VALUE);
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
        .nameNodePort(NameNode.DEFAULT_PORT)
        .startupOption(o==StartupOption.ROLLBACK ? StartupOption.REGULAR : o)
        .dnStartupOption(o!=StartupOption.ROLLBACK ? StartupOption.REGULAR : o)
        .build();
    fs = cluster.getFileSystem();
  }
}
