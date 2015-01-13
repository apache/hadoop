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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetAddress;

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
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFileTruncate {
  static {
    GenericTestUtils.setLogLevel(NameNode.stateChangeLog, Level.ALL);
    GenericTestUtils.setLogLevel(FSEditLogLoader.LOG, Level.ALL);
  }
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
    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));

    boolean recoveryTriggered = false;
    for(int i = 0; i < RECOVERY_ATTEMPTS; i++) {
      String leaseHolder =
          NameNodeAdapter.getLeaseHolderForPath(cluster.getNameNode(),
          p.toUri().getPath());
      if(leaseHolder.equals(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) {
        cluster.startDataNodes(conf, DATANODE_NUM, true,
            HdfsServerConstants.StartupOption.REGULAR, null);
        recoveryTriggered = true;
        break;
      }
      try { Thread.sleep(SLEEP); } catch (InterruptedException ignored) {}
    }
    assertThat("lease recovery should have occurred in ~" +
        SLEEP * RECOVERY_ATTEMPTS + " ms.", recoveryTriggered, is(true));

    checkBlockRecovery(p);

    NameNodeAdapter.getLeaseManager(cluster.getNamesystem())
        .setLeasePeriod(HdfsConstants.LEASE_SOFTLIMIT_PERIOD,
            HdfsConstants.LEASE_HARDLIMIT_PERIOD);

    fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));

    AppendTestUtil.checkFullFile(fs, p, newLength, contents, p.toString());
    fs.delete(p, false);
  }

  /**
   * EditLogOp load test for Truncate.
   */
  @Test
  public void testTruncateEditLogLoad() throws IOException {
    int startingFileSize = 2 * BLOCK_SIZE + BLOCK_SIZE / 2;
    int toTruncate = 1;

    byte[] contents = AppendTestUtil.initBuffer(startingFileSize);

    final Path p = new Path("/testTruncateEditLogLoad");
    writeContents(contents, startingFileSize, p);

    int newLength = startingFileSize - toTruncate;
    boolean isReady = fs.truncate(p, newLength);
    assertThat("truncate should have triggered block recovery.",
        isReady, is(false));

    checkBlockRecovery(p);

    cluster.restartNameNode();

    FileStatus fileStatus = fs.getFileStatus(p);
    assertThat(fileStatus.getLen(), is((long) newLength));

    AppendTestUtil.checkFullFile(fs, p, newLength, contents, p.toString());
    fs.delete(p, false);
  }

  /**
   * Check truncate recovery.
   */
  @Test
  public void testTruncateLastBlock() throws IOException {
    FSNamesystem fsn = cluster.getNamesystem();

    String src = "/file";
    Path srcPath = new Path(src);

    byte[] contents = AppendTestUtil.initBuffer(BLOCK_SIZE);
    writeContents(contents, BLOCK_SIZE, srcPath);

    INodeFile inode = fsn.getFSDirectory().getINode(src).asFile();
    long oldGenstamp = GenerationStamp.LAST_RESERVED_STAMP;
    DatanodeDescriptor dn = DFSTestUtil.getLocalDatanodeDescriptor();
    DatanodeStorageInfo storage = DFSTestUtil.createDatanodeStorageInfo(
        dn.getDatanodeUuid(), InetAddress.getLocalHost().getHostAddress());
    dn.isAlive = true;

    BlockInfoUnderConstruction blockInfo = new BlockInfoUnderConstruction(
        new Block(0, 1, oldGenstamp), (short) 1,
        HdfsServerConstants.BlockUCState.BEING_TRUNCATED,
        new DatanodeStorageInfo[] {storage});

    inode.setBlocks(new BlockInfo[] {blockInfo});
    fsn.writeLock();
    try {
      fsn.initializeBlockRecovery(inode);
      assertThat(inode.getLastBlock().getBlockUCState(),
          is(HdfsServerConstants.BlockUCState.BEING_TRUNCATED));
      long blockRecoveryId = ((BlockInfoUnderConstruction) inode.getLastBlock())
          .getBlockRecoveryId();
      assertThat(blockRecoveryId, is(oldGenstamp + 2));
    } finally {
      fsn.writeUnlock();
    }
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
}
