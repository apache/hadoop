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
package org.apache.hadoop.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NamenodeFsck;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Class is used to test client reporting corrupted block replica to name node.
 * The reporting policy is if block replica is more than one, if all replicas
 * are corrupted, client does not report (since the client can handicapped). If
 * some of the replicas are corrupted, client reports the corrupted block
 * replicas. In case of only one block replica, client always reports corrupted
 * replica.
 */
public class TestClientReportBadBlock {
  private static final Log LOG = LogFactory
      .getLog(TestClientReportBadBlock.class);

  static final long BLOCK_SIZE = 64 * 1024;
  private static int buffersize;
  private static MiniDFSCluster cluster;
  private static DistributedFileSystem dfs;
  private static final int numDataNodes = 3;
  private static final Configuration conf = new HdfsConfiguration();

  Random rand = new Random();

  @Before
  public void startUpCluster() throws IOException {
    // disable block scanner
    conf.setInt(DFSConfigKeys.DFS_DATANODE_SCAN_PERIOD_HOURS_KEY, -1); 
    // Set short retry timeouts so this test runs faster
    conf.setInt(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 10);
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDataNodes)
        .build();
    cluster.waitActive();
    dfs = cluster.getFileSystem();
    buffersize = conf.getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096);
  }

  @After
  public void shutDownCluster() throws IOException {
    if (dfs != null) {
      dfs.close();
      dfs = null;
    }
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /*
   * This test creates a file with one block replica. Corrupt the block. Make
   * DFSClient read the corrupted file. Corrupted block is expected to be
   * reported to name node.
   */
  @Test
  public void testOneBlockReplica() throws Exception {
    final short repl = 1;
    final int corruptBlockNumber = 1;
    for (int i = 0; i < 2; i++) {
      // create a file
      String fileName = "/tmp/testClientReportBadBlock/OneBlockReplica" + i;
      Path filePath = new Path(fileName);
      createAFileWithCorruptedBlockReplicas(filePath, repl, corruptBlockNumber);
      if (i == 0) {
        dfsClientReadFile(filePath);
      } else {
        dfsClientReadFileFromPosition(filePath);
      }
      // the only block replica is corrupted. The LocatedBlock should be marked
      // as corrupted. But the corrupted replica is expected to be returned
      // when calling Namenode#getBlockLocations() since all(one) replicas are
      // corrupted.
      int expectedReplicaCount = 1;
      verifyCorruptedBlockCount(filePath, expectedReplicaCount);
      verifyFirstBlockCorrupted(filePath, true);
      verifyFsckBlockCorrupted();
      testFsckListCorruptFilesBlocks(filePath, -1);
    }
  }

  /**
   * This test creates a file with three block replicas. Corrupt all of the
   * replicas. Make dfs client read the file. No block corruption should be
   * reported.
   */
  @Test
  public void testCorruptAllOfThreeReplicas() throws Exception {
    final short repl = 3;
    final int corruptBlockNumber = 3;
    for (int i = 0; i < 2; i++) {
      // create a file
      String fileName = "/tmp/testClientReportBadBlock/testCorruptAllReplicas"
          + i;
      Path filePath = new Path(fileName);
      createAFileWithCorruptedBlockReplicas(filePath, repl, corruptBlockNumber);
      // ask dfs client to read the file
      if (i == 0) {
        dfsClientReadFile(filePath);
      } else {
        dfsClientReadFileFromPosition(filePath);
      }
      // As all replicas are corrupted. We expect DFSClient does NOT report
      // corrupted replicas to the name node.
      int expectedReplicasReturned = repl;
      verifyCorruptedBlockCount(filePath, expectedReplicasReturned);
      // LocatedBlock should not have the block marked as corrupted.
      verifyFirstBlockCorrupted(filePath, false);
      verifyFsckHealth("");
      testFsckListCorruptFilesBlocks(filePath, 0);
    }
  }

  /**
   * This test creates a file with three block replicas. Corrupt two of the
   * replicas. Make dfs client read the file. The corrupted blocks with their
   * owner data nodes should be reported to the name node. 
   */
  @Test
  public void testCorruptTwoOutOfThreeReplicas() throws Exception {
    final short repl = 3;
    final int corruptBlocReplicas = 2;
    for (int i = 0; i < 2; i++) {
      String fileName = 
        "/tmp/testClientReportBadBlock/CorruptTwoOutOfThreeReplicas"+ i;
      Path filePath = new Path(fileName);
      createAFileWithCorruptedBlockReplicas(filePath, repl, corruptBlocReplicas);
      int replicaCount = 0;
      /*
       * The order of data nodes in LocatedBlock returned by name node is sorted 
       * by NetworkToplology#pseudoSortByDistance. In current MiniDFSCluster, 
       * when LocatedBlock is returned, the sorting is based on a random order.
       * That is to say, the DFS client and simulated data nodes in mini DFS
       * cluster are considered not on the same host nor the same rack.
       * Therefore, even we corrupted the first two block replicas based in 
       * order. When DFSClient read some block replicas, it is not guaranteed 
       * which block replicas (good/bad) will be returned first. So we try to 
       * re-read the file until we know the expected replicas numbers is 
       * returned.
       */
      while (replicaCount != repl - corruptBlocReplicas) {
          if (i == 0) {
            dfsClientReadFile(filePath);
          } else {
            dfsClientReadFileFromPosition(filePath);
          }
        LocatedBlocks blocks = dfs.dfs.getNamenode().
                  getBlockLocations(filePath.toString(), 0, Long.MAX_VALUE);
        replicaCount = blocks.get(0).getLocations().length;
      }
      verifyFirstBlockCorrupted(filePath, false);
      int expectedReplicaCount = repl-corruptBlocReplicas;
      verifyCorruptedBlockCount(filePath, expectedReplicaCount);
      verifyFsckHealth("Target Replicas is 3 but found 1 live replica");
      testFsckListCorruptFilesBlocks(filePath, 0);
    }
  }

  /**
   * Create a file with one block and corrupt some/all of the block replicas.
   */
  private void createAFileWithCorruptedBlockReplicas(Path filePath, short repl,
      int corruptBlockCount) throws IOException, AccessControlException,
      FileNotFoundException, UnresolvedLinkException, InterruptedException, TimeoutException {
    DFSTestUtil.createFile(dfs, filePath, BLOCK_SIZE, repl, 0);
    DFSTestUtil.waitReplication(dfs, filePath, repl);
    // Locate the file blocks by asking name node
    final LocatedBlocks locatedblocks = dfs.dfs.getNamenode()
        .getBlockLocations(filePath.toString(), 0L, BLOCK_SIZE);
    Assert.assertEquals(repl, locatedblocks.get(0).getLocations().length);
    // The file only has one block
    LocatedBlock lblock = locatedblocks.get(0);
    DatanodeInfo[] datanodeinfos = lblock.getLocations();
    ExtendedBlock block = lblock.getBlock();
    // corrupt some /all of the block replicas
    for (int i = 0; i < corruptBlockCount; i++) {
      DatanodeInfo dninfo = datanodeinfos[i];
      final DataNode dn = cluster.getDataNode(dninfo.getIpcPort());
      cluster.corruptReplica(dn, block);
      LOG.debug("Corrupted block " + block.getBlockName() + " on data node "
          + dninfo);

    }
  }

  /**
   * Verify the first block of the file is corrupted (for all its replica).
   */
  private void verifyFirstBlockCorrupted(Path filePath, boolean isCorrupted)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    final LocatedBlocks locatedBlocks = dfs.dfs.getNamenode()
        .getBlockLocations(filePath.toUri().getPath(), 0, Long.MAX_VALUE);
    final LocatedBlock firstLocatedBlock = locatedBlocks.get(0);
    Assert.assertEquals(isCorrupted, firstLocatedBlock.isCorrupt());
  }

  /**
   * Verify the number of corrupted block replicas by fetching the block
   * location from name node.
   */
  private void verifyCorruptedBlockCount(Path filePath, int expectedReplicas)
      throws AccessControlException, FileNotFoundException,
      UnresolvedLinkException, IOException {
    final LocatedBlocks lBlocks = dfs.dfs.getNamenode().getBlockLocations(
        filePath.toUri().getPath(), 0, Long.MAX_VALUE);
    // we expect only the first block of the file is used for this test
    LocatedBlock firstLocatedBlock = lBlocks.get(0);
    Assert.assertEquals(expectedReplicas,
        firstLocatedBlock.getLocations().length);
  }

  /**
   * Ask dfs client to read the file
   */
  private void dfsClientReadFile(Path corruptedFile) throws IOException,
      UnresolvedLinkException {
    DFSInputStream in = dfs.dfs.open(corruptedFile.toUri().getPath());
    byte[] buf = new byte[buffersize];
    int nRead = 0; // total number of bytes read
    
    try {
      do {
        nRead = in.read(buf, 0, buf.length);
      } while (nRead > 0);
    } catch (ChecksumException ce) {
      // caught ChecksumException if all replicas are bad, ignore and continue.
      LOG.debug("DfsClientReadFile caught ChecksumException.");
    } catch (BlockMissingException bme) {
      // caught BlockMissingException, ignore.
      LOG.debug("DfsClientReadFile caught BlockMissingException.");
    }
  }

  /**
   * DFS client read bytes starting from the specified position.
   */
  private void dfsClientReadFileFromPosition(Path corruptedFile)
      throws UnresolvedLinkException, IOException {
    DFSInputStream in = dfs.dfs.open(corruptedFile.toUri().getPath());
    byte[] buf = new byte[buffersize];
    int startPosition = 2;
    int nRead = 0; // total number of bytes read
    try {
      do {
        nRead = in.read(startPosition, buf, 0, buf.length);
        startPosition += buf.length;
      } while (nRead > 0);
    } catch (BlockMissingException bme) {
      LOG.debug("DfsClientReadFile caught BlockMissingException.");
    }
  }

  private static void verifyFsckHealth(String expected) throws Exception {
    // Fsck health has error code 0.
    // Make sure filesystem is in healthy state
    String outStr = runFsck(conf, 0, true, "/");
    LOG.info(outStr);
    Assert.assertTrue(outStr.contains(NamenodeFsck.HEALTHY_STATUS));
    if (!expected.equals("")) {
      Assert.assertTrue(outStr.contains(expected));
    }
  }

  private static void verifyFsckBlockCorrupted() throws Exception {
    String outStr = runFsck(conf, 1, true, "/");
    LOG.info(outStr);
    Assert.assertTrue(outStr.contains(NamenodeFsck.CORRUPT_STATUS));
  }
  
  private static void testFsckListCorruptFilesBlocks(Path filePath, int errorCode) throws Exception{
    String outStr = runFsck(conf, errorCode, true, filePath.toString(), "-list-corruptfileblocks");
    LOG.info("fsck -list-corruptfileblocks out: " + outStr);
    if (errorCode != 0) {
      Assert.assertTrue(outStr.contains("CORRUPT files"));
    }
  }

  static String runFsck(Configuration conf, int expectedErrCode,
      boolean checkErrorCode, String... path) throws Exception {
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(bStream, true);
    int errCode = ToolRunner.run(new DFSck(conf, out), path);
    if (checkErrorCode)
      Assert.assertEquals(expectedErrCode, errCode);
    return bStream.toString();
  }
}
