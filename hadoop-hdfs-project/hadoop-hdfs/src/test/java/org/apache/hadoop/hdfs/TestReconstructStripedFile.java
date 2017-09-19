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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.io.erasurecode.rawcoder.NativeRSRawErasureCoderFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestReconstructStripedFile {
  public static final Log LOG = LogFactory.getLog(TestReconstructStripedFile.class);

  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private final int dataBlkNum = ecPolicy.getNumDataUnits();
  private final int parityBlkNum = ecPolicy.getNumParityUnits();
  private final int cellSize = ecPolicy.getCellSize();
  private final int blockSize = cellSize * 3;
  private final int groupSize = dataBlkNum + parityBlkNum;
  private final int dnNum = groupSize + parityBlkNum;

  static {
    GenericTestUtils.setLogLevel(DFSClient.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(BlockManager.LOG, Level.ALL);
    GenericTestUtils.setLogLevel(BlockManager.blockLog, Level.ALL);
  }

  enum ReconstructionType {
    DataOnly,
    ParityOnly,
    Any
  }

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  // Map: DatanodeID -> datanode index in cluster
  private Map<DatanodeID, Integer> dnMap = new HashMap<>();
  private final Random random = new Random();

  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        cellSize - 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    if (ErasureCodeNative.isNativeCodeLoaded()) {
      conf.set(
          CodecUtil.IO_ERASURECODE_CODEC_RS_RAWCODERS_KEY,
          NativeRSRawErasureCoderFactory.CODER_NAME);
    }
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(dnNum).build();
    cluster.waitActive();

    fs = cluster.getFileSystem();
    fs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    fs.getClient().setErasureCodingPolicy("/",
        StripedFileTestUtil.getDefaultECPolicy().getName());

    List<DataNode> datanodes = cluster.getDataNodes();
    for (int i = 0; i < dnNum; i++) {
      dnMap.put(datanodes.get(i).getDatanodeId(), i);
    }
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 120000)
  public void testRecoverOneParityBlock() throws Exception {
    int fileLen = (dataBlkNum + 1) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverOneParityBlock", fileLen,
        ReconstructionType.ParityOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneParityBlock1() throws Exception {
    int fileLen = cellSize + cellSize / 10;
    assertFileBlocksReconstruction("/testRecoverOneParityBlock1", fileLen,
        ReconstructionType.ParityOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneParityBlock2() throws Exception {
    int fileLen = 1;
    assertFileBlocksReconstruction("/testRecoverOneParityBlock2", fileLen,
        ReconstructionType.ParityOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneParityBlock3() throws Exception {
    int fileLen = (dataBlkNum - 1) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverOneParityBlock3", fileLen,
        ReconstructionType.ParityOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverAllParityBlocks() throws Exception {
    int fileLen = dataBlkNum * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAllParityBlocks", fileLen,
        ReconstructionType.ParityOnly, parityBlkNum);
  }

  @Test(timeout = 120000)
  public void testRecoverAllDataBlocks() throws Exception {
    int fileLen = (dataBlkNum + parityBlkNum) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAllDataBlocks", fileLen,
        ReconstructionType.DataOnly, parityBlkNum);
  }

  @Test(timeout = 120000)
  public void testRecoverAllDataBlocks1() throws Exception {
    int fileLen = parityBlkNum * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAllDataBlocks1", fileLen,
        ReconstructionType.DataOnly, parityBlkNum);
  }

  @Test(timeout = 120000)
  public void testRecoverOneDataBlock() throws Exception {
    int fileLen = (dataBlkNum + 1) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverOneDataBlock", fileLen,
        ReconstructionType.DataOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneDataBlock1() throws Exception {
    int fileLen = cellSize + cellSize/10;
    assertFileBlocksReconstruction("/testRecoverOneDataBlock1", fileLen,
        ReconstructionType.DataOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverOneDataBlock2() throws Exception {
    int fileLen = 1;
    assertFileBlocksReconstruction("/testRecoverOneDataBlock2", fileLen,
        ReconstructionType.DataOnly, 1);
  }

  @Test(timeout = 120000)
  public void testRecoverAnyBlocks() throws Exception {
    int fileLen = parityBlkNum * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAnyBlocks", fileLen,
        ReconstructionType.Any, random.nextInt(parityBlkNum) + 1);
  }

  @Test(timeout = 120000)
  public void testRecoverAnyBlocks1() throws Exception {
    int fileLen = (dataBlkNum + parityBlkNum) * blockSize + blockSize / 10;
    assertFileBlocksReconstruction("/testRecoverAnyBlocks1", fileLen,
        ReconstructionType.Any, random.nextInt(parityBlkNum) + 1);
  }

  private int[] generateDeadDnIndices(ReconstructionType type, int deadNum,
      byte[] indices) {
    List<Integer> deadList = new ArrayList<>(deadNum);
    while (deadList.size() < deadNum) {
      int dead = random.nextInt(indices.length);
      boolean isOfType = true;
      if (type == ReconstructionType.DataOnly) {
        isOfType = indices[dead] < dataBlkNum;
      } else if (type == ReconstructionType.ParityOnly) {
        isOfType = indices[dead] >= dataBlkNum;
      }
      if (isOfType && !deadList.contains(dead)) {
        deadList.add(dead);
      }
    }
    int[] d = new int[deadNum];
    for (int i = 0; i < deadNum; i++) {
      d[i] = deadList.get(i);
    }
    return d;
  }

  private void shutdownDataNode(DataNode dn) throws IOException {
    /*
     * Kill the datanode which contains one replica
     * We need to make sure it dead in namenode: clear its update time and
     * trigger NN to check heartbeat.
     */
    dn.shutdown();
    cluster.setDataNodeDead(dn.getDatanodeId());
  }

  private int generateErrors(Map<ExtendedBlock, DataNode> corruptTargets,
      ReconstructionType type)
    throws IOException {
    int stoppedDNs = 0;
    for (Map.Entry<ExtendedBlock, DataNode> target :
        corruptTargets.entrySet()) {
      if (stoppedDNs == 0 || type != ReconstructionType.DataOnly
          || random.nextBoolean()) {
        // stop at least one DN to trigger reconstruction
        LOG.info("Note: stop DataNode " + target.getValue().getDisplayName()
            + " with internal block " + target.getKey());
        shutdownDataNode(target.getValue());
        stoppedDNs++;
      } else { // corrupt the data on the DN
        LOG.info("Note: corrupt data on " + target.getValue().getDisplayName()
            + " with internal block " + target.getKey());
        cluster.corruptReplica(target.getValue(), target.getKey());
      }
    }
    return stoppedDNs;
  }

  private static void writeFile(DistributedFileSystem fs, String fileName,
      int fileLen) throws Exception {
    final byte[] data = new byte[fileLen];
    Arrays.fill(data, (byte) 1);
    DFSTestUtil.writeFile(fs, new Path(fileName), data);
    StripedFileTestUtil.waitBlockGroupsReported(fs, fileName);
  }

  /**
   * Test the file blocks reconstruction.
   * 1. Check the replica is reconstructed in the target datanode,
   *    and verify the block replica length, generationStamp and content.
   * 2. Read the file and verify content.
   */
  private void assertFileBlocksReconstruction(String fileName, int fileLen,
      ReconstructionType type, int toRecoverBlockNum) throws Exception {
    if (toRecoverBlockNum < 1 || toRecoverBlockNum > parityBlkNum) {
      Assert.fail("toRecoverBlockNum should be between 1 ~ " + parityBlkNum);
    }
    assertTrue("File length must be positive.", fileLen > 0);

    Path file = new Path(fileName);

    writeFile(fs, fileName, fileLen);

    LocatedBlocks locatedBlocks =
        StripedFileTestUtil.getLocatedBlocks(file, fs);
    assertEquals(locatedBlocks.getFileLength(), fileLen);

    LocatedStripedBlock lastBlock =
        (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();

    DatanodeInfo[] storageInfos = lastBlock.getLocations();
    byte[] indices = lastBlock.getBlockIndices();

    BitSet bitset = new BitSet(dnNum);
    for (DatanodeInfo storageInfo : storageInfos) {
      bitset.set(dnMap.get(storageInfo));
    }

    int[] dead = generateDeadDnIndices(type, toRecoverBlockNum, indices);
    LOG.info("Note: indices == " + Arrays.toString(indices)
        + ". Generate errors on datanodes: " + Arrays.toString(dead));

    DatanodeInfo[] dataDNs = new DatanodeInfo[toRecoverBlockNum];
    int[] deadDnIndices = new int[toRecoverBlockNum];
    ExtendedBlock[] blocks = new ExtendedBlock[toRecoverBlockNum];
    File[] replicas = new File[toRecoverBlockNum];
    long[] replicaLengths = new long[toRecoverBlockNum];
    File[] metadatas = new File[toRecoverBlockNum];
    byte[][] replicaContents = new byte[toRecoverBlockNum][];
    Map<ExtendedBlock, DataNode> errorMap = new HashMap<>(dead.length);
    for (int i = 0; i < toRecoverBlockNum; i++) {
      dataDNs[i] = storageInfos[dead[i]];
      deadDnIndices[i] = dnMap.get(dataDNs[i]);

      // Check the block replica file on deadDn before it dead.
      blocks[i] = StripedBlockUtil.constructInternalBlock(
          lastBlock.getBlock(), cellSize, dataBlkNum, indices[dead[i]]);
      errorMap.put(blocks[i], cluster.getDataNodes().get(deadDnIndices[i]));
      replicas[i] = cluster.getBlockFile(deadDnIndices[i], blocks[i]);
      replicaLengths[i] = replicas[i].length();
      metadatas[i] = cluster.getBlockMetadataFile(deadDnIndices[i], blocks[i]);
      // the block replica on the datanode should be the same as expected
      assertEquals(replicaLengths[i],
          StripedBlockUtil.getInternalBlockLength(
          lastBlock.getBlockSize(), cellSize, dataBlkNum, indices[dead[i]]));
      assertTrue(metadatas[i].getName().
          endsWith(blocks[i].getGenerationStamp() + ".meta"));
      LOG.info("replica " + i + " locates in file: " + replicas[i]);
      replicaContents[i] = DFSTestUtil.readFileAsBytes(replicas[i]);
    }

    int lastGroupDataLen = fileLen % (dataBlkNum * blockSize);
    int lastGroupNumBlk = lastGroupDataLen == 0 ? dataBlkNum :
        Math.min(dataBlkNum, ((lastGroupDataLen - 1) / cellSize + 1));
    int groupSize = lastGroupNumBlk + parityBlkNum;

    // shutdown datanodes or generate corruption
    int stoppedDN = generateErrors(errorMap, type);

    // Check the locatedBlocks of the file again
    locatedBlocks = StripedFileTestUtil.getLocatedBlocks(file, fs);
    lastBlock = (LocatedStripedBlock)locatedBlocks.getLastLocatedBlock();
    storageInfos = lastBlock.getLocations();
    assertEquals(storageInfos.length, groupSize - stoppedDN);

    int[] targetDNs = new int[dnNum - groupSize];
    int n = 0;
    for (int i = 0; i < dnNum; i++) {
      if (!bitset.get(i)) { // not contain replica of the block.
        targetDNs[n++] = i;
      }
    }

    StripedFileTestUtil.waitForReconstructionFinished(file, fs, groupSize);

    targetDNs = sortTargetsByReplicas(blocks, targetDNs);

    // Check the replica on the new target node.
    for (int i = 0; i < toRecoverBlockNum; i++) {
      File replicaAfterReconstruction = cluster.getBlockFile(targetDNs[i], blocks[i]);
      LOG.info("replica after reconstruction " + replicaAfterReconstruction);
      File metadataAfterReconstruction =
          cluster.getBlockMetadataFile(targetDNs[i], blocks[i]);
      assertEquals(replicaLengths[i], replicaAfterReconstruction.length());
      LOG.info("replica before " + replicas[i]);
      assertTrue(metadataAfterReconstruction.getName().
          endsWith(blocks[i].getGenerationStamp() + ".meta"));
      byte[] replicaContentAfterReconstruction =
          DFSTestUtil.readFileAsBytes(replicaAfterReconstruction);

      Assert.assertArrayEquals(replicaContents[i], replicaContentAfterReconstruction);
    }
  }

  private int[] sortTargetsByReplicas(ExtendedBlock[] blocks, int[] targetDNs) {
    int[] result = new int[blocks.length];
    for (int i = 0; i < blocks.length; i++) {
      result[i] = -1;
      for (int j = 0; j < targetDNs.length; j++) {
        if (targetDNs[j] != -1) {
          File replica = cluster.getBlockFile(targetDNs[j], blocks[i]);
          if (replica != null) {
            result[i] = targetDNs[j];
            targetDNs[j] = -1;
            break;
          }
        }
      }
      if (result[i] == -1) {
        Assert.fail("Failed to reconstruct striped block: "
            + blocks[i].getBlockId());
      }
    }
    return result;
  }

  /*
   * Tests that processErasureCodingTasks should not throw exceptions out due to
   * invalid ECTask submission.
   */
  @Test
  public void testProcessErasureCodingTasksSubmitionShouldSucceed()
      throws Exception {
    DataNode dataNode = cluster.dataNodes.get(0).datanode;

    // Pack invalid(dummy) parameters in ecTasks. Irrespective of parameters, each task
    // thread pool submission should succeed, so that it will not prevent
    // processing other tasks in the list if any exceptions.
    int size = cluster.dataNodes.size();
    byte[] liveIndices = new byte[size];
    DatanodeInfo[] dataDNs = new DatanodeInfo[size + 1];
    DatanodeStorageInfo targetDnInfos_1 = BlockManagerTestUtil
        .newDatanodeStorageInfo(DFSTestUtil.getLocalDatanodeDescriptor(),
            new DatanodeStorage("s01"));
    DatanodeStorageInfo[] dnStorageInfo = new DatanodeStorageInfo[] {
        targetDnInfos_1 };

    BlockECReconstructionInfo invalidECInfo = new BlockECReconstructionInfo(
        new ExtendedBlock("bp-id", 123456), dataDNs, dnStorageInfo, liveIndices,
        StripedFileTestUtil.getDefaultECPolicy());
    List<BlockECReconstructionInfo> ecTasks = new ArrayList<>();
    ecTasks.add(invalidECInfo);
    dataNode.getErasureCodingWorker().processErasureCodingTasks(ecTasks);
  }

  // HDFS-12044
  @Test(timeout = 60000)
  public void testNNSendsErasureCodingTasks() throws Exception {
    testNNSendsErasureCodingTasks(1);
    testNNSendsErasureCodingTasks(2);
  }

  private void testNNSendsErasureCodingTasks(int deadDN) throws Exception {
    cluster.shutdown();

    final int numDataNodes = dnNum  + 1;
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY, 10);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MAX_STREAMS_KEY, 20);
    conf.setInt(DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_THREADS_KEY,
        2);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(numDataNodes).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    ErasureCodingPolicy policy = StripedFileTestUtil.getDefaultECPolicy();
    fs.getClient().setErasureCodingPolicy("/", policy.getName());

    final int fileLen = cellSize * ecPolicy.getNumDataUnits();
    for (int i = 0; i < 50; i++) {
      writeFile(fs, "/ec-file-" + i, fileLen);
    }

    // Inject data-loss by tear down desired number of DataNodes.
    assertTrue(policy.getNumParityUnits() >= deadDN);
    List<DataNode> dataNodes = new ArrayList<>(cluster.getDataNodes());
    Collections.shuffle(dataNodes);
    for (DataNode dn : dataNodes.subList(0, deadDN)) {
      shutdownDataNode(dn);
    }

    final FSNamesystem ns = cluster.getNamesystem();
    GenericTestUtils.waitFor(() -> ns.getPendingDeletionBlocks() == 0,
        500, 30000);

    // Make sure that all pending reconstruction tasks can be processed.
    while (ns.getPendingReconstructionBlocks() > 0) {
      long timeoutPending = ns.getNumTimedOutPendingReconstructions();
      assertTrue(String.format("Found %d timeout pending reconstruction tasks",
          timeoutPending), timeoutPending == 0);
      Thread.sleep(1000);
    }

    // Verify all DN reaches zero xmitsInProgress.
    GenericTestUtils.waitFor(() ->
        cluster.getDataNodes().stream().mapToInt(
            DataNode::getXmitsInProgress).sum() == 0,
        500, 30000
    );
  }
}
