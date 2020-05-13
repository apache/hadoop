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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManagerTestUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the decommissioning of datanode with striped blocks.
 */
public class TestDecommissionWithStriped {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestDecommissionWithStriped.class);

  // heartbeat interval in seconds
  private static final int HEARTBEAT_INTERVAL = 1;
  // block report in msec
  private static final int BLOCKREPORT_INTERVAL_MSEC = 1000;
  // replication interval
  private static final int NAMENODE_REPLICATION_INTERVAL = 1;

  private int replicationStreamsHardLimit =
      DFSConfigKeys.DFS_NAMENODE_REPLICATION_STREAMS_HARD_LIMIT_DEFAULT;

  private Path decommissionDir;
  private Path hostsFile;
  private Path excludeFile;
  private FileSystem localFileSys;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private final ErasureCodingPolicy ecPolicy =
      StripedFileTestUtil.getDefaultECPolicy();
  private int numDNs;
  private final int cellSize = ecPolicy.getCellSize();
  private final int dataBlocks = ecPolicy.getNumDataUnits();
  private final int parityBlocks = ecPolicy.getNumParityUnits();
  private final int blockSize = cellSize * 4;
  private final int blockGroupSize = blockSize * dataBlocks;
  private final Path ecDir = new Path("/" + this.getClass().getSimpleName());

  private FSNamesystem fsn;
  private BlockManager bm;
  private DFSClient client;

  protected Configuration createConfiguration() {
    return new HdfsConfiguration();
  }

  @Before
  public void setup() throws IOException {
    conf = createConfiguration();
    // Set up the hosts/exclude files.
    localFileSys = FileSystem.getLocal(conf);
    Path workingDir = localFileSys.getWorkingDirectory();
    decommissionDir = new Path(workingDir,
        PathUtils.getTestDirName(getClass()) + "/work-dir/decommission");
    hostsFile = new Path(decommissionDir, "hosts");
    excludeFile = new Path(decommissionDir, "exclude");
    writeConfigFile(hostsFile, null);
    writeConfigFile(excludeFile, null);

    // Setup conf
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        BLOCKREPORT_INTERVAL_MSEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
        4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        NAMENODE_REPLICATION_INTERVAL);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        cellSize - 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);

    numDNs = dataBlocks + parityBlocks + 5;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem(0);
    fsn = cluster.getNamesystem();
    bm = fsn.getBlockManager();
    client = getDfsClient(cluster.getNameNode(0), conf);

    dfs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    dfs.mkdirs(ecDir);
    dfs.setErasureCodingPolicy(ecDir,
        StripedFileTestUtil.getDefaultECPolicy().getName());
  }

  @After
  public void teardown() throws IOException {
    cleanupFile(localFileSys, decommissionDir);
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test(timeout = 120000)
  public void testFileFullBlockGroup() throws Exception {
    LOG.info("Starting test testFileFullBlockGroup");
    testDecommission(blockSize * dataBlocks, 9, 1, "testFileFullBlockGroup");
  }

  @Test(timeout = 120000)
  public void testFileMultipleBlockGroups() throws Exception {
    LOG.info("Starting test testFileMultipleBlockGroups");
    int writeBytes = 2 * blockSize * dataBlocks;
    testDecommission(writeBytes, 9, 1, "testFileMultipleBlockGroups");
  }

  @Test(timeout = 120000)
  public void testFileSmallerThanOneCell() throws Exception {
    LOG.info("Starting test testFileSmallerThanOneCell");
    testDecommission(cellSize - 1, 4, 1, "testFileSmallerThanOneCell");
  }

  @Test(timeout = 120000)
  public void testFileSmallerThanOneStripe() throws Exception {
    LOG.info("Starting test testFileSmallerThanOneStripe");
    testDecommission(cellSize * 2, 5, 1, "testFileSmallerThanOneStripe");
  }

  @Test(timeout = 120000)
  public void testDecommissionTwoNodes() throws Exception {
    LOG.info("Starting test testDecommissionTwoNodes");
    testDecommission(blockSize * dataBlocks, 9, 2, "testDecommissionTwoNodes");
  }

  @Test(timeout = 120000)
  public void testDecommissionWithURBlockForSameBlockGroup() throws Exception {
    LOG.info("Starting test testDecommissionWithURBlocksForSameBlockGroup");

    final Path ecFile = new Path(ecDir, "testDecommissionWithCorruptBlocks");
    int writeBytes = cellSize * dataBlocks * 2;
    writeStripedFile(dfs, ecFile, writeBytes);
    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());

    final List<DatanodeInfo> decommisionNodes = new ArrayList<DatanodeInfo>();
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(ecFile.toString(), 0)
        .get(0);
    DatanodeInfo[] dnLocs = lb.getLocations();
    assertEquals(dataBlocks + parityBlocks, dnLocs.length);
    int decommNodeIndex = dataBlocks - 1;
    int stopNodeIndex = 1;

    // add the nodes which will be decommissioning
    decommisionNodes.add(dnLocs[decommNodeIndex]);

    // stop excess dns to avoid immediate reconstruction.
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    List<DataNodeProperties> stoppedDns = new ArrayList<>();
    for (DatanodeInfo liveDn : info) {
      boolean usedNode = false;
      for (DatanodeInfo datanodeInfo : dnLocs) {
        if (liveDn.getXferAddr().equals(datanodeInfo.getXferAddr())) {
          usedNode = true;
          break;
        }
      }
      if (!usedNode) {
        DataNode dn = cluster.getDataNode(liveDn.getIpcPort());
        stoppedDns.add(cluster.stopDataNode(liveDn.getXferAddr()));
        cluster.setDataNodeDead(dn.getDatanodeId());
        LOG.info("stop datanode " + dn.getDatanodeId().getHostName());
      }
    }
    DataNode dn = cluster.getDataNode(dnLocs[stopNodeIndex].getIpcPort());
    cluster.stopDataNode(dnLocs[stopNodeIndex].getXferAddr());
    cluster.setDataNodeDead(dn.getDatanodeId());
    numDNs = numDNs - 1;

    // Decommission node in a new thread. Verify that node is decommissioned.
    final CountDownLatch decomStarted = new CountDownLatch(0);
    Thread decomTh = new Thread() {
      public void run() {
        try {
          decomStarted.countDown();
          decommissionNode(0, decommisionNodes, AdminStates.DECOMMISSIONED);
        } catch (Exception e) {
          LOG.error("Exception while decommissioning", e);
          Assert.fail("Shouldn't throw exception!");
        }
      };
    };
    int deadDecommissioned = fsn.getNumDecomDeadDataNodes();
    int liveDecommissioned = fsn.getNumDecomLiveDataNodes();
    decomTh.start();
    decomStarted.await(5, TimeUnit.SECONDS);
    Thread.sleep(3000); // grace period to trigger decommissioning call
    // start datanode so that decommissioning live node will be finished
    for (DataNodeProperties dnp : stoppedDns) {
      cluster.restartDataNode(dnp);
      LOG.info("Restarts stopped datanode:{} to trigger block reconstruction",
          dnp.datanode);
    }
    cluster.waitActive();

    LOG.info("Waiting to finish decommissioning node:{}", decommisionNodes);
    decomTh.join(20000); // waiting 20secs to finish decommission
    LOG.info("Finished decommissioning node:{}", decommisionNodes);

    assertEquals(deadDecommissioned, fsn.getNumDecomDeadDataNodes());
    assertEquals(liveDecommissioned + decommisionNodes.size(),
        fsn.getNumDecomLiveDataNodes());

    // Ensure decommissioned datanode is not automatically shutdown
    assertEquals("All datanodes must be alive", numDNs,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    assertNull(checkFile(dfs, ecFile, 9, decommisionNodes, numDNs));
    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes, decommisionNodes,
        null, blockGroupSize);
    cleanupFile(dfs, ecFile);
  }

  /**
   * DN decommission shouldn't reconstruction busy DN block.
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testDecommissionWithBusyNode() throws Exception {
    byte busyDNIndex = 1;
    byte decommisionDNIndex = 0;
    //1. create EC file
    final Path ecFile = new Path(ecDir, "testDecommissionWithBusyNode");
    int writeBytes = cellSize * dataBlocks;
    writeStripedFile(dfs, ecFile, writeBytes);
    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());
    FileChecksum fileChecksum1 = dfs.getFileChecksum(ecFile, writeBytes);

    //2. make once DN busy
    final INodeFile fileNode = cluster.getNamesystem().getFSDirectory()
        .getINode4Write(ecFile.toString()).asFile();
    BlockInfo firstBlock = fileNode.getBlocks()[0];
    DatanodeStorageInfo[] dnStorageInfos = bm.getStorages(firstBlock);
    DatanodeDescriptor busyNode =
        dnStorageInfos[busyDNIndex].getDatanodeDescriptor();
    for (int j = 0; j < replicationStreamsHardLimit; j++) {
      busyNode.incrementPendingReplicationWithoutTargets();
    }

    //3. decomission one node
    List<DatanodeInfo> decommisionNodes = new ArrayList<>();
    decommisionNodes.add(
        dnStorageInfos[decommisionDNIndex].getDatanodeDescriptor());
    decommissionNode(0, decommisionNodes, AdminStates.DECOMMISSIONED);
    assertEquals(decommisionNodes.size(), fsn.getNumDecomLiveDataNodes());

    //4. wait for decommission block to replicate
    Thread.sleep(3000);
    DatanodeStorageInfo[] newDnStorageInfos = bm.getStorages(firstBlock);
    Assert.assertEquals("Busy DN shouldn't be reconstructed",
        dnStorageInfos[busyDNIndex].getStorageID(),
        newDnStorageInfos[busyDNIndex].getStorageID());

    //5. check decommission DN block index, it should be reconstructed again
    LocatedBlocks lbs = cluster.getNameNodeRpc().getBlockLocations(
        ecFile.toString(), 0, writeBytes);
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));
    int decommissionBlockIndexCount = 0;
    for (byte index : bg.getBlockIndices()) {
      if (index == decommisionDNIndex) {
        decommissionBlockIndexCount++;
      }
    }

    Assert.assertEquals("Decommission DN block should be reconstructed", 2,
        decommissionBlockIndexCount);

    FileChecksum fileChecksum2 = dfs.getFileChecksum(ecFile, writeBytes);
    Assert.assertTrue("Checksum mismatches!",
        fileChecksum1.equals(fileChecksum2));
  }

  /**
   * Decommission may generate the parity block's content with all 0
   * in some case.
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testDecommission2NodeWithBusyNode() throws Exception {
    byte busyDNIndex = 6;
    byte decommissionDNIndex = 6;
    byte decommissionDNIndex2 = 8;
    //1. create EC file
    final Path ecFile = new Path(ecDir, "testDecommission2NodeWithBusyNode");
    int writeBytes = cellSize * dataBlocks;
    writeStripedFile(dfs, ecFile, writeBytes);

    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());
    FileChecksum fileChecksum1 = dfs.getFileChecksum(ecFile, writeBytes);

    //2. make once DN busy
    final INodeFile fileNode = cluster.getNamesystem().getFSDirectory()
        .getINode4Write(ecFile.toString()).asFile();
    BlockInfo firstBlock = fileNode.getBlocks()[0];
    DatanodeStorageInfo[] dnStorageInfos = bm.getStorages(firstBlock);
    DatanodeDescriptor busyNode = dnStorageInfos[busyDNIndex]
        .getDatanodeDescriptor();
    for (int j = 0; j < replicationStreamsHardLimit; j++) {
      busyNode.incrementPendingReplicationWithoutTargets();
    }

    //3. decommissioning one node
    List<DatanodeInfo> decommissionNodes = new ArrayList<>();
    decommissionNodes.add(dnStorageInfos[decommissionDNIndex]
        .getDatanodeDescriptor());
    decommissionNodes.add(dnStorageInfos[decommissionDNIndex2]
        .getDatanodeDescriptor());
    decommissionNode(0, decommissionNodes, AdminStates.DECOMMISSION_INPROGRESS);

    //4. wait for decommissioning and not busy block to replicate(9-2+1=8)
    GenericTestUtils.waitFor(
        () -> bm.countNodes(firstBlock).liveReplicas() >= 8,
        100, 60000);

    //5. release busy DN, make the decommissioning and busy block can replicate
    busyNode.decrementPendingReplicationWithoutTargets();

    //6. decommissioned one node,make the decommission finished
    decommissionNode(0, decommissionNodes, AdminStates.DECOMMISSIONED);

    //7. Busy DN shouldn't be reconstructed
    DatanodeStorageInfo[] newDnStorageInfos = bm.getStorages(firstBlock);
    Assert.assertEquals("Busy DN shouldn't be reconstructed",
        dnStorageInfos[busyDNIndex].getStorageID(),
        newDnStorageInfos[busyDNIndex].getStorageID());

    //8. check the checksum of a file
    FileChecksum fileChecksum2 = dfs.getFileChecksum(ecFile, writeBytes);
    Assert.assertEquals("Checksum mismatches!", fileChecksum1, fileChecksum2);

    //9. check the data is correct
    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes, decommissionNodes,
        null, blockGroupSize);
  }

  /**
   * Tests to verify that the file checksum should be able to compute after the
   * decommission operation.
   *
   * Below is the block indices list after the decommission. ' represents
   * decommissioned node index.
   *
   * 0, 2, 3, 4, 5, 6, 7, 8, 1, 1'
   *
   * Here, this list contains duplicated blocks and does not maintaining any
   * order.
   */
  @Test(timeout = 120000)
  public void testFileChecksumAfterDecommission() throws Exception {
    LOG.info("Starting test testFileChecksumAfterDecommission");

    final Path ecFile = new Path(ecDir, "testFileChecksumAfterDecommission");
    int writeBytes = cellSize * dataBlocks;
    writeStripedFile(dfs, ecFile, writeBytes);
    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());
    FileChecksum fileChecksum1 = dfs.getFileChecksum(ecFile, writeBytes);

    final List<DatanodeInfo> decommisionNodes = new ArrayList<DatanodeInfo>();
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(ecFile.toString(), 0)
        .get(0);
    DatanodeInfo[] dnLocs = lb.getLocations();
    assertEquals(dataBlocks + parityBlocks, dnLocs.length);
    int decommNodeIndex = 1;

    // add the node which will be decommissioning
    decommisionNodes.add(dnLocs[decommNodeIndex]);
    decommissionNode(0, decommisionNodes, AdminStates.DECOMMISSIONED);
    assertEquals(decommisionNodes.size(), fsn.getNumDecomLiveDataNodes());
    assertNull(checkFile(dfs, ecFile, 9, decommisionNodes, numDNs));
    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes, decommisionNodes,
        null, blockGroupSize);

    // verify checksum
    FileChecksum fileChecksum2 = dfs.getFileChecksum(ecFile, writeBytes);
    LOG.info("fileChecksum1:" + fileChecksum1);
    LOG.info("fileChecksum2:" + fileChecksum2);

    Assert.assertTrue("Checksum mismatches!",
        fileChecksum1.equals(fileChecksum2));
  }

  private void testDecommission(int writeBytes, int storageCount,
      int decomNodeCount, String filename) throws IOException, Exception {
    Path ecFile = new Path(ecDir, filename);
    writeStripedFile(dfs, ecFile, writeBytes);
    List<DatanodeInfo> decommisionNodes = getDecommissionDatanode(dfs, ecFile,
        writeBytes, decomNodeCount);

    int deadDecommissioned = fsn.getNumDecomDeadDataNodes();
    int liveDecommissioned = fsn.getNumDecomLiveDataNodes();
    List<LocatedBlock> lbs = ((HdfsDataInputStream) dfs.open(ecFile))
        .getAllBlocks();

    // prepare expected block index and token list.
    List<HashMap<DatanodeInfo, Byte>> locToIndexList = new ArrayList<>();
    List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList =
        new ArrayList<>();
    prepareBlockIndexAndTokenList(lbs, locToIndexList, locToTokenList);

    // Decommission node. Verify that node is decommissioned.
    decommissionNode(0, decommisionNodes, AdminStates.DECOMMISSIONED);

    assertEquals(deadDecommissioned, fsn.getNumDecomDeadDataNodes());
    assertEquals(liveDecommissioned + decommisionNodes.size(),
        fsn.getNumDecomLiveDataNodes());

    // Ensure decommissioned datanode is not automatically shutdown
    DFSClient client = getDfsClient(cluster.getNameNode(0), conf);
    assertEquals("All datanodes must be alive", numDNs,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    assertNull(checkFile(dfs, ecFile, storageCount, decommisionNodes, numDNs));
    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes, decommisionNodes,
        null, blockGroupSize);

    assertBlockIndexAndTokenPosition(lbs, locToIndexList, locToTokenList);

    cleanupFile(dfs, ecFile);
  }

  private void prepareBlockIndexAndTokenList(List<LocatedBlock> lbs,
      List<HashMap<DatanodeInfo, Byte>> locToIndexList,
      List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList) {
    for (LocatedBlock lb : lbs) {
      HashMap<DatanodeInfo, Byte> locToIndex = new HashMap<DatanodeInfo, Byte>();
      locToIndexList.add(locToIndex);

      HashMap<DatanodeInfo, Token<BlockTokenIdentifier>> locToToken =
          new HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>();
      locToTokenList.add(locToToken);

      DatanodeInfo[] di = lb.getLocations();
      LocatedStripedBlock stripedBlk = (LocatedStripedBlock) lb;
      for (int i = 0; i < di.length; i++) {
        locToIndex.put(di[i], stripedBlk.getBlockIndices()[i]);
        locToToken.put(di[i], stripedBlk.getBlockTokens()[i]);
      }
    }
  }

  /**
   * Verify block index and token values. Must update block indices and block
   * tokens after sorting.
   */
  private void assertBlockIndexAndTokenPosition(List<LocatedBlock> lbs,
      List<HashMap<DatanodeInfo, Byte>> locToIndexList,
      List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList) {
    for (int i = 0; i < lbs.size(); i++) {
      LocatedBlock lb = lbs.get(i);
      LocatedStripedBlock stripedBlk = (LocatedStripedBlock) lb;
      HashMap<DatanodeInfo, Byte> locToIndex = locToIndexList.get(i);
      HashMap<DatanodeInfo, Token<BlockTokenIdentifier>> locToToken =
          locToTokenList.get(i);
      DatanodeInfo[] di = lb.getLocations();
      for (int j = 0; j < di.length; j++) {
        Assert.assertEquals("Block index value mismatches after sorting",
            (byte) locToIndex.get(di[j]), stripedBlk.getBlockIndices()[j]);
        Assert.assertEquals("Block token value mismatches after sorting",
            locToToken.get(di[j]), stripedBlk.getBlockTokens()[j]);
      }
    }
  }

  private List<DatanodeInfo> getDecommissionDatanode(DistributedFileSystem dfs,
      Path ecFile, int writeBytes, int decomNodeCount) throws IOException {
    ArrayList<DatanodeInfo> decommissionedNodes = new ArrayList<>();
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    BlockLocation[] fileBlockLocations = dfs.getFileBlockLocations(ecFile, 0,
        writeBytes);
    for (String dnName : fileBlockLocations[0].getNames()) {
      for (DatanodeInfo dn : info) {
        if (dnName.equals(dn.getXferAddr())) {
          decommissionedNodes.add(dn);
        }
        if (decommissionedNodes.size() >= decomNodeCount) {
          return decommissionedNodes;
        }
      }
    }
    return decommissionedNodes;
  }

  /* Get DFSClient to the namenode */
  private static DFSClient getDfsClient(NameNode nn, Configuration conf)
      throws IOException {
    return new DFSClient(nn.getNameNodeAddress(), conf);
  }

  private byte[] writeStripedFile(DistributedFileSystem fs, Path ecFile,
      int writeBytes) throws Exception {
    byte[] bytes = StripedFileTestUtil.generateBytes(writeBytes);
    DFSTestUtil.writeFile(fs, ecFile, new String(bytes));
    StripedFileTestUtil.waitBlockGroupsReported(fs, ecFile.toString());

    StripedFileTestUtil.checkData(fs, ecFile, writeBytes,
        new ArrayList<DatanodeInfo>(), null, blockGroupSize);
    return bytes;
  }

  private void writeConfigFile(Path name, List<String> nodes)
      throws IOException {
    // delete if it already exists
    if (localFileSys.exists(name)) {
      localFileSys.delete(name, true);
    }

    FSDataOutputStream stm = localFileSys.create(name);

    if (nodes != null) {
      for (Iterator<String> it = nodes.iterator(); it.hasNext();) {
        String node = it.next();
        stm.writeBytes(node);
        stm.writeBytes("\n");
      }
    }
    stm.close();
  }

  private void cleanupFile(FileSystem fileSys, Path name) throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertTrue(!fileSys.exists(name));
  }

  /*
   * decommission the DN at index dnIndex or one random node if dnIndex is set
   * to -1 and wait for the node to reach the given {@code waitForState}.
   */
  private void decommissionNode(int nnIndex,
      List<DatanodeInfo> decommissionedNodes, AdminStates waitForState)
          throws IOException {
    DFSClient client = getDfsClient(cluster.getNameNode(nnIndex), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    // write nodename into the exclude file.
    ArrayList<String> excludeNodes = new ArrayList<String>();
    for (DatanodeInfo dn : decommissionedNodes) {
      boolean nodeExists = false;
      for (DatanodeInfo dninfo : info) {
        if (dninfo.getDatanodeUuid().equals(dn.getDatanodeUuid())) {
          nodeExists = true;
          break;
        }
      }
      assertTrue("Datanode: " + dn + " is not LIVE", nodeExists);
      excludeNodes.add(dn.getName());
      LOG.info("Decommissioning node: " + dn.getName());
    }
    writeConfigFile(excludeFile, excludeNodes);
    refreshNodes(cluster.getNamesystem(nnIndex), conf);
    for (DatanodeInfo dn : decommissionedNodes) {
      DatanodeInfo ret = NameNodeAdapter
          .getDatanode(cluster.getNamesystem(nnIndex), dn);
      waitNodeState(ret, waitForState);
    }
  }

  private static void refreshNodes(final FSNamesystem ns,
      final Configuration conf) throws IOException {
    ns.getBlockManager().getDatanodeManager().refreshNodes(conf);
  }

  /*
   * Wait till node is fully decommissioned.
   */
  private void waitNodeState(DatanodeInfo node, AdminStates state) {
    boolean done = state == node.getAdminState();
    while (!done) {
      LOG.info("Waiting for node " + node + " to change state to " + state
          + " current state: " + node.getAdminState());
      try {
        Thread.sleep(HEARTBEAT_INTERVAL * 500);
      } catch (InterruptedException e) {
        // nothing
      }
      done = state == node.getAdminState();
    }
    LOG.info("node " + node + " reached the state " + state);
  }

  /**
   * Verify that the number of replicas are as expected for each block in the
   * given file. For blocks with a decommissioned node, verify that their
   * replication is 1 more than what is specified. For blocks without
   * decommissioned nodes, verify their replication is equal to what is
   * specified.
   *
   * @param decommissionedNodes
   *          - if null, there is no decommissioned node for this file.
   * @return - null if no failure found, else an error message string.
   */
  private static String checkFile(FileSystem fileSys, Path name, int repl,
      List<DatanodeInfo> decommissionedNodes, int numDatanodes)
          throws IOException {
    boolean isNodeDown = decommissionedNodes.size() > 0;
    // need a raw stream
    assertTrue("Not HDFS:" + fileSys.getUri(),
        fileSys instanceof DistributedFileSystem);
    HdfsDataInputStream dis = (HdfsDataInputStream) fileSys.open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();
    for (LocatedBlock blk : dinfo) { // for each block
      int hasdown = 0;
      DatanodeInfo[] nodes = blk.getLocations();
      for (int j = 0; j < nodes.length; j++) { // for each replica
        LOG.info("Block Locations size={}, locs={}, j=", nodes.length,
            nodes[j].toString(), j);
        boolean found = false;
        for (DatanodeInfo datanodeInfo : decommissionedNodes) {
          // check against decommissioned list
          if (isNodeDown
              && nodes[j].getXferAddr().equals(datanodeInfo.getXferAddr())) {
            found = true;
            hasdown++;
            // Downnode must actually be decommissioned
            if (!nodes[j].isDecommissioned()) {
              return "For block " + blk.getBlock() + " replica on " + nodes[j]
                  + " is given as downnode, " + "but is not decommissioned";
            }
            // Decommissioned node (if any) should only be last node in list.
            if (j < repl) {
              return "For block " + blk.getBlock() + " decommissioned node "
                  + nodes[j] + " was not last node in list: " + (j + 1) + " of "
                  + nodes.length;
            }
            // should only be last node in list.
            LOG.info("Block " + blk.getBlock() + " replica on " + nodes[j]
                + " is decommissioned.");
          }
        }
        // Non-downnodes must not be decommissioned
        if (!found && nodes[j].isDecommissioned()) {
          return "For block " + blk.getBlock() + " replica on " + nodes[j]
              + " is unexpectedly decommissioned";
        }
      }

      LOG.info("Block " + blk.getBlock() + " has " + hasdown
          + " decommissioned replica.");
      if (Math.min(numDatanodes, repl + hasdown) != nodes.length) {
        return "Wrong number of replicas for block " + blk.getBlock() + ": "
            + nodes.length + ", expected "
            + Math.min(numDatanodes, repl + hasdown);
      }
    }
    return null;
  }

  /**
   * Simulate that There are 2 nodes(dn0,dn1) in decommission. Firstly dn0
   * replicates in success, dn1 replicates in failure. Decommissions go on.
   */
  @Test (timeout = 120000)
  public void testDecommissionWithFailedReplicating() throws Exception {

    // Write ec file.
    Path ecFile = new Path(ecDir, "firstReplicationFailedFile");
    int writeBytes = cellSize * 6;
    writeStripedFile(dfs, ecFile, writeBytes);

    // Get 2 nodes of ec block and set them in decommission.
    // The 2 nodes are not in pendingNodes of DatanodeAdminManager.
    List<LocatedBlock> lbs = ((HdfsDataInputStream) dfs.open(ecFile))
        .getAllBlocks();
    LocatedStripedBlock blk = (LocatedStripedBlock) lbs.get(0);
    DatanodeInfo[] dnList = blk.getLocations();
    DatanodeDescriptor dn0 = bm.getDatanodeManager()
        .getDatanode(dnList[0].getDatanodeUuid());
    dn0.startDecommission();
    DatanodeDescriptor dn1 = bm.getDatanodeManager()
        .getDatanode(dnList[1].getDatanodeUuid());
    dn1.startDecommission();

    assertEquals(0, bm.getDatanodeManager().getDatanodeAdminManager()
        .getNumPendingNodes());

    // Replicate dn0 block to another dn
    // Simulate that dn0 replicates in success, dn1 replicates in failure.
    final byte blockIndex = blk.getBlockIndices()[0];
    final Block targetBlk = new Block(blk.getBlock().getBlockId() + blockIndex,
        cellSize, blk.getBlock().getGenerationStamp());
    DatanodeInfo extraDn = getDatanodeOutOfTheBlock(blk);
    DatanodeDescriptor target = bm.getDatanodeManager()
        .getDatanode(extraDn.getDatanodeUuid());
    dn0.addBlockToBeReplicated(targetBlk,
        new DatanodeStorageInfo[] {target.getStorageInfos()[0]});

    // dn0 replicates in success
    GenericTestUtils.waitFor(
        () -> dn0.getNumberOfReplicateBlocks() == 0,
        100, 60000);
    GenericTestUtils.waitFor(
        () -> {
          Iterator<DatanodeStorageInfo> it =
              bm.getStoredBlock(targetBlk).getStorageInfos();
          while(it.hasNext()) {
            if (it.next().getDatanodeDescriptor().equals(target)) {
              return true;
            }
          }
          return false;
        },
        100, 60000);

    // There are 8 live replicas
    BlockInfoStriped blockInfo =
        (BlockInfoStriped)bm.getStoredBlock(
            new Block(blk.getBlock().getBlockId()));
    assertEquals(8, bm.countNodes(blockInfo).liveReplicas());

    // Add the 2 nodes to pendingNodes of DatanodeAdminManager
    bm.getDatanodeManager().getDatanodeAdminManager()
        .getPendingNodes().add(dn0);
    bm.getDatanodeManager().getDatanodeAdminManager()
        .getPendingNodes().add(dn1);

    waitNodeState(dn0, AdminStates.DECOMMISSIONED);
    waitNodeState(dn1, AdminStates.DECOMMISSIONED);

    // There are 9 live replicas
    assertEquals(9, bm.countNodes(blockInfo).liveReplicas());

    // After dn0 & dn1 decommissioned, all internal Blocks(0~8) are there
    Iterator<DatanodeStorageInfo> it = blockInfo.getStorageInfos();
    BitSet indexBitSet = new BitSet(9);
    while(it.hasNext()) {
      DatanodeStorageInfo storageInfo = it.next();
      if(storageInfo.getDatanodeDescriptor().equals(dn0)
          || storageInfo.getDatanodeDescriptor().equals(dn1)) {
        // Skip decommissioned nodes
        continue;
      }
      byte index = blockInfo.getStorageBlockIndex(storageInfo);
      indexBitSet.set(index);
    }
    for (int i = 0; i < 9; ++i) {
      assertEquals(true, indexBitSet.get(i));
    }
  }

  /**
   * Get a Datanode which does not contain the block.
   */
  private DatanodeInfo getDatanodeOutOfTheBlock(LocatedStripedBlock blk)
      throws Exception {
    DatanodeInfo[] allDnInfos = client.datanodeReport(DatanodeReportType.LIVE);
    DatanodeInfo[] blkDnInos= blk.getLocations();
    for (DatanodeInfo dnInfo : allDnInfos) {
      boolean in = false;
      for (DatanodeInfo blkDnInfo : blkDnInos) {
        if (blkDnInfo.equals(dnInfo)) {
          in = true;
        }
      }
      if(!in) {
        return dnInfo;
      }
    }
    return null;
  }

  @Test (timeout = 120000)
  public void testDecommissionWithMissingBlock() throws Exception {
    // Write ec file.
    Path ecFile = new Path(ecDir, "missingOneInternalBLockFile");
    int writeBytes = cellSize * 6;
    writeStripedFile(dfs, ecFile, writeBytes);

    final List<DatanodeInfo> decommisionNodes = new ArrayList<DatanodeInfo>();
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(ecFile.toString(), 0)
        .get(0);
    LocatedStripedBlock lsb = (LocatedStripedBlock)lb;
    DatanodeInfo[] dnLocs = lsb.getLocations();
    BlockInfoStriped blockInfo =
        (BlockInfoStriped)bm.getStoredBlock(
            new Block(lsb.getBlock().getBlockId()));

    assertEquals(dataBlocks + parityBlocks, dnLocs.length);
    int decommNodeIndex = 1;
    int numDecommission= 4;
    int stopNodeIndex = 0;

    // Add the 4 nodes, and set the 4 nodes decommissioning.
    // So that they are decommissioning at the same time
    for (int i = decommNodeIndex; i < numDecommission + decommNodeIndex; ++i) {
      decommisionNodes.add(dnLocs[i]);
      DatanodeDescriptor dn = bm.getDatanodeManager()
          .getDatanode(dnLocs[i].getDatanodeUuid());
      dn.startDecommission();
    }
    GenericTestUtils.waitFor(
        () -> bm.countNodes(blockInfo).decommissioning() == numDecommission,
        100, 10000);

    // Namenode does not handle decommissioning nodes now
    assertEquals(0, bm.getDatanodeManager().getDatanodeAdminManager()
        .getNumPendingNodes());

    // Replicate dn1 block to another dn
    // So that one of the 4 replicas has been replicated.
    final byte blockIndex = lsb.getBlockIndices()[decommNodeIndex];
    final Block targetBlk = new Block(lsb.getBlock().getBlockId() + blockIndex,
        cellSize, lsb.getBlock().getGenerationStamp());
    DatanodeInfo extraDn = getDatanodeOutOfTheBlock(lsb);
    DatanodeDescriptor target = bm.getDatanodeManager()
        .getDatanode(extraDn.getDatanodeUuid());
    DatanodeDescriptor dnStartIndexDecommission = bm.getDatanodeManager()
        .getDatanode(dnLocs[decommNodeIndex].getDatanodeUuid());
    dnStartIndexDecommission.addBlockToBeReplicated(targetBlk,
        new DatanodeStorageInfo[] {target.getStorageInfos()[0]});

    // Wait for replication success.
    GenericTestUtils.waitFor(
        () -> {
          Iterator<DatanodeStorageInfo> it =
              bm.getStoredBlock(targetBlk).getStorageInfos();
          while(it.hasNext()) {
            if (it.next().getDatanodeDescriptor().equals(target)) {
              return true;
            }
          }
          return false;
        },
        100, 60000);

    // Reopen ecFile, get the new locations.
    lb = dfs.getClient().getLocatedBlocks(ecFile.toString(), 0)
        .get(0);
    lsb = (LocatedStripedBlock)lb;
    DatanodeInfo[] newDnLocs = lsb.getLocations();

    // Now the block has 10 internal blocks.
    assertEquals(10, newDnLocs.length);

    // Stop the dn0(stopNodeIndex) datanode
    // So that the internal block from this dn misses
    DataNode dn = cluster.getDataNode(dnLocs[stopNodeIndex].getIpcPort());
    cluster.stopDataNode(dnLocs[stopNodeIndex].getXferAddr());
    cluster.setDataNodeDead(dn.getDatanodeId());

    // So far, there are 4 decommissioning nodes, 1 replica has been
    // replicated, and 1 replica misses. There are 8 total internal
    // blocks, 5 live and 3 decommissioning internal blocks.
    assertEquals(5, bm.countNodes(blockInfo).liveReplicas());
    assertEquals(3, bm.countNodes(blockInfo).decommissioning());

    // Handle decommission nodes in a new thread.
    // Verify that nodes are decommissioned.
    final CountDownLatch decomStarted = new CountDownLatch(0);
    new Thread(
        () -> {
          try {
            decomStarted.countDown();
            decommissionNode(0, decommisionNodes, AdminStates.DECOMMISSIONED);
          } catch (Exception e) {
            LOG.error("Exception while decommissioning", e);
            Assert.fail("Shouldn't throw exception!");
          }
        }).start();
    decomStarted.await(5, TimeUnit.SECONDS);

    // Wake up to reconstruct the block.
    BlockManagerTestUtil.wakeupPendingReconstructionTimerThread(bm);

    // Wait for decommissioning
    GenericTestUtils.waitFor(
        // Whether there are 8 live replicas after decommission.
        () -> bm.countNodes(blockInfo).liveReplicas() == 9,
        100, 60000);

    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes, decommisionNodes,
        null, blockGroupSize);
    cleanupFile(dfs, ecFile);
  }

  @Test (timeout = 120000)
  public void testCountNodes() throws Exception{
    // Write ec file.
    Path ecFile = new Path(ecDir, "testCountNodes");
    int writeBytes = cellSize * 6;
    writeStripedFile(dfs, ecFile, writeBytes);

    List<LocatedBlock> lbs = ((HdfsDataInputStream) dfs.open(ecFile))
        .getAllBlocks();
    LocatedStripedBlock blk = (LocatedStripedBlock) lbs.get(0);
    DatanodeInfo[] dnList = blk.getLocations();
    DatanodeDescriptor dn0 = bm.getDatanodeManager()
        .getDatanode(dnList[0].getDatanodeUuid());
    dn0.startDecommission();

    // Replicate dn0 block to another dn
    final byte blockIndex = blk.getBlockIndices()[0];
    final Block targetBlk = new Block(blk.getBlock().getBlockId() + blockIndex,
        cellSize, blk.getBlock().getGenerationStamp());
    DatanodeInfo extraDn = getDatanodeOutOfTheBlock(blk);
    DatanodeDescriptor target = bm.getDatanodeManager()
        .getDatanode(extraDn.getDatanodeUuid());
    dn0.addBlockToBeReplicated(targetBlk,
        new DatanodeStorageInfo[] {target.getStorageInfos()[0]});

    // dn0 replicates in success
    GenericTestUtils.waitFor(
        () -> dn0.getNumberOfReplicateBlocks() == 0,
        100, 60000);
    GenericTestUtils.waitFor(
        () -> {
          Iterator<DatanodeStorageInfo> it =
              bm.getStoredBlock(targetBlk).getStorageInfos();
          while(it.hasNext()) {
            if (it.next().getDatanodeDescriptor().equals(target)) {
              return true;
            }
          }
          return false;
        },
        100, 60000);

    // There are 9 live replicas, 0 decommissioning replicas.
    BlockInfoStriped blockInfo =
        (BlockInfoStriped)bm.getStoredBlock(
            new Block(blk.getBlock().getBlockId()));
    Iterator<BlockInfoStriped.StorageAndBlockIndex> it =
        blockInfo.getStorageAndIndexInfos().iterator();
    DatanodeStorageInfo decommissioningStorage = null;
    DatanodeStorageInfo liveStorage = null;
    while(it.hasNext()) {
      BlockInfoStriped.StorageAndBlockIndex si = it.next();
      if(si.getStorage().getDatanodeDescriptor().equals(dn0)) {
        decommissioningStorage = si.getStorage();
      }
      if(si.getStorage().getDatanodeDescriptor().equals(target)) {
        liveStorage = si.getStorage();
      }
    }
    assertNotNull(decommissioningStorage);
    assertNotNull(liveStorage);

    // Adjust internal block locations
    // [b0(decommissioning), b1, b2, b3, b4, b5, b6, b7, b8, b0(live)] changed
    // to [b0(live), b1, b2, b3, b4, b5, b6, b7, b8, b0(decommissioning)]
    BlockManagerTestUtil.removeStorage(blockInfo, decommissioningStorage);
    BlockManagerTestUtil.addStorage(blockInfo, liveStorage, targetBlk);
    BlockManagerTestUtil.addStorage(blockInfo, decommissioningStorage,
        targetBlk);
    assertEquals(0, bm.countNodes(blockInfo).decommissioning());
    assertEquals(9, bm.countNodes(blockInfo).liveReplicas());
    cleanupFile(dfs, ecFile);
  }

  /**
   * Test recovery for an ec block, its storage array contains these internal
   * blocks which are {b0, b1, b2, b3, null, b5, b6, b7, b8, b0, b1, b2,
   * b3}, array[0]{b0} in decommissioning, array[1-3]{b1, b2, b3} are
   * in decommissioned. array[4] is null, array[5-12]{b[5-8],b[0-3]} are
   * in live.
   */
  @Test (timeout = 120000)
  public void testRecoveryWithDecommission() throws Exception {
    final Path ecFile = new Path(ecDir, "testRecoveryWithDecommission");
    int writeBytes = cellSize * dataBlocks;
    byte[] originBytesArray = writeStripedFile(dfs, ecFile, writeBytes);
    List<LocatedBlock> lbs = ((HdfsDataInputStream) dfs.open(ecFile))
        .getAllBlocks();
    LocatedStripedBlock blk = (LocatedStripedBlock) lbs.get(0);
    DatanodeInfo[] dnList = blk.getLocations();
    BlockInfoStriped blockInfo =
        (BlockInfoStriped)bm.getStoredBlock(
            new Block(blk.getBlock().getBlockId()));

    // Decommission datanode dn0 contains block b0
    // Aim to add storageinfo of replicated block b0 to storages[9] of ec block
    List<DatanodeInfo> decommissionedNodes = new ArrayList<>();
    decommissionedNodes.add(dnList[0]);
    decommissionNode(0, decommissionedNodes, AdminStates.DECOMMISSIONED);

    // Now storages of ec block are (b0{decommissioned}, b[1-8]{live},
    // b0{live})
    assertEquals(9, bm.countNodes(blockInfo).liveReplicas());
    assertEquals(1, bm.countNodes(blockInfo).decommissioned());

    int decommissionNodesNum = 4;

    // Decommission nodes contain blocks of b[0-3]
    // dn0 has been decommissioned
    for (int i = 1; i < decommissionNodesNum; i++) {
      decommissionedNodes.add(dnList[i]);
    }
    decommissionNode(0, decommissionedNodes, AdminStates.DECOMMISSIONED);

    // Now storages of ec block are (b[0-3]{decommissioned}, b[4-8]{live},
    // b0{live}, b[1-3]{live})
    // There are 9 live and 4 decommissioned internal blocks
    assertEquals(9, bm.countNodes(blockInfo).liveReplicas());
    assertEquals(4, bm.countNodes(blockInfo).decommissioned());

    // There are no reconstruction tasks
    assertEquals(0, bm.getDatanodeManager().getDatanodeAdminManager()
        .getNumPendingNodes());
    assertEquals(0, bm.getUnderReplicatedNotMissingBlocks());

    // Set dn0 in decommissioning
    // So that the block on dn0 can be used for reconstruction task
    DatanodeDescriptor dn0 = bm.getDatanodeManager()
        .getDatanode(dnList[0].getDatanodeUuid());
    dn0.startDecommission();

    // Stop the datanode contains b4
    DataNode dn = cluster.getDataNode(
        dnList[decommissionNodesNum].getIpcPort());
    cluster.stopDataNode(dnList[decommissionNodesNum].getXferAddr());
    cluster.setDataNodeDead(dn.getDatanodeId());

    // Now storages of ec block are (b[0]{decommissioning},
    // b[1-3]{decommissioned}, null, b[5-8]{live}, b0{live}, b[1-3]{live})
    // There are 8 live and 1 decommissioning internal blocks
    // Wait for reconstruction EC block.
    GenericTestUtils.waitFor(
        () -> bm.countNodes(blockInfo).liveReplicas() == 9,
        100, 10000);

    byte[] readBytesArray = new byte[writeBytes];
    StripedFileTestUtil.verifyPread(dfs, ecFile, writeBytes,
        originBytesArray, readBytesArray, ecPolicy);
    cleanupFile(dfs, ecFile);
  }
}
