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

import static org.apache.hadoop.hdfs.StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_DATA_BLOCKS;
import static org.apache.hadoop.hdfs.StripedFileTestUtil.NUM_PARITY_BLOCKS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.StripedFileTestUtil;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.security.token.Token;
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

  private Path decommissionDir;
  private Path hostsFile;
  private Path excludeFile;
  private FileSystem localFileSys;

  private Configuration conf;
  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private int numDNs;
  private final int blockSize = StripedFileTestUtil.blockSize;
  private final int cellSize = StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE;
  private int dataBlocks = StripedFileTestUtil.NUM_DATA_BLOCKS;
  private final Path ecDir = new Path("/" + this.getClass().getSimpleName());

  private FSNamesystem fsn;
  private BlockManager bm;
  private DFSClient client;

  @Before
  public void setup() throws IOException {
    conf = new HdfsConfiguration();

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
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);
    conf.set(DFSConfigKeys.DFS_HOSTS, hostsFile.toUri().getPath());
    conf.set(DFSConfigKeys.DFS_HOSTS_EXCLUDE, excludeFile.toUri().getPath());
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        2000);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        BLOCKREPORT_INTERVAL_MSEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_KEY,
        4);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY,
        NAMENODE_REPLICATION_INTERVAL);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, blockSize);
    conf.setInt(
        DFSConfigKeys.DFS_DN_EC_RECONSTRUCTION_STRIPED_READ_BUFFER_SIZE_KEY,
        StripedFileTestUtil.BLOCK_STRIPED_CELL_SIZE - 1);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_INTERVAL_KEY, 1);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REPLICATION_CONSIDERLOAD_KEY,
        false);

    numDNs = NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS + 2;
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(numDNs).build();
    cluster.waitActive();
    dfs = cluster.getFileSystem(0);
    fsn = cluster.getNamesystem();
    bm = fsn.getBlockManager();
    client = getDfsClient(cluster.getNameNode(0), conf);

    dfs.mkdirs(ecDir);
    dfs.setErasureCodingPolicy(ecDir, null);
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
    int writeBytes = BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS * 2;
    writeStripedFile(dfs, ecFile, writeBytes);
    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());

    final List<DatanodeInfo> decommisionNodes = new ArrayList<DatanodeInfo>();
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(ecFile.toString(), 0)
        .get(0);
    DatanodeInfo[] dnLocs = lb.getLocations();
    assertEquals(NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS, dnLocs.length);
    int decommNodeIndex = NUM_DATA_BLOCKS - 1;
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
    int deadDecomissioned = fsn.getNumDecomDeadDataNodes();
    int liveDecomissioned = fsn.getNumDecomLiveDataNodes();
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

    assertEquals(deadDecomissioned, fsn.getNumDecomDeadDataNodes());
    assertEquals(liveDecomissioned + decommisionNodes.size(),
        fsn.getNumDecomLiveDataNodes());

    // Ensure decommissioned datanode is not automatically shutdown
    DFSClient client = getDfsClient(cluster.getNameNode(0), conf);
    assertEquals("All datanodes must be alive", numDNs,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    assertNull(checkFile(dfs, ecFile, 9, decommisionNodes, numDNs));
    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes, decommisionNodes,
        null);
    cleanupFile(dfs, ecFile);
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
    int writeBytes = BLOCK_STRIPED_CELL_SIZE * NUM_DATA_BLOCKS;
    writeStripedFile(dfs, ecFile, writeBytes);
    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());
    FileChecksum fileChecksum1 = dfs.getFileChecksum(ecFile, writeBytes);

    final List<DatanodeInfo> decommisionNodes = new ArrayList<DatanodeInfo>();
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(ecFile.toString(), 0)
        .get(0);
    DatanodeInfo[] dnLocs = lb.getLocations();
    assertEquals(NUM_DATA_BLOCKS + NUM_PARITY_BLOCKS, dnLocs.length);
    int decommNodeIndex = 1;

    // add the node which will be decommissioning
    decommisionNodes.add(dnLocs[decommNodeIndex]);
    decommissionNode(0, decommisionNodes, AdminStates.DECOMMISSIONED);
    assertEquals(decommisionNodes.size(), fsn.getNumDecomLiveDataNodes());
    assertNull(checkFile(dfs, ecFile, 9, decommisionNodes, numDNs));
    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes, decommisionNodes,
        null);

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

    int deadDecomissioned = fsn.getNumDecomDeadDataNodes();
    int liveDecomissioned = fsn.getNumDecomLiveDataNodes();
    List<LocatedBlock> lbs = ((HdfsDataInputStream) dfs.open(ecFile))
        .getAllBlocks();

    // prepare expected block index and token list.
    List<HashMap<DatanodeInfo, Byte>> locToIndexList = new ArrayList<>();
    List<HashMap<DatanodeInfo, Token<BlockTokenIdentifier>>> locToTokenList =
        new ArrayList<>();
    prepareBlockIndexAndTokenList(lbs, locToIndexList, locToTokenList);

    // Decommission node. Verify that node is decommissioned.
    decommissionNode(0, decommisionNodes, AdminStates.DECOMMISSIONED);

    assertEquals(deadDecomissioned, fsn.getNumDecomDeadDataNodes());
    assertEquals(liveDecomissioned + decommisionNodes.size(),
        fsn.getNumDecomLiveDataNodes());

    // Ensure decommissioned datanode is not automatically shutdown
    DFSClient client = getDfsClient(cluster.getNameNode(0), conf);
    assertEquals("All datanodes must be alive", numDNs,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    assertNull(checkFile(dfs, ecFile, storageCount, decommisionNodes, numDNs));
    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes, decommisionNodes,
        null);

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

  private void writeStripedFile(DistributedFileSystem dfs, Path ecFile,
      int writeBytes) throws IOException, Exception {
    byte[] bytes = StripedFileTestUtil.generateBytes(writeBytes);
    DFSTestUtil.writeFile(dfs, ecFile, new String(bytes));
    StripedFileTestUtil.waitBlockGroupsReported(dfs, ecFile.toString());

    StripedFileTestUtil.checkData(dfs, ecFile, writeBytes,
        new ArrayList<DatanodeInfo>(), null);
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
   * @param downnode
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
}
