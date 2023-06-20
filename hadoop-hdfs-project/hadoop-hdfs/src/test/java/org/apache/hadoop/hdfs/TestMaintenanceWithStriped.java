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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the in maintenance of datanode with striped blocks.
 */
public class TestMaintenanceWithStriped {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMaintenanceWithStriped.class);

  // heartbeat interval in seconds
  private static final int HEARTBEAT_INTERVAL = 1;
  // block report in msec
  private static final int BLOCKREPORT_INTERVAL_MSEC = 1000;
  // replication interval
  private static final int NAMENODE_REPLICATION_INTERVAL = 1;

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
  private HostsFileWriter hostsFileWriter;
  private boolean useCombinedHostFileManager = true;

  private FSNamesystem fsn;
  private BlockManager bm;

  protected Configuration createConfiguration() {
    return new HdfsConfiguration();
  }

  @Before
  public void setup() throws IOException {
    // Set up the hosts/exclude files.
    hostsFileWriter = new HostsFileWriter();
    conf = createConfiguration();
    if (useCombinedHostFileManager) {
      conf.setClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
          CombinedHostFileManager.class, HostConfigManager.class);
    }
    hostsFileWriter.initialize(conf, "temp/admin");


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

    dfs.enableErasureCodingPolicy(
        StripedFileTestUtil.getDefaultECPolicy().getName());
    dfs.mkdirs(ecDir);
    dfs.setErasureCodingPolicy(ecDir,
        StripedFileTestUtil.getDefaultECPolicy().getName());
  }

  @After
  public void teardown() throws IOException {
    hostsFileWriter.cleanup();
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  /**
   * test DN maintenance with striped blocks.
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testInMaintenance() throws Exception {
    //1. create EC file
    // d0 d1 d2 d3 d4 d5 d6 d7 d8
    final Path ecFile = new Path(ecDir, "testInMaintenance");
    int writeBytes = cellSize * dataBlocks;
    writeStripedFile(dfs, ecFile, writeBytes);
    Assert.assertEquals(0, bm.numOfUnderReplicatedBlocks());
    FileChecksum fileChecksum1 = dfs.getFileChecksum(ecFile, writeBytes);

    final INodeFile fileNode = cluster.getNamesystem().getFSDirectory()
        .getINode4Write(ecFile.toString()).asFile();
    BlockInfo firstBlock = fileNode.getBlocks()[0];
    DatanodeStorageInfo[] dnStorageInfos = bm.getStorages(firstBlock);

    //2. maintenance node
    // d4 d5 d6 d7 d8
    int maintenanceDNIndex = 4;
    int numMaintenance= 5;
    List<DatanodeInfo> maintenanceNodes = new ArrayList<>();

    for (int i = maintenanceDNIndex; i < numMaintenance + maintenanceDNIndex; ++i) {
      maintenanceNodes.add(dnStorageInfos[i].getDatanodeDescriptor());
    }

    maintenanceNode(0, maintenanceNodes, AdminStates.IN_MAINTENANCE, Long.MAX_VALUE);

    //3. wait for maintenance block to replicate
    GenericTestUtils.waitFor(
        () -> maintenanceNodes.size() == fsn.getNumInMaintenanceLiveDataNodes(),
            100, 60000);

    //4. check DN status, it should be reconstructed again
    LocatedBlocks lbs = cluster.getNameNodeRpc().getBlockLocations(
        ecFile.toString(), 0, writeBytes);
    LocatedStripedBlock bg = (LocatedStripedBlock) (lbs.get(0));

    BlockInfoStriped blockInfo =
        (BlockInfoStriped)bm.getStoredBlock(
            new Block(bg.getBlock().getBlockId()));

    // So far, there are 11 total internal blocks, 6 live (d0 d1 d2 d3 d4' d5')
    // and 5 in maintenance (d4 d5 d6 d7 d8) internal blocks.

    assertEquals(6, bm.countNodes(blockInfo).liveReplicas());
    assertEquals(5, bm.countNodes(blockInfo).maintenanceNotForReadReplicas());

    FileChecksum fileChecksum2 = dfs.getFileChecksum(ecFile, writeBytes);
    Assert.assertEquals("Checksum mismatches!", fileChecksum1, fileChecksum2);
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

  /*
   * maintenance the DN at index dnIndex or one random node if dnIndex is set
   * to -1 and wait for the node to reach the given {@code waitForState}.
   */
  private void maintenanceNode(int nnIndex, List<DatanodeInfo> maintenancedNodes,
      AdminStates waitForState, long maintenanceExpirationInMS)
          throws IOException, TimeoutException, InterruptedException {
    DFSClient client = getDfsClient(cluster.getNameNode(nnIndex), conf);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);

    // write nodename into the exclude file.
    Map<String, Long> maintenanceNodes = new HashMap<>();

    for (DatanodeInfo dn : maintenancedNodes) {
      boolean nodeExists = false;
      for (DatanodeInfo dninfo : info) {
        if (dninfo.getDatanodeUuid().equals(dn.getDatanodeUuid())) {
          nodeExists = true;
          break;
        }
      }
      assertTrue("Datanode: " + dn + " is not LIVE", nodeExists);
      maintenanceNodes.put(dn.getName(), maintenanceExpirationInMS);
      LOG.info("Maintenance node: " + dn.getName());
    }
    // write node names into the json host file.
    hostsFileWriter.initOutOfServiceHosts(null, maintenanceNodes);

    refreshNodes(cluster.getNamesystem(nnIndex), conf);
    for (DatanodeInfo dn : maintenancedNodes) {
      DatanodeInfo ret = NameNodeAdapter
          .getDatanode(cluster.getNamesystem(nnIndex), dn);
      LOG.info("Waiting for node " + ret + " to change state to " + waitForState
          + " current state: " + ret.getAdminState());
      GenericTestUtils.waitFor(
          () -> ret.getAdminState() == waitForState,
              100, 60000);
      LOG.info("node " + ret + " reached the state " + waitForState);
    }
  }

  private static void refreshNodes(final FSNamesystem ns,
      final Configuration conf) throws IOException {
    ns.getBlockManager().getDatanodeManager().refreshNodes(conf);
  }

}
