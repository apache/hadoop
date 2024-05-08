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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.util.Lists;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.junit.After;
import org.junit.Before;

/**
 * This class provide utilities for testing of the admin operations of nodes.
 */
public class AdminStatesBaseTest {
  public static final Logger LOG =
      LoggerFactory.getLogger(AdminStatesBaseTest.class);
  static final long seed = 0xDEADBEEFL;
  static final int blockSize = 8192;
  static final int fileSize = 16384;
  static final int HEARTBEAT_INTERVAL = 1; // heartbeat interval in seconds
  static final int BLOCKREPORT_INTERVAL_MSEC = 1000; //block report in msec
  static final int NAMENODE_REPLICATION_INTERVAL = 1; //replication interval

  final private Random myrand = new Random();

  @Rule
  public TemporaryFolder baseDir = new TemporaryFolder();

  private HostsFileWriter hostsFileWriter;
  private Configuration conf;
  private MiniDFSCluster cluster = null;
  private boolean useCombinedHostFileManager = false;

  protected void setUseCombinedHostFileManager() {
    useCombinedHostFileManager = true;
  }

  protected Configuration getConf() {
    return conf;
  }

  protected MiniDFSCluster getCluster() {
    return cluster;
  }

  @Before
  public void setup() throws IOException {
    // Set up the hosts/exclude files.
    hostsFileWriter = new HostsFileWriter();
    conf = new HdfsConfiguration();

    if (useCombinedHostFileManager) {
      conf.setClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
          CombinedHostFileManager.class, HostConfigManager.class);
    }

    // Setup conf
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        false);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        200);
    conf.setInt(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, HEARTBEAT_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY,
        BLOCKREPORT_INTERVAL_MSEC);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_INTERVAL_SECONDS_KEY,
        NAMENODE_REPLICATION_INTERVAL);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY, 1);

    hostsFileWriter.initialize(conf, "temp/admin");

  }

  @After
  public void teardown() throws IOException {
    hostsFileWriter.cleanup();
    shutdownCluster();
  }

  static public FSDataOutputStream writeIncompleteFile(FileSystem fileSys,
      Path name, short repl, short numOfBlocks) throws IOException {
    return writeFile(fileSys, name, repl, numOfBlocks, false);
  }

  static protected void writeFile(FileSystem fileSys, Path name, int repl)
      throws IOException {
    writeFile(fileSys, name, repl, 2);
  }

  static protected void writeFile(FileSystem fileSys, Path name, int repl,
      int numOfBlocks) throws IOException {
    writeFile(fileSys, name, repl, numOfBlocks, true);
  }

  static protected FSDataOutputStream writeFile(FileSystem fileSys, Path name,
      int repl, int numOfBlocks, boolean completeFile)
    throws IOException {
    // create and write a file that contains two blocks of data
    FSDataOutputStream stm = fileSys.create(name, true, fileSys.getConf()
        .getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
        (short) repl, blockSize);
    byte[] buffer = new byte[blockSize*numOfBlocks];
    Random rand = new Random(seed);
    rand.nextBytes(buffer);
    stm.write(buffer);
    LOG.info("Created file " + name + " with " + repl + " replicas.");
    if (completeFile) {
      stm.close();
      return null;
    } else {
      stm.flush();
      // Do not close stream, return it
      // so that it is not garbage collected
      return stm;
    }
  }

  /**
   * Decommission or perform Maintenance for DataNodes and wait for them to
   * reach the expected state.
   *
   * @param nnIndex NameNode index
   * @param datanodeUuid DataNode to decommission/maintenance, or a random
   *                     DataNode if null
   * @param maintenanceExpirationInMS Maintenance expiration time
   * @param decommissionedNodes List of DataNodes already decommissioned
   * @param waitForState Await for this state for datanodeUuid DataNode
   * @return DatanodeInfo DataNode taken out of service
   * @throws IOException
   */
  protected DatanodeInfo takeNodeOutofService(int nnIndex,
      String datanodeUuid, long maintenanceExpirationInMS,
      ArrayList<DatanodeInfo> decommissionedNodes,
      AdminStates waitForState) throws IOException {
    return takeNodeOutofService(nnIndex, datanodeUuid,
        maintenanceExpirationInMS, decommissionedNodes, null, waitForState);
  }

  /**
   * Decommission or perform Maintenance for DataNodes and wait for them to
   * reach the expected state.
   *
   * @param nnIndex NameNode index
   * @param datanodeUuid DataNode to decommission/maintenance, or a random
   *                     DataNode if null
   * @param maintenanceExpirationInMS Maintenance expiration time
   * @param decommissionedNodes List of DataNodes already decommissioned
   * @param inMaintenanceNodes Map of DataNodes already entering/in maintenance
   * @param waitForState Await for this state for datanodeUuid DataNode
   * @return DatanodeInfo DataNode taken out of service
   * @throws IOException
   */
  protected DatanodeInfo takeNodeOutofService(int nnIndex,
      String datanodeUuid, long maintenanceExpirationInMS,
      List<DatanodeInfo> decommissionedNodes,
      Map<DatanodeInfo, Long> inMaintenanceNodes, AdminStates waitForState)
      throws IOException {
    return takeNodeOutofService(nnIndex, (datanodeUuid != null ?
            Lists.newArrayList(datanodeUuid) : null),
        maintenanceExpirationInMS, decommissionedNodes, inMaintenanceNodes,
        waitForState).get(0);
  }

  /**
   * Decommission or perform Maintenance for DataNodes and wait for them to
   * reach the expected state.
   *
   * @param nnIndex NameNode index
   * @param dataNodeUuids DataNodes to decommission/maintenance, or a random
   *                     DataNode if null
   * @param maintenanceExpirationInMS Maintenance expiration time
   * @param decommissionedNodes List of DataNodes already decommissioned
   * @param inMaintenanceNodes Map of DataNodes already entering/in maintenance
   * @param waitForState Await for this state for datanodeUuid DataNode
   * @return DatanodeInfo DataNode taken out of service
   * @throws IOException
   */
  protected List<DatanodeInfo> takeNodeOutofService(int nnIndex,
      List<String> dataNodeUuids, long maintenanceExpirationInMS,
      List<DatanodeInfo> decommissionedNodes,
      Map<DatanodeInfo, Long> inMaintenanceNodes, AdminStates waitForState)
      throws IOException {
    DFSClient client = getDfsClient(nnIndex);
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.ALL);
    boolean isDecommissionRequest =
        waitForState == AdminStates.DECOMMISSION_INPROGRESS ||
            waitForState == AdminStates.DECOMMISSIONED;

    List<String> dataNodeNames = new ArrayList<>();
    List<DatanodeInfo> datanodeInfos = new ArrayList<>();
    // pick one DataNode randomly unless the caller specifies one.
    if (dataNodeUuids == null) {
      boolean found = false;
      while (!found) {
        int index = myrand.nextInt(info.length);
        if ((isDecommissionRequest && !info[index].isDecommissioned()) ||
            (!isDecommissionRequest && !info[index].isInMaintenance())) {
          dataNodeNames.add(info[index].getXferAddr());
          datanodeInfos.add(NameNodeAdapter.getDatanode(
              cluster.getNamesystem(nnIndex), info[index]));
          found = true;
        }
      }
    } else {
      // The caller specified a DataNode
      for (String datanodeUuid : dataNodeUuids) {
        boolean found = false;
        for (int index = 0; index < info.length; index++) {
          if (info[index].getDatanodeUuid().equals(datanodeUuid)) {
            dataNodeNames.add(info[index].getXferAddr());
            datanodeInfos.add(NameNodeAdapter.getDatanode(
                cluster.getNamesystem(nnIndex), info[index]));
            found = true;
            break;
          }
        }
        if (!found) {
          throw new IOException("invalid datanodeUuid " + datanodeUuid);
        }
      }
    }
    LOG.info("Taking node: " + Arrays.toString(dataNodeNames.toArray())
        + " out of service");

    ArrayList<String> decommissionNodes = new ArrayList<String>();
    if (decommissionedNodes != null) {
      for (DatanodeInfo dn : decommissionedNodes) {
        decommissionNodes.add(dn.getName());
      }
    }
    Map<String, Long> maintenanceNodes = new HashMap<>();
    if (inMaintenanceNodes != null) {
      for (Map.Entry<DatanodeInfo, Long> dn :
          inMaintenanceNodes.entrySet()) {
        maintenanceNodes.put(dn.getKey().getName(), dn.getValue());
      }
    }

    if (isDecommissionRequest) {
      for (String dataNodeName : dataNodeNames) {
        decommissionNodes.add(dataNodeName);
      }
    } else {
      for (String dataNodeName : dataNodeNames) {
        maintenanceNodes.put(dataNodeName, maintenanceExpirationInMS);
      }
    }

    // write node names into the json host file.
    hostsFileWriter.initOutOfServiceHosts(decommissionNodes, maintenanceNodes);
    refreshNodes(nnIndex);
    waitNodeState(datanodeInfos, waitForState);
    return datanodeInfos;
  }

  /* Ask a specific NN to put the datanode in service and wait for it
   * to reach the NORMAL state.
   */
  protected void putNodeInService(int nnIndex,
      DatanodeInfo outOfServiceNode) throws IOException {
    LOG.info("Putting node: " + outOfServiceNode + " in service");
    ArrayList<String> decommissionNodes = new ArrayList<>();
    Map<String, Long> maintenanceNodes = new HashMap<>();

    DatanodeManager dm =
        cluster.getNamesystem(nnIndex).getBlockManager().getDatanodeManager();
    List<DatanodeDescriptor> nodes =
        dm.getDatanodeListForReport(DatanodeReportType.ALL);
    for (DatanodeDescriptor node : nodes) {
      if (node.isMaintenance()) {
        maintenanceNodes.put(node.getName(),
            node.getMaintenanceExpireTimeInMS());
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        decommissionNodes.add(node.getName());
      }
    }
    decommissionNodes.remove(outOfServiceNode.getName());
    maintenanceNodes.remove(outOfServiceNode.getName());

    hostsFileWriter.initOutOfServiceHosts(decommissionNodes, maintenanceNodes);
    refreshNodes(nnIndex);
    waitNodeState(outOfServiceNode, AdminStates.NORMAL);
  }

  protected void putNodeInService(int nnIndex,
      String datanodeUuid) throws IOException {
    DatanodeInfo datanodeInfo =
        getDatanodeDesriptor(cluster.getNamesystem(nnIndex), datanodeUuid);
    putNodeInService(nnIndex, datanodeInfo);
  }

  /**
   * Wait till DataNode is transitioned to the expected state.
   */
  protected void waitNodeState(DatanodeInfo node, AdminStates state) {
    waitNodeState(Lists.newArrayList(node), state);
  }

  /**
   * Wait till all DataNodes are transitioned to the expected state.
   */
  protected void waitNodeState(List<DatanodeInfo> nodes, AdminStates state) {
    for (DatanodeInfo node : nodes) {
      boolean done = (state == node.getAdminState());
      while (!done) {
        LOG.info("Waiting for node " + node + " to change state to "
            + state + " current state: " + node.getAdminState());
        try {
          Thread.sleep(HEARTBEAT_INTERVAL * 500);
        } catch (InterruptedException e) {
          // nothing
        }
        done = (state == node.getAdminState());
      }
      LOG.info("node " + node + " reached the state " + state);
    }
  }

  protected void initIncludeHost(String hostNameAndPort) throws IOException {
    hostsFileWriter.initIncludeHost(hostNameAndPort);
  }

  protected void initIncludeHosts(String[] hostNameAndPorts)
      throws IOException {
    hostsFileWriter.initIncludeHosts(hostNameAndPorts);
  }

  protected void initExcludeHost(String hostNameAndPort) throws IOException {
    hostsFileWriter.initExcludeHost(hostNameAndPort);
  }

  protected void initExcludeHosts(List<String> hostNameAndPorts)
      throws IOException {
    hostsFileWriter.initExcludeHosts(hostNameAndPorts);
  }

  /* Get DFSClient to the namenode */
  protected DFSClient getDfsClient(final int nnIndex) throws IOException {
    return new DFSClient(cluster.getNameNode(nnIndex).getNameNodeAddress(),
        conf);
  }

  /* Validate cluster has expected number of datanodes */
  protected static void validateCluster(DFSClient client, int numDNs)
      throws IOException {
    DatanodeInfo[] info = client.datanodeReport(DatanodeReportType.LIVE);
    assertEquals("Number of Datanodes ", numDNs, info.length);
  }

  /** Start a MiniDFSCluster.
   * @throws IOException */
  protected void startCluster(int numNameNodes, int numDatanodes,
      boolean setupHostsFile, long[] nodesCapacity,
      boolean checkDataNodeHostConfig) throws IOException {
    startCluster(numNameNodes, numDatanodes, setupHostsFile, nodesCapacity,
        checkDataNodeHostConfig, true);
  }

  protected void startCluster(int numNameNodes, int numDatanodes,
      boolean setupHostsFile, long[] nodesCapacity,
      boolean checkDataNodeHostConfig, boolean federation) throws IOException {
    MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf, baseDir.getRoot())
        .numDataNodes(numDatanodes);
    if (federation) {
      builder.nnTopology(
          MiniDFSNNTopology.simpleFederatedTopology(numNameNodes));
    }
    if (setupHostsFile) {
      builder.setupHostsFile(setupHostsFile);
    }
    if (nodesCapacity != null) {
      builder.simulatedCapacities(nodesCapacity);
    }
    if (checkDataNodeHostConfig) {
      builder.checkDataNodeHostConfig(checkDataNodeHostConfig);
    }
    cluster = builder.build();
    cluster.waitActive();
    for (int i = 0; i < numNameNodes; i++) {
      DFSClient client = getDfsClient(i);
      validateCluster(client, numDatanodes);
    }
  }

  protected void startCluster(int numNameNodes, int numDatanodes)
      throws IOException {
    startCluster(numNameNodes, numDatanodes, false, null, false);
  }

  protected void startSimpleCluster(int numNameNodes, int numDatanodes)
      throws IOException {
    startCluster(numNameNodes, numDatanodes, false, null, false, false);
  }


  protected void startSimpleHACluster(int numDatanodes) throws IOException {
    cluster = new MiniDFSCluster.Builder(conf, baseDir.getRoot())
        .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(
        numDatanodes).build();
    cluster.transitionToActive(0);
    cluster.waitActive();
  }

  protected void shutdownCluster() {
    if (cluster != null) {
      cluster.shutdown(true);
    }
  }

  protected void refreshNodes(final int nnIndex) throws IOException {
    cluster.getNamesystem(nnIndex).getBlockManager().getDatanodeManager().
        refreshNodes(conf);
  }

  static DatanodeDescriptor getDatanodeDesriptor(
      final FSNamesystem ns, final String datanodeUuid) {
    return ns.getBlockManager().getDatanodeManager().getDatanode(datanodeUuid);
  }

  static public void cleanupFile(FileSystem fileSys, Path name)
      throws IOException {
    assertTrue(fileSys.exists(name));
    fileSys.delete(name, true);
    assertFalse(fileSys.exists(name));
  }
}
