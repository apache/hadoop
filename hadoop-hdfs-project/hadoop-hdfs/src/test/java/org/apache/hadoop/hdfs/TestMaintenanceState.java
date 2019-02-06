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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

/**
 * This class tests node maintenance.
 */
public class TestMaintenanceState extends AdminStatesBaseTest {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestMaintenanceState.class);
  static private final long EXPIRATION_IN_MS = 50;
  private int minMaintenanceR =
      DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_DEFAULT;

  public TestMaintenanceState() {
    setUseCombinedHostFileManager();
  }

  void setMinMaintenanceR(int minMaintenanceR) {
    this.minMaintenanceR = minMaintenanceR;
    getConf().setInt(
        DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY,
        minMaintenanceR);
  }

  /**
   * Test valid value range for the config namenode.maintenance.replication.min.
   */
  @Test (timeout = 60000)
  public void testMaintenanceMinReplConfigRange() {
    LOG.info("Setting testMaintenanceMinReplConfigRange");

    // Case 1: Maintenance min replication less allowed minimum 0
    setMinMaintenanceR(-1);
    try {
      startCluster(1, 1);
      fail("Cluster start should fail when 'dfs.namenode.maintenance" +
          ".replication.min=-1'");
    } catch (IOException e) {
      LOG.info("Expected exception: " + e);
    }

    // Case 2: Maintenance min replication greater
    // allowed max of DFSConfigKeys.DFS_REPLICATION_KEY
    int defaultRepl = getConf().getInt(
        DFSConfigKeys.DFS_REPLICATION_KEY,
        DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    setMinMaintenanceR(defaultRepl + 1);
    try {
      startCluster(1, 1);
      fail("Cluster start should fail when 'dfs.namenode.maintenance" +
          ".replication.min > " + defaultRepl + "'");
    } catch (IOException e) {
      LOG.info("Expected exception: " + e);
    }
  }

  /**
   * Verify a node can transition from AdminStates.ENTERING_MAINTENANCE to
   * AdminStates.NORMAL.
   */
  @Test(timeout = 360000)
  public void testTakeNodeOutOfEnteringMaintenance() throws Exception {
    LOG.info("Starting testTakeNodeOutOfEnteringMaintenance");
    final int replicas = 1;
    final Path file = new Path("/testTakeNodeOutOfEnteringMaintenance.dat");

    startCluster(1, 1);

    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file, replicas, 1);

    final DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        null, Long.MAX_VALUE, null, AdminStates.ENTERING_MAINTENANCE);

    // When node is in ENTERING_MAINTENANCE state, it can still serve read
    // requests
    checkWithRetry(ns, fileSys, file, replicas, null,
        nodeOutofService);

    putNodeInService(0, nodeOutofService.getDatanodeUuid());

    cleanupFile(fileSys, file);
  }

  /**
   * Verify a AdminStates.ENTERING_MAINTENANCE node can expire and transition
   * to AdminStates.NORMAL upon timeout.
   */
  @Test(timeout = 360000)
  public void testEnteringMaintenanceExpiration() throws Exception {
    LOG.info("Starting testEnteringMaintenanceExpiration");
    final int replicas = 1;
    final Path file = new Path("/testEnteringMaintenanceExpiration.dat");

    startCluster(1, 1);

    final FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file, replicas, 1);

    final DatanodeInfo nodeOutofService = takeNodeOutofService(0, null,
        Long.MAX_VALUE, null, AdminStates.ENTERING_MAINTENANCE);

    // Adjust the expiration.
    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(),
        Time.now() + EXPIRATION_IN_MS, null, AdminStates.NORMAL);

    cleanupFile(fileSys, file);
  }

  /**
   * Verify node stays in AdminStates.NORMAL with invalid expiration.
   */
  @Test(timeout = 360000)
  public void testInvalidExpiration() throws Exception {
    LOG.info("Starting testInvalidExpiration");
    final int replicas = 1;
    final Path file = new Path("/testInvalidExpiration.dat");

    startCluster(1, 1);

    final FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file, replicas, 1);

    // expiration has to be greater than Time.now().
    takeNodeOutofService(0, null, Time.now(), null,
        AdminStates.NORMAL);

    cleanupFile(fileSys, file);
  }

  /**
   * When a dead node is put to maintenance, it transitions directly to
   * AdminStates.IN_MAINTENANCE.
   */
  @Test(timeout = 360000)
  public void testPutDeadNodeToMaintenance() throws Exception {
    LOG.info("Starting testPutDeadNodeToMaintenance");
    final int replicas = 1;
    final Path file = new Path("/testPutDeadNodeToMaintenance.dat");

    startCluster(1, 1);

    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file, replicas, 1);

    final MiniDFSCluster.DataNodeProperties dnProp =
        getCluster().stopDataNode(0);
    DFSTestUtil.waitForDatanodeState(
        getCluster(), dnProp.datanode.getDatanodeUuid(), false, 20000);

    int deadInMaintenance = ns.getNumInMaintenanceDeadDataNodes();
    int liveInMaintenance = ns.getNumInMaintenanceLiveDataNodes();

    takeNodeOutofService(0, dnProp.datanode.getDatanodeUuid(), Long.MAX_VALUE,
        null, AdminStates.IN_MAINTENANCE);

    assertEquals(deadInMaintenance + 1, ns.getNumInMaintenanceDeadDataNodes());
    assertEquals(liveInMaintenance, ns.getNumInMaintenanceLiveDataNodes());

    cleanupFile(fileSys, file);
  }

  /**
   * When a dead node is put to maintenance, it transitions directly to
   * AdminStates.IN_MAINTENANCE. Then AdminStates.IN_MAINTENANCE expires and
   * transitions to AdminStates.NORMAL.
   */
  @Test(timeout = 360000)
  public void testPutDeadNodeToMaintenanceWithExpiration() throws Exception {
    LOG.info("Starting testPutDeadNodeToMaintenanceWithExpiration");
    final Path file =
        new Path("/testPutDeadNodeToMaintenanceWithExpiration.dat");

    startCluster(1, 1);

    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file, 1, 1);

    MiniDFSCluster.DataNodeProperties dnProp = getCluster().stopDataNode(0);
    DFSTestUtil.waitForDatanodeState(
        getCluster(), dnProp.datanode.getDatanodeUuid(), false, 20000);

    int deadInMaintenance = ns.getNumInMaintenanceDeadDataNodes();
    int liveInMaintenance = ns.getNumInMaintenanceLiveDataNodes();

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        dnProp.datanode.getDatanodeUuid(),
        Long.MAX_VALUE, null, AdminStates.IN_MAINTENANCE);

    // Adjust the expiration.
    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(),
        Time.now() + EXPIRATION_IN_MS, null, AdminStates.NORMAL);

    // no change
    assertEquals(deadInMaintenance, ns.getNumInMaintenanceDeadDataNodes());
    assertEquals(liveInMaintenance, ns.getNumInMaintenanceLiveDataNodes());

    cleanupFile(fileSys, file);
  }

  /**
   * Transition from decommissioned state to maintenance state.
   */
  @Test(timeout = 360000)
  public void testTransitionFromDecommissioned() throws IOException {
    LOG.info("Starting testTransitionFromDecommissioned");
    final Path file = new Path("/testTransitionFromDecommissioned.dat");

    startCluster(1, 4);

    final FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file, 3, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0, null, 0, null,
        AdminStates.DECOMMISSIONED);

    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(), Long.MAX_VALUE,
        null, AdminStates.IN_MAINTENANCE);

    cleanupFile(fileSys, file);
  }

  /**
   * Transition from decommissioned state to maintenance state.
   * After the maintenance state expires, it is transitioned to NORMAL.
   */
  @Test(timeout = 360000)
  public void testTransitionFromDecommissionedAndExpired() throws IOException {
    LOG.info("Starting testTransitionFromDecommissionedAndExpired");
    final Path file =
        new Path("/testTransitionFromDecommissionedAndExpired.dat");

    startCluster(1, 4);

    final FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file, 3, 1);

    final DatanodeInfo nodeOutofService = takeNodeOutofService(0, null, 0,
        null, AdminStates.DECOMMISSIONED);

    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(),
        Long.MAX_VALUE, null, AdminStates.IN_MAINTENANCE);

    // Adjust the expiration.
    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(),
        Time.now() + EXPIRATION_IN_MS, null, AdminStates.NORMAL);

    cleanupFile(fileSys, file);
  }

  /**
   * When a node is put to maintenance, it first transitions to
   * AdminStates.ENTERING_MAINTENANCE. It makes sure all blocks have minimal
   * replication before it can be transitioned to AdminStates.IN_MAINTENANCE.
   * If node becomes dead when it is in AdminStates.ENTERING_MAINTENANCE, it
   * should stay in AdminStates.ENTERING_MAINTENANCE state.
   */
  @Test(timeout = 360000)
  public void testNodeDeadWhenInEnteringMaintenance() throws Exception {
    LOG.info("Starting testNodeDeadWhenInEnteringMaintenance");
    final int numNamenodes = 1;
    final int numDatanodes = 1;
    final int replicas = 1;
    final Path file = new Path("/testNodeDeadWhenInEnteringMaintenance.dat");

    startCluster(numNamenodes, numDatanodes);

    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file, replicas, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        getFirstBlockFirstReplicaUuid(fileSys, file), Long.MAX_VALUE, null,
        AdminStates.ENTERING_MAINTENANCE);
    assertEquals(1, ns.getNumEnteringMaintenanceDataNodes());

    MiniDFSCluster.DataNodeProperties dnProp =
        getCluster().stopDataNode(nodeOutofService.getXferAddr());
    DFSTestUtil.waitForDatanodeState(
        getCluster(), nodeOutofService.getDatanodeUuid(), false, 20000);
    DFSClient client = getDfsClient(0);
    assertEquals("maintenance node shouldn't be live", numDatanodes - 1,
        client.datanodeReport(DatanodeReportType.LIVE).length);
    assertEquals(1, ns.getNumEnteringMaintenanceDataNodes());

    getCluster().restartDataNode(dnProp, true);
    getCluster().waitActive();
    waitNodeState(nodeOutofService, AdminStates.ENTERING_MAINTENANCE);
    assertEquals(1, ns.getNumEnteringMaintenanceDataNodes());
    assertEquals("maintenance node should be live", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    cleanupFile(fileSys, file);
  }

  /**
   * When a node is put to maintenance, it first transitions to
   * AdminStates.ENTERING_MAINTENANCE. It makes sure all blocks have
   * been properly replicated before it can be transitioned to
   * AdminStates.IN_MAINTENANCE. The expected replication count takes
   * DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY and
   * its file's replication factor into account.
   */
  @Test(timeout = 360000)
  public void testExpectedReplications() throws IOException {
    LOG.info("Starting testExpectedReplications");
    testExpectedReplication(1);
    testExpectedReplication(2);
    testExpectedReplication(3);
    testExpectedReplication(4);
  }

  private void testExpectedReplication(int replicationFactor)
      throws IOException {
    testExpectedReplication(replicationFactor,
        Math.max(replicationFactor - 1, this.minMaintenanceR));
  }

  private void testExpectedReplication(int replicationFactor,
      int expectedReplicasInRead) throws IOException {
    setup();
    startCluster(1, 5);

    final Path file = new Path("/testExpectedReplication.dat");

    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);

    writeFile(fileSys, file, replicationFactor, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        getFirstBlockFirstReplicaUuid(fileSys, file), Long.MAX_VALUE,
        null, AdminStates.IN_MAINTENANCE);

    // The block should be replicated to another datanode to meet
    // expected replication count.
    checkWithRetry(ns, fileSys, file, expectedReplicasInRead,
        nodeOutofService);

    cleanupFile(fileSys, file);
    teardown();
  }

  /**
   * Verify a node can transition directly to AdminStates.IN_MAINTENANCE when
   * DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY is set to zero.
   */
  @Test(timeout = 360000)
  public void testZeroMinMaintenanceReplication() throws Exception {
    LOG.info("Starting testZeroMinMaintenanceReplication");
    setMinMaintenanceR(0);
    startCluster(1, 1);

    final Path file = new Path("/testZeroMinMaintenanceReplication.dat");
    final int replicas = 1;

    FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file, replicas, 1);

    takeNodeOutofService(0, null, Long.MAX_VALUE, null,
        AdminStates.IN_MAINTENANCE);

    cleanupFile(fileSys, file);
  }

  /**
   * Verify a node can transition directly to AdminStates.IN_MAINTENANCE when
   * DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY is set to zero. Then later
   * transition to NORMAL after maintenance expiration.
   */
  @Test(timeout = 360000)
  public void testZeroMinMaintenanceReplicationWithExpiration()
      throws Exception {
    LOG.info("Starting testZeroMinMaintenanceReplicationWithExpiration");
    setMinMaintenanceR(0);
    startCluster(1, 1);

    final Path file =
        new Path("/testZeroMinMaintenanceReplicationWithExpiration.dat");

    FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file, 1, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0, null,
        Long.MAX_VALUE, null, AdminStates.IN_MAINTENANCE);

    // Adjust the expiration.
    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(),
        Time.now() + EXPIRATION_IN_MS, null, AdminStates.NORMAL);

    cleanupFile(fileSys, file);
  }

  /**
   * Test file block replication lesser than maintenance minimum.
   */
  @Test(timeout = 360000)
  public void testFileBlockReplicationAffectingMaintenance()
      throws Exception {
    int defaultReplication = getConf().getInt(DFSConfigKeys
        .DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT);
    int defaultMaintenanceMinRepl = getConf().getInt(DFSConfigKeys
        .DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAINTENANCE_REPLICATION_MIN_DEFAULT);

    // Case 1:
    //  * Maintenance min larger than default min replication
    //  * File block replication larger than maintenance min
    //  * Initial data nodes not sufficient to remove all maintenance nodes
    //    as file block replication is greater than maintenance min.
    //  * Data nodes added later for the state transition to progress
    int maintenanceMinRepl = defaultMaintenanceMinRepl + 1;
    int fileBlockReplication = maintenanceMinRepl + 1;
    int numAddedDataNodes = 1;
    int numInitialDataNodes = (maintenanceMinRepl * 2 - numAddedDataNodes);
    Assert.assertTrue(maintenanceMinRepl <= defaultReplication);
    testFileBlockReplicationImpl(maintenanceMinRepl,
        numInitialDataNodes, numAddedDataNodes, fileBlockReplication);

    // Case 2:
    //  * Maintenance min larger than default min replication
    //  * File block replication lesser than maintenance min
    //  * Initial data nodes after removal of maintenance nodes is still
    //    sufficient for the file block replication.
    //  * No new data nodes to be added, still the state transition happens
    maintenanceMinRepl = defaultMaintenanceMinRepl + 1;
    fileBlockReplication = maintenanceMinRepl - 1;
    numAddedDataNodes = 0;
    numInitialDataNodes = (maintenanceMinRepl * 2 - numAddedDataNodes);
    testFileBlockReplicationImpl(maintenanceMinRepl,
        numInitialDataNodes, numAddedDataNodes, fileBlockReplication);
  }

  private void testFileBlockReplicationImpl(
      int maintenanceMinRepl, int numDataNodes, int numNewDataNodes,
      int fileBlockRepl)
      throws Exception {
    setup();
    LOG.info("Starting testLargerMinMaintenanceReplication - maintMinRepl: "
        + maintenanceMinRepl + ", numDNs: " + numDataNodes + ", numNewDNs: "
        + numNewDataNodes + ", fileRepl: " + fileBlockRepl);
    LOG.info("Setting maintenance minimum replication: " + maintenanceMinRepl);
    setMinMaintenanceR(maintenanceMinRepl);
    startCluster(1, numDataNodes);

    final Path file = new Path("/testLargerMinMaintenanceReplication.dat");

    FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file, fileBlockRepl, 1);
    final DatanodeInfo[] nodes = getFirstBlockReplicasDatanodeInfos(fileSys,
        file);

    ArrayList<String> nodeUuids = new ArrayList<>();
    for (int i = 0; i < maintenanceMinRepl && i < nodes.length; i++) {
      nodeUuids.add(nodes[i].getDatanodeUuid());
    }

    List<DatanodeInfo> maintenanceDNs = takeNodeOutofService(0, nodeUuids,
        Long.MAX_VALUE, null, null, AdminStates.ENTERING_MAINTENANCE);

    for (int i = 0; i < numNewDataNodes; i++) {
      getCluster().startDataNodes(getConf(), 1, true, null, null);
    }
    getCluster().waitActive();
    refreshNodes(0);
    waitNodeState(maintenanceDNs, AdminStates.IN_MAINTENANCE);
    cleanupFile(fileSys, file);
    teardown();
  }

  /**
   * Transition from IN_MAINTENANCE to DECOMMISSIONED.
   */
  @Test(timeout = 360000)
  public void testTransitionToDecommission() throws IOException {
    LOG.info("Starting testTransitionToDecommission");
    final int numNamenodes = 1;
    final int numDatanodes = 4;
    startCluster(numNamenodes, numDatanodes);

    final Path file = new Path("testTransitionToDecommission.dat");
    final int replicas = 3;

    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);

    writeFile(fileSys, file, replicas, 25);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        getFirstBlockFirstReplicaUuid(fileSys, file), Long.MAX_VALUE, null,
        AdminStates.IN_MAINTENANCE);

    DFSClient client = getDfsClient(0);
    assertEquals("All datanodes must be alive", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    // test 1, verify the replica in IN_MAINTENANCE state isn't in LocatedBlock
    checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService);

    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(), 0, null,
        AdminStates.DECOMMISSIONED);

    // test 2 after decommission has completed, the replication count is
    // replicas + 1 which includes the decommissioned node.
    checkWithRetry(ns, fileSys, file, replicas + 1, null);

    // test 3, put the node in service, replication count should restore.
    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    checkWithRetry(ns, fileSys, file, replicas, null);

    cleanupFile(fileSys, file);
  }

  /**
   * Transition from decommissioning state to maintenance state.
   */
  @Test(timeout = 360000)
  public void testTransitionFromDecommissioning() throws IOException {
    LOG.info("Starting testTransitionFromDecommissioning");
    startCluster(1, 3);

    final Path file = new Path("/testTransitionFromDecommissioning.dat");
    final int replicas = 3;

    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);

    writeFile(fileSys, file, replicas);

    final DatanodeInfo nodeOutofService = takeNodeOutofService(0, null, 0,
        null, AdminStates.DECOMMISSION_INPROGRESS);

    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(), Long.MAX_VALUE,
        null, AdminStates.IN_MAINTENANCE);

    checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService);

    cleanupFile(fileSys, file);
  }


  /**
   * First put a node in maintenance, then put a different node
   * in decommission. Make sure decommission process take
   * maintenance replica into account.
   */
  @Test(timeout = 360000)
  public void testDecommissionDifferentNodeAfterMaintenances()
      throws Exception {
    testDecommissionDifferentNodeAfterMaintenance(2);
    testDecommissionDifferentNodeAfterMaintenance(3);
    testDecommissionDifferentNodeAfterMaintenance(4);
  }

  private void testDecommissionDifferentNodeAfterMaintenance(int repl)
      throws Exception {
    setup();
    startCluster(1, 5);

    final Path file =
        new Path("/testDecommissionDifferentNodeAfterMaintenance.dat");

    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);

    writeFile(fileSys, file, repl, 1);
    final DatanodeInfo[] nodes = getFirstBlockReplicasDatanodeInfos(fileSys,
        file);
    String maintenanceDNUuid = nodes[0].getDatanodeUuid();
    String decommissionDNUuid = nodes[1].getDatanodeUuid();
    DatanodeInfo maintenanceDN = takeNodeOutofService(0, maintenanceDNUuid,
        Long.MAX_VALUE, null, null, AdminStates.IN_MAINTENANCE);

    Map<DatanodeInfo, Long> maintenanceNodes = new HashMap<>();
    maintenanceNodes.put(nodes[0], Long.MAX_VALUE);
    takeNodeOutofService(0, decommissionDNUuid, 0, null, maintenanceNodes,
        AdminStates.DECOMMISSIONED);
    // Out of the replicas returned, one is the decommissioned node.
    checkWithRetry(ns, fileSys, file, repl, maintenanceDN);

    putNodeInService(0, maintenanceDN);
    checkWithRetry(ns, fileSys, file, repl + 1, null);

    cleanupFile(fileSys, file);
    teardown();
  }

  /**
   * Verify if multiple DataNodes can transition to maintenance state
   * at the same time.
   */
  @Test(timeout = 360000)
  public void testMultipleNodesMaintenance() throws Exception {
    startCluster(1, 5);
    final Path file = new Path("/testMultipleNodesMaintenance.dat");
    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);

    int repl = 3;
    writeFile(fileSys, file, repl, 1);
    DFSTestUtil.waitForReplication((DistributedFileSystem) fileSys, file,
        (short) repl, 10000);
    final DatanodeInfo[] nodes = getFirstBlockReplicasDatanodeInfos(fileSys,
        file);

    // Request maintenance for DataNodes 1 and 2 which has the file blocks.
    List<DatanodeInfo> maintenanceDN = takeNodeOutofService(0,
        Lists.newArrayList(nodes[0].getDatanodeUuid(),
            nodes[1].getDatanodeUuid()), Long.MAX_VALUE, null, null,
        AdminStates.IN_MAINTENANCE);

    // Verify file replication matches maintenance state min replication
    checkWithRetry(ns, fileSys, file, 1, null, nodes[0]);

    // Put the maintenance nodes back in service
    for (DatanodeInfo datanodeInfo : maintenanceDN) {
      putNodeInService(0, datanodeInfo);
    }

    // Verify file replication catching up to the old state
    checkWithRetry(ns, fileSys, file, repl, null);

    cleanupFile(fileSys, file);
  }

  @Test(timeout = 360000)
  public void testChangeReplicationFactors() throws IOException {
    // Prior to any change, there is 1 maintenance node and 2 live nodes.

    // Replication factor is adjusted from 3 to 4.
    // After the change, given 1 maintenance + 2 live is less than the
    // newFactor, one live nodes will be added.
    testChangeReplicationFactor(3, 4, 3);

    // Replication factor is adjusted from 3 to 2.
    // After the change, given 2 live nodes is the same as the newFactor,
    // no live nodes will be invalidated.
    testChangeReplicationFactor(3, 2, 2);

    // Replication factor is adjusted from 3 to 1.
    // After the change, given 2 live nodes is greater than the newFactor,
    // one live nodes will be invalidated.
    testChangeReplicationFactor(3, 1, 1);
  }

  /**
   * After the change of replication factor, # of live replicas <=
   * the new replication factor.
   */
  private void testChangeReplicationFactor(int oldFactor, int newFactor,
      int expectedLiveReplicas) throws IOException {
    setup();
    LOG.info("Starting testChangeReplicationFactor {} {} {}",
        oldFactor, newFactor, expectedLiveReplicas);
    startCluster(1, 5);

    final Path file = new Path("/testChangeReplicationFactor.dat");

    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);

    writeFile(fileSys, file, oldFactor, 1);

    final DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        getFirstBlockFirstReplicaUuid(fileSys, file), Long.MAX_VALUE, null,
        AdminStates.IN_MAINTENANCE);

    // Verify that the nodeOutofService remains in blocksMap and
    // # of live replicas For read operation is expected.
    checkWithRetry(ns, fileSys, file, oldFactor - 1,
        nodeOutofService);

    final DFSClient client = getDfsClient(0);
    client.setReplication(file.toString(), (short)newFactor);

    // Verify that the nodeOutofService remains in blocksMap and
    // # of live replicas for read operation.
    checkWithRetry(ns, fileSys, file, expectedLiveReplicas,
        nodeOutofService);

    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    checkWithRetry(ns, fileSys, file, newFactor, null);

    cleanupFile(fileSys, file);
    teardown();
  }


  /**
   * Verify the following scenario.
   * a. Put a live node to maintenance => 1 maintenance, 2 live.
   * b. The maintenance node becomes dead => block map still has 1 maintenance,
   *    2 live.
   * c. Take the node out of maintenance => NN should schedule the replication
   *    and end up with 3 live.
   */
  @Test(timeout = 360000)
  public void testTakeDeadNodeOutOfMaintenance() throws Exception {
    LOG.info("Starting testTakeDeadNodeOutOfMaintenance");
    final int numNamenodes = 1;
    final int numDatanodes = 4;
    startCluster(numNamenodes, numDatanodes);

    final Path file = new Path("/testTakeDeadNodeOutOfMaintenance.dat");
    final int replicas = 3;

    final FileSystem fileSys = getCluster().getFileSystem(0);
    final FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file, replicas, 1);

    final DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        getFirstBlockFirstReplicaUuid(fileSys, file), Long.MAX_VALUE, null,
        AdminStates.IN_MAINTENANCE);

    checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService);

    final DFSClient client = getDfsClient(0);
    assertEquals("All datanodes must be alive", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    getCluster().stopDataNode(nodeOutofService.getXferAddr());
    DFSTestUtil.waitForDatanodeState(
        getCluster(), nodeOutofService.getDatanodeUuid(), false, 20000);
    assertEquals("maintenance node shouldn't be alive", numDatanodes - 1,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    // Dead maintenance node's blocks should remain in block map.
    checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService);

    // When dead maintenance mode is transitioned to out of maintenance mode,
    // its blocks should be removed from block map.
    // This will then trigger replication to restore the live replicas back
    // to replication factor.
    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    checkWithRetry(ns, fileSys, file, replicas, nodeOutofService,
        null);

    cleanupFile(fileSys, file);
  }


  /**
   * Verify the following scenario.
   * a. Put a live node to maintenance => 1 maintenance, 2 live.
   * b. The maintenance node becomes dead => block map still has 1 maintenance,
   *    2 live.
   * c. Restart nn => block map only has 2 live => restore the 3 live.
   * d. Restart the maintenance dn => 1 maintenance, 3 live.
   * e. Take the node out of maintenance => over replication => 3 live.
   */
  @Test(timeout = 360000)
  public void testWithNNAndDNRestart() throws Exception {
    LOG.info("Starting testWithNNAndDNRestart");
    final int numNamenodes = 1;
    final int numDatanodes = 4;
    startCluster(numNamenodes, numDatanodes);

    final Path file = new Path("/testWithNNAndDNRestart.dat");
    final int replicas = 3;

    final FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file, replicas, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        getFirstBlockFirstReplicaUuid(fileSys, file), Long.MAX_VALUE, null,
        AdminStates.IN_MAINTENANCE);

    checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService);

    DFSClient client = getDfsClient(0);
    assertEquals("All datanodes must be alive", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    MiniDFSCluster.DataNodeProperties dnProp =
        getCluster().stopDataNode(nodeOutofService.getXferAddr());
    DFSTestUtil.waitForDatanodeState(
        getCluster(), nodeOutofService.getDatanodeUuid(), false, 20000);
    assertEquals("maintenance node shouldn't be alive", numDatanodes - 1,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    // Dead maintenance node's blocks should remain in block map.
    checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService);

    // restart nn, nn will restore 3 live replicas given it doesn't
    // know the maintenance node has the replica.
    getCluster().restartNameNode(0);
    ns = getCluster().getNamesystem(0);
    checkWithRetry(ns, fileSys, file, replicas, null);

    // restart dn, nn has 1 maintenance replica and 3 live replicas.
    getCluster().restartDataNode(dnProp, true);
    getCluster().waitActive();
    checkWithRetry(ns, fileSys, file, replicas, nodeOutofService);

    // Put the node in service, a redundant replica should be removed.
    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    checkWithRetry(ns, fileSys, file, replicas, null);

    cleanupFile(fileSys, file);
  }


  /**
   * Machine under maintenance state won't be chosen for new block allocation.
   */
  @Test(timeout = 3600000)
  public void testWriteAfterMaintenance() throws IOException {
    LOG.info("Starting testWriteAfterMaintenance");
    startCluster(1, 3);

    final Path file = new Path("/testWriteAfterMaintenance.dat");
    int replicas = 3;

    final FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);

    final DatanodeInfo nodeOutofService = takeNodeOutofService(0, null,
        Long.MAX_VALUE, null, AdminStates.IN_MAINTENANCE);

    writeFile(fileSys, file, replicas, 2);

    // Verify nodeOutofService wasn't chosen for write operation.
    checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService, null);

    // Put the node back to service, live replicas should be restored.
    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    checkWithRetry(ns, fileSys, file, replicas, null);

    cleanupFile(fileSys, file);
  }

  /**
   * A node has blocks under construction when it is put to maintenance.
   * Given there are minReplication replicas somewhere else,
   * it can be transitioned to AdminStates.IN_MAINTENANCE.
   */
  @Test(timeout = 360000)
  public void testEnterMaintenanceWhenFileOpen() throws Exception {
    LOG.info("Starting testEnterMaintenanceWhenFileOpen");
    startCluster(1, 3);

    final Path file = new Path("/testEnterMaintenanceWhenFileOpen.dat");

    final FileSystem fileSys = getCluster().getFileSystem(0);
    writeIncompleteFile(fileSys, file, (short)3, (short)2);

    takeNodeOutofService(0, null, Long.MAX_VALUE, null,
        AdminStates.IN_MAINTENANCE);

    cleanupFile(fileSys, file);
  }

  /**
   * Machine under maintenance state won't be chosen for invalidation.
   */
  @Test(timeout = 360000)
  public void testInvalidation() throws IOException {
    LOG.info("Starting testInvalidation");
    int numNamenodes = 1;
    int numDatanodes = 3;
    startCluster(numNamenodes, numDatanodes);

    Path file = new Path("/testInvalidation.dat");
    int replicas = 3;

    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);

    writeFile(fileSys, file, replicas);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0, null,
        Long.MAX_VALUE, null, AdminStates.IN_MAINTENANCE);

    DFSClient client = getDfsClient(0);
    client.setReplication(file.toString(), (short) 1);

    // Verify the nodeOutofService remains in blocksMap.
    checkWithRetry(ns, fileSys, file, 1, nodeOutofService);

    // Restart NN and verify the nodeOutofService remains in blocksMap.
    getCluster().restartNameNode(0);
    ns = getCluster().getNamesystem(0);
    checkWithRetry(ns, fileSys, file, 1, nodeOutofService);

    cleanupFile(fileSys, file);
  }

  @Test(timeout = 120000)
  public void testFileCloseAfterEnteringMaintenance() throws Exception {
    LOG.info("Starting testFileCloseAfterEnteringMaintenance");
    int expirationInMs = 30 * 1000;
    int numDataNodes = 3;
    int numNameNodes = 1;
    getConf().setInt(DFSConfigKeys.DFS_NAMENODE_REPLICATION_MIN_KEY, 2);

    startCluster(numNameNodes, numDataNodes);
    getCluster().waitActive();

    FSNamesystem fsn = getCluster().getNameNode().getNamesystem();
    List<String> hosts = new ArrayList<>();
    for (DataNode dn : getCluster().getDataNodes()) {
      hosts.add(dn.getDisplayName());
      putNodeInService(0, dn.getDatanodeUuid());
    }
    assertEquals(numDataNodes, fsn.getNumLiveDataNodes());

    Path openFile = new Path("/testClosingFileInMaintenance.dat");
    // Lets write 2 blocks of data to the openFile
    writeFile(getCluster().getFileSystem(), openFile, (short) 3);

    // Lets write some more data and keep the file open
    FSDataOutputStream fsDataOutputStream = getCluster().getFileSystem()
        .append(openFile);
    byte[] bytes = new byte[1024];
    fsDataOutputStream.write(bytes);
    fsDataOutputStream.hsync();

    LocatedBlocks lbs = NameNodeAdapter.getBlockLocations(
        getCluster().getNameNode(0), openFile.toString(), 0, 3 * blockSize);
    DatanodeInfo[] dnInfos4LastBlock = lbs.getLastLocatedBlock().getLocations();

    // Request maintenance for DataNodes 1 and 2 which has the last block.
    takeNodeOutofService(0,
        Lists.newArrayList(dnInfos4LastBlock[0].getDatanodeUuid(),
            dnInfos4LastBlock[1].getDatanodeUuid()),
        Time.now() + expirationInMs,
        null, null, AdminStates.ENTERING_MAINTENANCE);

    // Closing the file should succeed even when the
    // last blocks' nodes are entering maintenance.
    fsDataOutputStream.close();
    cleanupFile(getCluster().getFileSystem(), openFile);
  }

  static String getFirstBlockFirstReplicaUuid(FileSystem fileSys,
      Path name) throws IOException {
    DatanodeInfo[] nodes = getFirstBlockReplicasDatanodeInfos(fileSys, name);
    if (nodes != null && nodes.length != 0) {
      return nodes[0].getDatanodeUuid();
    } else {
      return null;
    }
  }

  /*
  * Verify that the number of replicas are as expected for each block in
  * the given file.
  *
  * @return - null if no failure found, else an error message string.
  */
  static String checkFile(FSNamesystem ns, FileSystem fileSys,
      Path name, int repl, DatanodeInfo expectedExcludedNode,
      DatanodeInfo expectedMaintenanceNode) throws IOException {
    // need a raw stream
    assertTrue("Not HDFS:"+fileSys.getUri(),
        fileSys instanceof DistributedFileSystem);
    HdfsDataInputStream dis = (HdfsDataInputStream)fileSys.open(name);
    BlockManager bm = ns.getBlockManager();
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();
    String output;
    for (LocatedBlock blk : dinfo) { // for each block
      DatanodeInfo[] nodes = blk.getLocations();
      for (int j = 0; j < nodes.length; j++) { // for each replica
        if (expectedExcludedNode != null &&
            nodes[j].equals(expectedExcludedNode)) {
          //excluded node must not be in LocatedBlock.
          output = "For block " + blk.getBlock() + " replica on " +
              nodes[j] + " found in LocatedBlock.";
          LOG.info(output);
          return output;
        } else {
          if (nodes[j].isInMaintenance()) {
            //IN_MAINTENANCE node must not be in LocatedBlock.
            output = "For block " + blk.getBlock() + " replica on " +
                nodes[j] + " which is in maintenance state.";
            LOG.info(output);
            return output;
          }
        }
      }
      if (repl != nodes.length) {
        output = "Wrong number of replicas for block " + blk.getBlock() +
            ": expected " + repl + ", got " + nodes.length + " ,";
        for (int j = 0; j < nodes.length; j++) { // for each replica
          output += nodes[j] + ",";
        }
        output += "pending block # " + ns.getPendingReplicationBlocks() + " ,";
        output += "under replicated # " + ns.getUnderReplicatedBlocks() + " ,";
        if (expectedExcludedNode != null) {
          output += "excluded node " + expectedExcludedNode;
        }

        LOG.info(output);
        return output;
      }

      // Verify it has the expected maintenance node
      Iterator<DatanodeStorageInfo> storageInfoIter =
          bm.getStorages(blk.getBlock().getLocalBlock()).iterator();
      List<DatanodeInfo> maintenanceNodes = new ArrayList<>();
      while (storageInfoIter.hasNext()) {
        DatanodeInfo node = storageInfoIter.next().getDatanodeDescriptor();
        if (node.isMaintenance()) {
          maintenanceNodes.add(node);
        }
      }

      if (expectedMaintenanceNode != null) {
        if (!maintenanceNodes.contains(expectedMaintenanceNode)) {
          output = "No maintenance replica on " + expectedMaintenanceNode;
          LOG.info(output);
          return output;
        }
      } else {
        if (maintenanceNodes.size() != 0) {
          output = "Has maintenance replica(s)";
          LOG.info(output);
          return output;
        }
      }
    }
    return null;
  }

  static void checkWithRetry(FSNamesystem ns, FileSystem fileSys, Path name,
      int repl, DatanodeInfo inMaintenanceNode) {
    checkWithRetry(ns, fileSys, name, repl, inMaintenanceNode,
        inMaintenanceNode);
  }

  static void checkWithRetry(final FSNamesystem ns, final FileSystem fileSys,
      final Path name, final int repl, final DatanodeInfo excludedNode,
      final DatanodeInfo underMaintenanceNode) {
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {

        @Override
        public Boolean get() {
          String output = null;
          try {
            output = checkFile(ns, fileSys, name, repl, excludedNode,
                underMaintenanceNode);
          } catch (Exception ignored) {
          }

          return (output == null);
        }
      }, 100, 60000);
    } catch (Exception ignored) {
    }
  }

  static private DatanodeInfo[] getFirstBlockReplicasDatanodeInfos(
      FileSystem fileSys, Path name) throws IOException {
    // need a raw stream
    assertTrue("Not HDFS:"+fileSys.getUri(),
        fileSys instanceof DistributedFileSystem);
    HdfsDataInputStream dis = (HdfsDataInputStream)fileSys.open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();
    if (dinfo.iterator().hasNext()) { // for the first block
      return dinfo.iterator().next().getLocations();
    } else {
      return null;
    }
  }

  @Test(timeout = 120000)
  public void testReportMaintenanceNodes() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setOut(new PrintStream(out));
    System.setErr(new PrintStream(err));

    LOG.info("Starting testReportMaintenanceNodes");
    int expirationInMs = 30 * 1000;
    int numNodes = 2;
    setMinMaintenanceR(numNodes);

    startCluster(1, numNodes);
    getCluster().waitActive();

    FileSystem fileSys = getCluster().getFileSystem(0);
    getConf().set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        fileSys.getUri().toString());
    DFSAdmin dfsAdmin = new DFSAdmin(getConf());

    FSNamesystem fsn = getCluster().getNameNode().getNamesystem();
    assertEquals(numNodes, fsn.getNumLiveDataNodes());

    int ret = ToolRunner.run(dfsAdmin,
        new String[] {"-report", "-enteringmaintenance", "-inmaintenance"});
    assertEquals(0, ret);
    assertThat(out.toString(),
        is(allOf(containsString("Entering maintenance datanodes (0):"),
            containsString("In maintenance datanodes (0):"),
            not(containsString(
                getCluster().getDataNodes().get(0).getDisplayName())),
            not(containsString(
                getCluster().getDataNodes().get(1).getDisplayName())))));

    final Path file = new Path("/testReportMaintenanceNodes.dat");
    writeFile(fileSys, file, numNodes, 1);

    DatanodeInfo[] nodes = getFirstBlockReplicasDatanodeInfos(fileSys, file);
    // Request maintenance for DataNodes1. The DataNode1 will not transition
    // to the next state AdminStates.IN_MAINTENANCE immediately since there
    // are not enough candidate nodes to satisfy the min maintenance
    // replication.
    DatanodeInfo maintenanceDN = takeNodeOutofService(0,
        nodes[0].getDatanodeUuid(), Time.now() + expirationInMs, null, null,
        AdminStates.ENTERING_MAINTENANCE);
    assertEquals(1, fsn.getNumEnteringMaintenanceDataNodes());

    // reset stream
    out.reset();
    err.reset();

    ret = ToolRunner.run(dfsAdmin,
        new String[] {"-report", "-enteringmaintenance"});
    assertEquals(0, ret);
    assertThat(out.toString(),
        is(allOf(containsString("Entering maintenance datanodes (1):"),
            containsString(nodes[0].getXferAddr()),
            not(containsString(nodes[1].getXferAddr())))));

    // reset stream
    out.reset();
    err.reset();

    // start a new datanode to make state transition to
    // AdminStates.IN_MAINTENANCE
    getCluster().startDataNodes(getConf(), 1, true, null, null);
    getCluster().waitActive();

    waitNodeState(maintenanceDN, AdminStates.IN_MAINTENANCE);
    assertEquals(1, fsn.getNumInMaintenanceLiveDataNodes());

    ret = ToolRunner.run(dfsAdmin,
        new String[] {"-report", "-inmaintenance"});
    assertEquals(0, ret);
    assertThat(out.toString(),
        is(allOf(containsString("In maintenance datanodes (1):"),
            containsString(nodes[0].getXferAddr()),
            not(containsString(nodes[1].getXferAddr())),
            not(containsString(
                getCluster().getDataNodes().get(2).getDisplayName())))));

    cleanupFile(getCluster().getFileSystem(), file);
  }
}
