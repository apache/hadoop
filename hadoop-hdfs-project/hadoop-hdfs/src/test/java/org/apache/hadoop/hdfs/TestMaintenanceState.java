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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.util.Time;
import org.junit.Test;

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
    assertNull(checkWithRetry(ns, fileSys, file, replicas, null,
        nodeOutofService));

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
        Time.monotonicNow() + EXPIRATION_IN_MS, null, AdminStates.NORMAL);

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

    // expiration has to be greater than Time.monotonicNow().
    takeNodeOutofService(0, null, Time.monotonicNow(), null,
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
        Time.monotonicNow() + EXPIRATION_IN_MS, null, AdminStates.NORMAL);

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
        Time.monotonicNow() + EXPIRATION_IN_MS, null, AdminStates.NORMAL);

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
    assertNull(checkWithRetry(ns, fileSys, file, expectedReplicasInRead,
        nodeOutofService));

    cleanupFile(fileSys, file);
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
        Time.monotonicNow() + EXPIRATION_IN_MS, null, AdminStates.NORMAL);

    cleanupFile(fileSys, file);
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

    writeFile(fileSys, file, replicas, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        getFirstBlockFirstReplicaUuid(fileSys, file), Long.MAX_VALUE, null,
        AdminStates.IN_MAINTENANCE);

    DFSClient client = getDfsClient(0);
    assertEquals("All datanodes must be alive", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    // test 1, verify the replica in IN_MAINTENANCE state isn't in LocatedBlock
    assertNull(checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService));

    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(), 0, null,
        AdminStates.DECOMMISSIONED);

    // test 2 after decommission has completed, the replication count is
    // replicas + 1 which includes the decommissioned node.
    assertNull(checkWithRetry(ns, fileSys, file, replicas + 1, null));

    // test 3, put the node in service, replication count should restore.
    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    assertNull(checkWithRetry(ns, fileSys, file, replicas, null));

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

    assertNull(checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService));

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
    assertNull(checkWithRetry(ns, fileSys, file, repl, maintenanceDN));

    putNodeInService(0, maintenanceDN);
    assertNull(checkWithRetry(ns, fileSys, file, repl + 1, null));

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
    assertNull(checkWithRetry(ns, fileSys, file, oldFactor - 1,
        nodeOutofService));

    final DFSClient client = getDfsClient(0);
    client.setReplication(file.toString(), (short)newFactor);

    // Verify that the nodeOutofService remains in blocksMap and
    // # of live replicas for read operation.
    assertNull(checkWithRetry(ns, fileSys, file, expectedLiveReplicas,
        nodeOutofService));

    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    assertNull(checkWithRetry(ns, fileSys, file, newFactor, null));

    cleanupFile(fileSys, file);
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

    assertNull(checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService));

    final DFSClient client = getDfsClient(0);
    assertEquals("All datanodes must be alive", numDatanodes,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    getCluster().stopDataNode(nodeOutofService.getXferAddr());
    DFSTestUtil.waitForDatanodeState(
        getCluster(), nodeOutofService.getDatanodeUuid(), false, 20000);
    assertEquals("maintenance node shouldn't be alive", numDatanodes - 1,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    // Dead maintenance node's blocks should remain in block map.
    assertNull(checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService));

    // When dead maintenance mode is transitioned to out of maintenance mode,
    // its blocks should be removed from block map.
    // This will then trigger replication to restore the live replicas back
    // to replication factor.
    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    assertNull(checkWithRetry(ns, fileSys, file, replicas, nodeOutofService,
        null));

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

    assertNull(checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService));

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
    assertNull(checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService));

    // restart nn, nn will restore 3 live replicas given it doesn't
    // know the maintenance node has the replica.
    getCluster().restartNameNode(0);
    ns = getCluster().getNamesystem(0);
    assertNull(checkWithRetry(ns, fileSys, file, replicas, null));

    // restart dn, nn has 1 maintenance replica and 3 live replicas.
    getCluster().restartDataNode(dnProp, true);
    getCluster().waitActive();
    assertNull(checkWithRetry(ns, fileSys, file, replicas, nodeOutofService));

    // Put the node in service, a redundant replica should be removed.
    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    assertNull(checkWithRetry(ns, fileSys, file, replicas, null));

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
    assertNull(checkWithRetry(ns, fileSys, file, replicas - 1,
        nodeOutofService, null));

    // Put the node back to service, live replicas should be restored.
    putNodeInService(0, nodeOutofService.getDatanodeUuid());
    assertNull(checkWithRetry(ns, fileSys, file, replicas, null));

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
    assertNull(checkWithRetry(ns, fileSys, file, 1, nodeOutofService));

    // Restart NN and verify the nodeOutofService remains in blocksMap.
    getCluster().restartNameNode(0);
    ns = getCluster().getNamesystem(0);
    assertNull(checkWithRetry(ns, fileSys, file, 1, nodeOutofService));

    cleanupFile(fileSys, file);
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

  static String checkWithRetry(FSNamesystem ns, FileSystem fileSys,
      Path name, int repl, DatanodeInfo inMaintenanceNode)
          throws IOException {
    return checkWithRetry(ns, fileSys, name, repl, inMaintenanceNode,
        inMaintenanceNode);
  }

  static String checkWithRetry(FSNamesystem ns, FileSystem fileSys,
      Path name, int repl, DatanodeInfo excludedNode,
      DatanodeInfo underMaintenanceNode) throws IOException {
    int tries = 0;
    String output = null;
    while (tries++ < 200) {
      try {
        Thread.sleep(100);
        output = checkFile(ns, fileSys, name, repl, excludedNode,
            underMaintenanceNode);
        if (output == null) {
          break;
        }
      } catch (InterruptedException ie) {
      }
    }
    return output;
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
}
