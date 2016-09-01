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
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.util.Time;
import org.junit.Test;

/**
 * This class tests node maintenance.
 */
public class TestMaintenanceState extends AdminStatesBaseTest {
  public static final Log LOG = LogFactory.getLog(TestMaintenanceState.class);
  static private final long EXPIRATION_IN_MS = 500;

  public TestMaintenanceState() {
    setUseCombinedHostFileManager();
  }

  /**
   * Verify a node can transition from AdminStates.ENTERING_MAINTENANCE to
   * AdminStates.NORMAL.
   */
  @Test(timeout = 360000)
  public void testTakeNodeOutOfEnteringMaintenance() throws Exception {
    LOG.info("Starting testTakeNodeOutOfEnteringMaintenance");
    final int replicas = 1;
    final int numNamenodes = 1;
    final int numDatanodes = 1;
    final Path file1 = new Path("/testTakeNodeOutOfEnteringMaintenance.dat");

    startCluster(numNamenodes, numDatanodes);

    FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file1, replicas, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        null, Long.MAX_VALUE, null, AdminStates.ENTERING_MAINTENANCE);

    putNodeInService(0, nodeOutofService.getDatanodeUuid());

    cleanupFile(fileSys, file1);
  }

  /**
   * Verify a AdminStates.ENTERING_MAINTENANCE node can expire and transition
   * to AdminStates.NORMAL upon timeout.
   */
  @Test(timeout = 360000)
  public void testEnteringMaintenanceExpiration() throws Exception {
    LOG.info("Starting testEnteringMaintenanceExpiration");
    final int replicas = 1;
    final int numNamenodes = 1;
    final int numDatanodes = 1;
    final Path file1 = new Path("/testTakeNodeOutOfEnteringMaintenance.dat");

    startCluster(numNamenodes, numDatanodes);

    FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file1, replicas, 1);

    // expires in 500 milliseconds
    DatanodeInfo nodeOutofService = takeNodeOutofService(0, null,
        Time.monotonicNow() + EXPIRATION_IN_MS, null,
        AdminStates.ENTERING_MAINTENANCE);

    waitNodeState(nodeOutofService, AdminStates.NORMAL);

    cleanupFile(fileSys, file1);
  }

  /**
   * Verify node stays in AdminStates.NORMAL with invalid expiration.
   */
  @Test(timeout = 360000)
  public void testInvalidExpiration() throws Exception {
    LOG.info("Starting testInvalidExpiration");
    final int replicas = 1;
    final int numNamenodes = 1;
    final int numDatanodes = 1;
    final Path file1 = new Path("/testTakeNodeOutOfEnteringMaintenance.dat");

    startCluster(numNamenodes, numDatanodes);

    FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file1, replicas, 1);

    // expiration has to be greater than Time.monotonicNow().
    takeNodeOutofService(0, null, Time.monotonicNow(), null,
        AdminStates.NORMAL);

    cleanupFile(fileSys, file1);
  }

  /**
   * When a dead node is put to maintenance, it transitions directly to
   * AdminStates.IN_MAINTENANCE.
   */
  @Test(timeout = 360000)
  public void testPutDeadNodeToMaintenance() throws Exception {
    LOG.info("Starting testPutDeadNodeToMaintenance");
    final int numNamenodes = 1;
    final int numDatanodes = 1;
    final int replicas = 1;
    final Path file1 = new Path("/testPutDeadNodeToMaintenance.dat");

    startCluster(numNamenodes, numDatanodes);

    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file1, replicas, 1);

    MiniDFSCluster.DataNodeProperties dnProp = getCluster().stopDataNode(0);
    DFSTestUtil.waitForDatanodeState(
        getCluster(), dnProp.datanode.getDatanodeUuid(), false, 20000);

    int deadInMaintenance = ns.getNumInMaintenanceDeadDataNodes();
    int liveInMaintenance = ns.getNumInMaintenanceLiveDataNodes();

    takeNodeOutofService(0, dnProp.datanode.getDatanodeUuid(), Long.MAX_VALUE,
        null, AdminStates.IN_MAINTENANCE);

    assertEquals(deadInMaintenance + 1, ns.getNumInMaintenanceDeadDataNodes());
    assertEquals(liveInMaintenance, ns.getNumInMaintenanceLiveDataNodes());

    cleanupFile(fileSys, file1);
  }

  /**
   * When a dead node is put to maintenance, it transitions directly to
   * AdminStates.IN_MAINTENANCE. Then AdminStates.IN_MAINTENANCE expires and
   * transitions to AdminStates.NORMAL.
   */
  @Test(timeout = 360000)
  public void testPutDeadNodeToMaintenanceWithExpiration() throws Exception {
    LOG.info("Starting testPutDeadNodeToMaintenanceWithExpiration");
    final int numNamenodes = 1;
    final int numDatanodes = 1;
    final int replicas = 1;
    final Path file1 = new Path("/testPutDeadNodeToMaintenance.dat");

    startCluster(numNamenodes, numDatanodes);

    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file1, replicas, 1);

    MiniDFSCluster.DataNodeProperties dnProp = getCluster().stopDataNode(0);
    DFSTestUtil.waitForDatanodeState(
        getCluster(), dnProp.datanode.getDatanodeUuid(), false, 20000);

    int deadInMaintenance = ns.getNumInMaintenanceDeadDataNodes();
    int liveInMaintenance = ns.getNumInMaintenanceLiveDataNodes();

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        dnProp.datanode.getDatanodeUuid(),
        Time.monotonicNow() + EXPIRATION_IN_MS, null,
        AdminStates.IN_MAINTENANCE);

    waitNodeState(nodeOutofService, AdminStates.NORMAL);

    // no change
    assertEquals(deadInMaintenance, ns.getNumInMaintenanceDeadDataNodes());
    assertEquals(liveInMaintenance, ns.getNumInMaintenanceLiveDataNodes());

    cleanupFile(fileSys, file1);
  }

  /**
   * Transition from decommissioned state to maintenance state.
   */
  @Test(timeout = 360000)
  public void testTransitionFromDecommissioned() throws IOException {
    LOG.info("Starting testTransitionFromDecommissioned");
    final int numNamenodes = 1;
    final int numDatanodes = 4;
    final int replicas = 3;
    final Path file1 = new Path("/testTransitionFromDecommissioned.dat");

    startCluster(numNamenodes, numDatanodes);

    FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file1, replicas, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0, null, 0, null,
        AdminStates.DECOMMISSIONED);

    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(), Long.MAX_VALUE,
        null, AdminStates.IN_MAINTENANCE);

    cleanupFile(fileSys, file1);
  }

  /**
   * Transition from decommissioned state to maintenance state.
   * After the maintenance state expires, it is transitioned to NORMAL.
   */
  @Test(timeout = 360000)
  public void testTransitionFromDecommissionedAndExpired() throws IOException {
    LOG.info("Starting testTransitionFromDecommissionedAndExpired");
    final int numNamenodes = 1;
    final int numDatanodes = 4;
    final int replicas = 3;
    final Path file1 = new Path("/testTransitionFromDecommissioned.dat");

    startCluster(numNamenodes, numDatanodes);

    FileSystem fileSys = getCluster().getFileSystem(0);
    writeFile(fileSys, file1, replicas, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0, null, 0, null,
        AdminStates.DECOMMISSIONED);

    takeNodeOutofService(0, nodeOutofService.getDatanodeUuid(),
        Time.monotonicNow() + EXPIRATION_IN_MS, null,
        AdminStates.IN_MAINTENANCE);

    waitNodeState(nodeOutofService, AdminStates.NORMAL);

    cleanupFile(fileSys, file1);
  }

  /**
   * When a node is put to maintenance, it first transitions to
   * AdminStates.ENTERING_MAINTENANCE. It makes sure all blocks have minimal
   * replication before it can be transitioned to AdminStates.IN_MAINTENANCE.
   * If node becomes dead when it is in AdminStates.ENTERING_MAINTENANCE, admin
   * state should stay in AdminStates.ENTERING_MAINTENANCE state.
   */
  @Test(timeout = 360000)
  public void testNodeDeadWhenInEnteringMaintenance() throws Exception {
    LOG.info("Starting testNodeDeadWhenInEnteringMaintenance");
    final int numNamenodes = 1;
    final int numDatanodes = 1;
    final int replicas = 1;
    final Path file1 = new Path("/testNodeDeadWhenInEnteringMaintenance.dat");

    startCluster(numNamenodes, numDatanodes);

    FileSystem fileSys = getCluster().getFileSystem(0);
    FSNamesystem ns = getCluster().getNamesystem(0);
    writeFile(fileSys, file1, replicas, 1);

    DatanodeInfo nodeOutofService = takeNodeOutofService(0,
        getFirstBlockFirstReplicaUuid(fileSys, file1), Long.MAX_VALUE, null,
        AdminStates.ENTERING_MAINTENANCE);
    assertEquals(1, ns.getNumEnteringMaintenanceDataNodes());

    MiniDFSCluster.DataNodeProperties dnProp =
        getCluster().stopDataNode(nodeOutofService.getXferAddr());
    DFSTestUtil.waitForDatanodeState(
        getCluster(), nodeOutofService.getDatanodeUuid(), false, 20000);
    DFSClient client = getDfsClient(0);
    assertEquals("maintenance node shouldn't be alive", numDatanodes - 1,
        client.datanodeReport(DatanodeReportType.LIVE).length);

    getCluster().restartDataNode(dnProp, true);
    getCluster().waitActive();
    waitNodeState(nodeOutofService, AdminStates.ENTERING_MAINTENANCE);
    assertEquals(1, ns.getNumEnteringMaintenanceDataNodes());

    cleanupFile(fileSys, file1);
  }

  static protected String getFirstBlockFirstReplicaUuid(FileSystem fileSys,
      Path name) throws IOException {
    // need a raw stream
    assertTrue("Not HDFS:"+fileSys.getUri(),
        fileSys instanceof DistributedFileSystem);
    HdfsDataInputStream dis = (HdfsDataInputStream)fileSys.open(name);
    Collection<LocatedBlock> dinfo = dis.getAllBlocks();
    for (LocatedBlock blk : dinfo) { // for each block
      DatanodeInfo[] nodes = blk.getLocations();
      if (nodes.length > 0) {
        return nodes[0].getDatanodeUuid();
      }
    }
    return null;
  }
}
