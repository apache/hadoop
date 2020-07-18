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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.BlockReportOptions;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Test;

import java.util.function.Supplier;

import java.net.InetSocketAddress;
import java.security.Permission;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * This class tests data node registration.
 */
public class TestDatanodeRegistration {
  
  public static final Logger LOG =
      LoggerFactory.getLogger(TestDatanodeRegistration.class);

  private static class MonitorDNS extends SecurityManager {
    int lookups = 0;
    @Override
    public void checkPermission(Permission perm) {}    
    @Override
    public void checkConnect(String host, int port) {
      if (port == -1) {
        lookups++;
      }
    }
  }

  /**
   * Ensure the datanode manager does not do host lookup after registration,
   * especially for node reports.
   * @throws Exception
   */
  @Test
  public void testDNSLookups() throws Exception {
    MonitorDNS sm = new MonitorDNS();
    System.setSecurityManager(sm);
    
    MiniDFSCluster cluster = null;
    try {
      HdfsConfiguration conf = new HdfsConfiguration();
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(8).build();
      cluster.waitActive();
      
      int initialLookups = sm.lookups;
      assertTrue("dns security manager is active", initialLookups != 0);
      
      DatanodeManager dm =
          cluster.getNamesystem().getBlockManager().getDatanodeManager();
      
      // make sure no lookups occur
      dm.refreshNodes(conf);
      assertEquals(initialLookups, sm.lookups);

      dm.refreshNodes(conf);
      assertEquals(initialLookups, sm.lookups);
      
      // ensure none of the reports trigger lookups
      dm.getDatanodeListForReport(DatanodeReportType.ALL);
      assertEquals(initialLookups, sm.lookups);
      
      dm.getDatanodeListForReport(DatanodeReportType.LIVE);
      assertEquals(initialLookups, sm.lookups);
      
      dm.getDatanodeListForReport(DatanodeReportType.DEAD);
      assertEquals(initialLookups, sm.lookups);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      System.setSecurityManager(null);
    }
  }
  
  /**
   * Regression test for HDFS-894 ensures that, when datanodes
   * are restarted, the new IPC port is registered with the
   * namenode.
   */
  @Test
  public void testChangeIpcPort() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      InetSocketAddress addr = new InetSocketAddress(
        "localhost",
        cluster.getNameNodePort());
      DFSClient client = new DFSClient(addr, conf);

      // Restart datanodes
      cluster.restartDataNodes();

      // Wait until we get a heartbeat from the new datanode
      DatanodeInfo[] report = client.datanodeReport(DatanodeReportType.ALL);
      long firstUpdateAfterRestart = report[0].getLastUpdate();

      boolean gotHeartbeat = false;
      for (int i = 0; i < 10 && !gotHeartbeat; i++) {
        try {
          Thread.sleep(i*1000);
        } catch (InterruptedException ie) {}

        report = client.datanodeReport(DatanodeReportType.ALL);
        gotHeartbeat = (report[0].getLastUpdate() > firstUpdateAfterRestart);
      }
      if (!gotHeartbeat) {
        fail("Never got a heartbeat from restarted datanode.");
      }

      int realIpcPort = cluster.getDataNodes().get(0).getIpcPort();
      // Now make sure the reported IPC port is the correct one.
      assertEquals(realIpcPort, report[0].getIpcPort());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testChangeStorageID() throws Exception {
    final String DN_IP_ADDR = "127.0.0.1";
    final String DN_HOSTNAME = "localhost";
    final int DN_XFER_PORT = 12345;
    final int DN_INFO_PORT = 12346;
    final int DN_INFO_SECURE_PORT = 12347;
    final int DN_IPC_PORT = 12348;
    Configuration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(0)
          .build();
      InetSocketAddress addr = new InetSocketAddress(
        "localhost",
        cluster.getNameNodePort());
      DFSClient client = new DFSClient(addr, conf);
      NamenodeProtocols rpcServer = cluster.getNameNodeRpc();

      // register a datanode
      DatanodeID dnId = new DatanodeID(DN_IP_ADDR, DN_HOSTNAME,
          "fake-datanode-id", DN_XFER_PORT, DN_INFO_PORT, DN_INFO_SECURE_PORT,
          DN_IPC_PORT);
      long nnCTime = cluster.getNamesystem().getFSImage().getStorage()
          .getCTime();
      StorageInfo mockStorageInfo = mock(StorageInfo.class);
      doReturn(nnCTime).when(mockStorageInfo).getCTime();
      doReturn(HdfsServerConstants.DATANODE_LAYOUT_VERSION).when(mockStorageInfo)
          .getLayoutVersion();
      DatanodeRegistration dnReg = new DatanodeRegistration(dnId,
          mockStorageInfo, null, VersionInfo.getVersion());
      rpcServer.registerDatanode(dnReg);

      DatanodeInfo[] report = client.datanodeReport(DatanodeReportType.ALL);
      assertEquals("Expected a registered datanode", 1, report.length);

      // register the same datanode again with a different storage ID
      dnId = new DatanodeID(DN_IP_ADDR, DN_HOSTNAME,
          "changed-fake-datanode-id", DN_XFER_PORT, DN_INFO_PORT,
          DN_INFO_SECURE_PORT, DN_IPC_PORT);
      dnReg = new DatanodeRegistration(dnId,
          mockStorageInfo, null, VersionInfo.getVersion());
      rpcServer.registerDatanode(dnReg);

      report = client.datanodeReport(DatanodeReportType.ALL);
      assertEquals("Datanode with changed storage ID not recognized",
          1, report.length);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testRegistrationWithDifferentSoftwareVersions() throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY, "3.0.0");
    conf.set(DFSConfigKeys.DFS_NAMENODE_MIN_SUPPORTED_DATANODE_VERSION_KEY, "3.0.0");
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(0)
          .build();
      
      NamenodeProtocols rpcServer = cluster.getNameNodeRpc();
      
      long nnCTime = cluster.getNamesystem().getFSImage().getStorage().getCTime();
      StorageInfo mockStorageInfo = mock(StorageInfo.class);
      doReturn(nnCTime).when(mockStorageInfo).getCTime();
      
      DatanodeRegistration mockDnReg = mock(DatanodeRegistration.class);
      doReturn(HdfsServerConstants.DATANODE_LAYOUT_VERSION).when(mockDnReg).getVersion();
      doReturn("127.0.0.1").when(mockDnReg).getIpAddr();
      doReturn(123).when(mockDnReg).getXferPort();
      doReturn("fake-storage-id").when(mockDnReg).getDatanodeUuid();
      doReturn(mockStorageInfo).when(mockDnReg).getStorageInfo();
      
      // Should succeed when software versions are the same.
      doReturn("3.0.0").when(mockDnReg).getSoftwareVersion();
      rpcServer.registerDatanode(mockDnReg);
      
      // Should succeed when software version of DN is above minimum required by NN.
      doReturn("4.0.0").when(mockDnReg).getSoftwareVersion();
      rpcServer.registerDatanode(mockDnReg);
      
      // Should fail when software version of DN is below minimum required by NN.
      doReturn("2.0.0").when(mockDnReg).getSoftwareVersion();
      try {
        rpcServer.registerDatanode(mockDnReg);
        fail("Should not have been able to register DN with too-low version.");
      } catch (IncorrectVersionException ive) {
        GenericTestUtils.assertExceptionContains(
            "The reported DataNode version is too low", ive);
        LOG.info("Got expected exception", ive);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  @Test
  public void testRegistrationWithDifferentSoftwareVersionsDuringUpgrade()
      throws Exception {
    Configuration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_DATANODE_MIN_SUPPORTED_NAMENODE_VERSION_KEY, "1.0.0");
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
          .numDataNodes(0)
          .build();
      
      NamenodeProtocols rpcServer = cluster.getNameNodeRpc();
      
      long nnCTime = cluster.getNamesystem().getFSImage().getStorage().getCTime();
      StorageInfo mockStorageInfo = mock(StorageInfo.class);
      doReturn(nnCTime).when(mockStorageInfo).getCTime();
      
      DatanodeRegistration mockDnReg = mock(DatanodeRegistration.class);
      doReturn(HdfsServerConstants.DATANODE_LAYOUT_VERSION).when(mockDnReg).getVersion();
      doReturn("fake-storage-id").when(mockDnReg).getDatanodeUuid();
      doReturn(mockStorageInfo).when(mockDnReg).getStorageInfo();
      
      // Should succeed when software versions are the same and CTimes are the
      // same.
      doReturn(VersionInfo.getVersion()).when(mockDnReg).getSoftwareVersion();
      doReturn("127.0.0.1").when(mockDnReg).getIpAddr();
      doReturn(123).when(mockDnReg).getXferPort();
      rpcServer.registerDatanode(mockDnReg);
      
      // Should succeed when software versions are the same and CTimes are
      // different.
      doReturn(nnCTime + 1).when(mockStorageInfo).getCTime();
      rpcServer.registerDatanode(mockDnReg);
      
      // Should fail when software version of DN is different from NN and CTimes
      // are different.
      doReturn(VersionInfo.getVersion() + ".1").when(mockDnReg).getSoftwareVersion();
      try {
        rpcServer.registerDatanode(mockDnReg);
        fail("Should not have been able to register DN with different software" +
            " versions and CTimes");
      } catch (IncorrectVersionException ive) {
        GenericTestUtils.assertExceptionContains(
            "does not match CTime of NN", ive);
        LOG.info("Got expected exception", ive);
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  // IBRs are async operations to free up IPC handlers.  This means the IBR
  // response will not contain non-IPC level exceptions - which in practice
  // should not occur other than dead/unregistered node which will trigger a
  // re-registration.  If a non-IPC exception does occur, the safety net is
  // a forced re-registration on the next heartbeat.
  @Test
  public void testForcedRegistration() throws Exception {
    final Configuration conf = new HdfsConfiguration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HANDLER_COUNT_KEY, 4);
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, Integer.MAX_VALUE);

    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      cluster.getHttpUri(0);
      FSNamesystem fsn = cluster.getNamesystem();
      String bpId = fsn.getBlockPoolId();

      DataNode dn = cluster.getDataNodes().get(0);
      DatanodeDescriptor dnd =
          NameNodeAdapter.getDatanode(fsn, dn.getDatanodeId());
      DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
      DatanodeStorageInfo storage = dnd.getStorageInfos()[0];

      // registration should not change after heartbeat.
      assertTrue(dnd.isRegistered());
      DatanodeRegistration lastReg = dn.getDNRegistrationForBP(bpId);
      waitForHeartbeat(dn, dnd);
      assertSame(lastReg, dn.getDNRegistrationForBP(bpId));

      // force a re-registration on next heartbeat.
      dnd.setForceRegistration(true);
      assertFalse(dnd.isRegistered());
      waitForHeartbeat(dn, dnd);
      assertTrue(dnd.isRegistered());
      DatanodeRegistration newReg = dn.getDNRegistrationForBP(bpId);
      assertNotSame(lastReg, newReg);
      lastReg = newReg;

      // registration should not change on subsequent heartbeats.
      waitForHeartbeat(dn, dnd);
      assertTrue(dnd.isRegistered());
      assertSame(lastReg, dn.getDNRegistrationForBP(bpId));
      assertTrue("block report is not processed for DN " + dnd,
          waitForBlockReport(dn, dnd));
      assertTrue(dnd.isRegistered());
      assertSame(lastReg, dn.getDNRegistrationForBP(bpId));

      // check that block report is not processed and registration didn't
      // change.
      dnd.setForceRegistration(true);
      assertFalse("block report is processed for DN " + dnd,
          waitForBlockReport(dn, dnd));
      assertFalse(dnd.isRegistered());
      assertSame(lastReg, dn.getDNRegistrationForBP(bpId));

      // heartbeat should trigger re-registration, and next block report
      // should not change registration.
      waitForHeartbeat(dn, dnd);
      assertTrue(dnd.isRegistered());
      newReg = dn.getDNRegistrationForBP(bpId);
      assertNotSame(lastReg, newReg);
      lastReg = newReg;
      assertTrue("block report is not processed for DN " + dnd,
          waitForBlockReport(dn, dnd));
      assertTrue(dnd.isRegistered());
      assertSame(lastReg, dn.getDNRegistrationForBP(bpId));

      // registration doesn't change.
      ExtendedBlock eb = new ExtendedBlock(bpId, 1234);
      dn.notifyNamenodeDeletedBlock(eb, storage.getStorageID());
      DataNodeTestUtils.triggerDeletionReport(dn);
      assertTrue(dnd.isRegistered());
      assertSame(lastReg, dn.getDNRegistrationForBP(bpId));

      // a failed IBR will effectively unregister the node.
      boolean failed = false;
      try {
        // pass null to cause a failure since there aren't any easy failure
        // modes since it shouldn't happen.
        fsn.processIncrementalBlockReport(lastReg, null);
      } catch (NullPointerException npe) {
        failed = true;
      }
      assertTrue("didn't fail", failed);
      assertFalse(dnd.isRegistered());

      // should remain unregistered until next heartbeat.
      dn.notifyNamenodeDeletedBlock(eb, storage.getStorageID());
      DataNodeTestUtils.triggerDeletionReport(dn);
      assertFalse(dnd.isRegistered());
      assertSame(lastReg, dn.getDNRegistrationForBP(bpId));
      waitForHeartbeat(dn, dnd);
      assertTrue(dnd.isRegistered());
      assertNotSame(lastReg, dn.getDNRegistrationForBP(bpId));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void waitForHeartbeat(final DataNode dn, final DatanodeDescriptor dnd)
      throws Exception {
    final long lastUpdate = dnd.getLastUpdateMonotonic();
    Thread.sleep(1);
    DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, false);
    DataNodeTestUtils.triggerHeartbeat(dn);
    GenericTestUtils.waitFor(new Supplier<Boolean>() {
      @Override
      public Boolean get() {
        return lastUpdate != dnd.getLastUpdateMonotonic();
      }
    }, 10, 100000);
    DataNodeTestUtils.setHeartbeatsDisabledForTests(dn, true);
  }

  private boolean waitForBlockReport(final DataNode dn,
      final DatanodeDescriptor dnd) throws Exception {
    final DatanodeStorageInfo storage = dnd.getStorageInfos()[0];
    final long lastCount = storage.getBlockReportCount();
    dn.triggerBlockReport(
        new BlockReportOptions.Factory().setIncremental(false).build());
    try {
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return lastCount != storage.getBlockReportCount();
        }
      }, 10, 6000);
    } catch (TimeoutException te) {
      LOG.error("Timeout waiting for block report for {}", dnd);
      return false;
    }
    return true;
  }
}
