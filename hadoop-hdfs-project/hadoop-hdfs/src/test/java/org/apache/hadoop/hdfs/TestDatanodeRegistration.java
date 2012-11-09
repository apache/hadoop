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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.security.Permission;

import junit.framework.TestCase;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;

import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestDatanodeRegistration extends TestCase {

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

  public void testChangeStorageID() throws Exception {
    final String DN_IP_ADDR = "127.0.0.1";
    final int DN_XFER_PORT = 12345;
    HdfsConfiguration conf = new HdfsConfiguration();
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
      String nodeName = DN_IP_ADDR + ":" + DN_XFER_PORT;
      long nnCTime = cluster.getNameNodeRpc().versionRequest().getCTime();
      StorageInfo mockStorageInfo = mock(StorageInfo.class);
      doReturn(nnCTime).when(mockStorageInfo).getCTime();
      doReturn(HdfsConstants.LAYOUT_VERSION).when(mockStorageInfo)
          .getLayoutVersion();
      doReturn("fake-storage-id").when(mockStorageInfo).getClusterID();
      DatanodeRegistration dnReg = new DatanodeRegistration(nodeName);
      dnReg.storageInfo = mockStorageInfo;
      rpcServer.registerDatanode(dnReg);

      DatanodeInfo[] report = client.datanodeReport(DatanodeReportType.ALL);
      assertEquals("Expected a registered datanode", 1, report.length);

      // register the same datanode again with a different storage ID
      doReturn("changed-fake-storage-id").when(mockStorageInfo).getClusterID();
      dnReg = new DatanodeRegistration(nodeName);
      dnReg.storageInfo = mockStorageInfo;
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
}
