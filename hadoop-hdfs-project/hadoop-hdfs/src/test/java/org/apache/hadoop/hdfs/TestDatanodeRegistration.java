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
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.VersionInfo;
import org.junit.Test;

/**
 * This class tests that a file need not be closed before its
 * data can be read by another client.
 */
public class TestDatanodeRegistration {
  
  public static final Log LOG = LogFactory.getLog(TestDatanodeRegistration.class);

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
      doReturn(HdfsConstants.LAYOUT_VERSION).when(mockDnReg).getVersion();
      doReturn("fake-storage-id").when(mockDnReg).getStorageID();
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
      doReturn(HdfsConstants.LAYOUT_VERSION).when(mockDnReg).getVersion();
      doReturn("fake-storage-id").when(mockDnReg).getStorageID();
      doReturn(mockStorageInfo).when(mockDnReg).getStorageInfo();
      
      // Should succeed when software versions are the same and CTimes are the
      // same.
      doReturn(VersionInfo.getVersion()).when(mockDnReg).getSoftwareVersion();
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
}
