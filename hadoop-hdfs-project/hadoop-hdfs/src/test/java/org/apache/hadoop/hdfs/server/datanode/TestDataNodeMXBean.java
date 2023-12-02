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
package org.apache.hadoop.hdfs.server.datanode;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.eclipse.jetty.util.ajax.JSON;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Class for testing {@link DataNodeMXBean} implementation
 */
public class TestDataNodeMXBean extends SaslDataTransferTestCase {

  public static final Logger LOG =
      LoggerFactory.getLogger(TestDataNodeMXBean.class);

  @Test
  public void testDataNodeMXBean() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      List<DataNode> datanodes = cluster.getDataNodes();
      Assert.assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=DataNode,name=DataNodeInfo");
      // get attribute "ClusterId"
      String clusterId = (String) mbs.getAttribute(mxbeanName, "ClusterId");
      Assert.assertEquals(datanode.getClusterId(), clusterId);
      // get attribute "Version"
      String version = (String)mbs.getAttribute(mxbeanName, "Version");
      Assert.assertEquals(datanode.getVersion(),version);
      // get attribute "DNStartedTimeInMillis"
      long startTime = (long) mbs.getAttribute(mxbeanName, "DNStartedTimeInMillis");
      Assert.assertTrue("Datanode start time should not be 0", startTime > 0);
      Assert.assertEquals(datanode.getDNStartedTimeInMillis(), startTime);
      // get attribute "SotfwareVersion"
      String softwareVersion =
          (String)mbs.getAttribute(mxbeanName, "SoftwareVersion");
      Assert.assertEquals(datanode.getSoftwareVersion(),softwareVersion);
      Assert.assertEquals(version, softwareVersion
          + ", r" + datanode.getRevision());
      // get attribute "RpcPort"
      String rpcPort = (String)mbs.getAttribute(mxbeanName, "RpcPort");
      Assert.assertEquals(datanode.getRpcPort(),rpcPort);
      // get attribute "HttpPort"
      String httpPort = (String)mbs.getAttribute(mxbeanName, "HttpPort");
      Assert.assertNotNull(httpPort);
      Assert.assertEquals(datanode.getHttpPort(),httpPort);
      // get attribute "NamenodeAddresses"
      String namenodeAddresses = (String)mbs.getAttribute(mxbeanName, 
          "NamenodeAddresses");
      Assert.assertEquals(datanode.getNamenodeAddresses(),namenodeAddresses);
      // get attribute "getDatanodeHostname"
      String datanodeHostname = (String)mbs.getAttribute(mxbeanName,
          "DatanodeHostname");
      Assert.assertEquals(datanode.getDatanodeHostname(),datanodeHostname);
      // get attribute "getVolumeInfo"
      String volumeInfo = (String)mbs.getAttribute(mxbeanName, "VolumeInfo");
      Assert.assertEquals(replaceDigits(datanode.getVolumeInfo()),
          replaceDigits(volumeInfo));
      // Ensure mxbean's XceiverCount is same as the DataNode's
      // live value.
      int xceiverCount = (Integer)mbs.getAttribute(mxbeanName,
          "XceiverCount");
      Assert.assertEquals(datanode.getXceiverCount(), xceiverCount);
      // Ensure mxbean's XmitsInProgress is same as the DataNode's
      // live value.
      int xmitsInProgress =
          (Integer) mbs.getAttribute(mxbeanName, "XmitsInProgress");
      Assert.assertEquals(datanode.getXmitsInProgress(), xmitsInProgress);
      String bpActorInfo = (String)mbs.getAttribute(mxbeanName,
          "BPServiceActorInfo");
      Assert.assertEquals(datanode.getBPServiceActorInfo(), bpActorInfo);
      String slowDisks = (String)mbs.getAttribute(mxbeanName, "SlowDisks");
      Assert.assertEquals(datanode.getSlowDisks(), slowDisks);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testDataNodeMXBeanSecurityEnabled() throws Exception {
    Configuration simpleConf = new Configuration();
    Configuration secureConf = createSecureConfig("authentication");

    // get attribute "SecurityEnabled" with simple configuration
    try (MiniDFSCluster cluster =
                 new MiniDFSCluster.Builder(simpleConf).build()) {
      List<DataNode> datanodes = cluster.getDataNodes();
      Assert.assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
              "Hadoop:service=DataNode,name=DataNodeInfo");

      boolean securityEnabled = (boolean) mbs.getAttribute(mxbeanName,
              "SecurityEnabled");
      Assert.assertFalse(securityEnabled);
      Assert.assertEquals(datanode.isSecurityEnabled(), securityEnabled);
    }

    // get attribute "SecurityEnabled" with secure configuration
    try (MiniDFSCluster cluster =
                 new MiniDFSCluster.Builder(secureConf).build()) {
      List<DataNode> datanodes = cluster.getDataNodes();
      Assert.assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
              "Hadoop:service=DataNode,name=DataNodeInfo");

      boolean securityEnabled = (boolean) mbs.getAttribute(mxbeanName,
              "SecurityEnabled");
      Assert.assertTrue(securityEnabled);
      Assert.assertEquals(datanode.isSecurityEnabled(), securityEnabled);
    }

    // setting back the authentication method
    UserGroupInformation.setConfiguration(simpleConf);
  }
  
  private static String replaceDigits(final String s) {
    return s.replaceAll("[0-9]+", "_DIGITS_");
  }

  @Test
  public void testDataNodeMXBeanBlockSize() throws Exception {
    Configuration conf = new Configuration();

    try(MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).build()) {
      DataNode dn = cluster.getDataNodes().get(0);
      for (int i = 0; i < 100; i++) {
        DFSTestUtil.writeFile(
            cluster.getFileSystem(),
            new Path("/foo" + String.valueOf(i) + ".txt"), "test content");
      }
      DataNodeTestUtils.triggerBlockReport(dn);
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=DataNode,name=DataNodeInfo");
      String bpActorInfo = (String)mbs.getAttribute(mxbeanName,
          "BPServiceActorInfo");
      Assert.assertEquals(dn.getBPServiceActorInfo(), bpActorInfo);
      LOG.info("bpActorInfo is " + bpActorInfo);
      TypeReference<ArrayList<Map<String, String>>> typeRef
          = new TypeReference<ArrayList<Map<String, String>>>() {};
      ArrayList<Map<String, String>> bpActorInfoList =
          new ObjectMapper().readValue(bpActorInfo, typeRef);
      int maxDataLength =
          Integer.valueOf(bpActorInfoList.get(0).get("maxDataLength"));
      int confMaxDataLength = dn.getConf().getInt(
          CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
          CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
      int maxBlockReportSize =
          Integer.valueOf(bpActorInfoList.get(0).get("maxBlockReportSize"));
      LOG.info("maxDataLength is " + maxDataLength);
      LOG.info("maxBlockReportSize is " + maxBlockReportSize);
      assertTrue("maxBlockReportSize should be greater than zero",
          maxBlockReportSize > 0);
      assertEquals("maxDataLength should be exactly "
          + "the same value of ipc.maximum.data.length",
          confMaxDataLength,
          maxDataLength);
    }
  }

  @Test
  public void testDataNodeMXBeanBlockCount() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      List<DataNode> datanodes = cluster.getDataNodes();
      assertEquals(datanodes.size(), 1);

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName =
              new ObjectName("Hadoop:service=DataNode,name=DataNodeInfo");
      FileSystem fs = cluster.getFileSystem();
      for (int i = 0; i < 5; i++) {
        DFSTestUtil.createFile(fs, new Path("/tmp.txt" + i), 1024, (short) 1,
                1L);
      }
      assertEquals("Before restart DN", 5, getTotalNumBlocks(mbs, mxbeanName));
      cluster.restartDataNode(0);
      cluster.waitActive();
      assertEquals("After restart DN", 5, getTotalNumBlocks(mbs, mxbeanName));
      fs.delete(new Path("/tmp.txt1"), true);
      // The total numBlocks should be updated after one file is deleted
      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          try {
            return getTotalNumBlocks(mbs, mxbeanName) == 4;
          } catch (Exception e) {
            e.printStackTrace();
            return false;
          }
        }
      }, 100, 30000);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private int getTotalNumBlocks(MBeanServer mbs, ObjectName mxbeanName)
          throws Exception {
    int totalBlocks = 0;
    String volumeInfo = (String) mbs.getAttribute(mxbeanName, "VolumeInfo");
    Map<?, ?> m = (Map<?, ?>) JSON.parse(volumeInfo);
    Collection<Map<String, Long>> values =
            (Collection<Map<String, Long>>) m.values();
    for (Map<String, Long> volumeInfoMap : values) {
      totalBlocks += volumeInfoMap.get("numBlocks");
    }
    return totalBlocks;
  }

  @Test
  public void testDataNodeMXBeanSlowDisksEnabled() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys
        .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY, 100);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      List<DataNode> datanodes = cluster.getDataNodes();
      Assert.assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);
      String slowDiskPath = "test/data1/slowVolume";
      datanode.getDiskMetrics().addSlowDiskForTesting(slowDiskPath, null);

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=DataNode,name=DataNodeInfo");

      String slowDisks = (String)mbs.getAttribute(mxbeanName, "SlowDisks");
      Assert.assertEquals(datanode.getSlowDisks(), slowDisks);
      Assert.assertTrue(slowDisks.contains(slowDiskPath));
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }

  @Test
  public void testDataNodeMXBeanLastHeartbeats() throws Exception {
    Configuration conf = new Configuration();
    try (MiniDFSCluster cluster = new MiniDFSCluster
        .Builder(conf)
        .nnTopology(MiniDFSNNTopology.simpleHATopology(2))
        .build()) {
      cluster.waitActive();
      cluster.transitionToActive(0);
      cluster.transitionToStandby(1);

      DataNode datanode = cluster.getDataNodes().get(0);

      // Verify and wait until one of the BP service actor identifies active namenode as active
      // and another as standby.
      cluster.waitDatanodeConnectedToActive(datanode, 5000);

      // Verify that last heartbeat sent to both namenodes in last 5 sec.
      assertLastHeartbeatSentTime(datanode, "LastHeartbeat");
      // Verify that last heartbeat response from both namenodes have been received within
      // last 5 sec.
      assertLastHeartbeatSentTime(datanode, "LastHeartbeatResponseTime");


      NameNode sbNameNode = cluster.getNameNode(1);

      // Stopping standby namenode
      sbNameNode.stop();

      // Verify that last heartbeat response time from one of the namenodes would stay much higher
      // after stopping one namenode.
      GenericTestUtils.waitFor(() -> {
        List<Map<String, String>> bpServiceActorInfo = datanode.getBPServiceActorInfoMap();
        Map<String, String> bpServiceActorInfo1 = bpServiceActorInfo.get(0);
        Map<String, String> bpServiceActorInfo2 = bpServiceActorInfo.get(1);

        long lastHeartbeatResponseTime1 =
            Long.parseLong(bpServiceActorInfo1.get("LastHeartbeatResponseTime"));
        long lastHeartbeatResponseTime2 =
            Long.parseLong(bpServiceActorInfo2.get("LastHeartbeatResponseTime"));

        LOG.info("Last heartbeat response from namenode 1: {}", lastHeartbeatResponseTime1);
        LOG.info("Last heartbeat response from namenode 2: {}", lastHeartbeatResponseTime2);

        return (lastHeartbeatResponseTime1 < 5L && lastHeartbeatResponseTime2 > 5L) || (
            lastHeartbeatResponseTime1 > 5L && lastHeartbeatResponseTime2 < 5L);

      }, 200, 15000,
          "Last heartbeat response should be higher than 5s for at least one namenode");

      // Verify that last heartbeat sent to both namenodes in last 5 sec even though
      // the last heartbeat received from one of the namenodes is greater than 5 sec ago.
      assertLastHeartbeatSentTime(datanode, "LastHeartbeat");
    }
  }

  private static void assertLastHeartbeatSentTime(DataNode datanode, String lastHeartbeat) {
    List<Map<String, String>> bpServiceActorInfo = datanode.getBPServiceActorInfoMap();
    Map<String, String> bpServiceActorInfo1 = bpServiceActorInfo.get(0);
    Map<String, String> bpServiceActorInfo2 = bpServiceActorInfo.get(1);

    long lastHeartbeatSent1 =
        Long.parseLong(bpServiceActorInfo1.get(lastHeartbeat));
    long lastHeartbeatSent2 =
        Long.parseLong(bpServiceActorInfo2.get(lastHeartbeat));

    Assert.assertTrue(lastHeartbeat + " for first bp service actor is higher than 5s",
        lastHeartbeatSent1 < 5L);
    Assert.assertTrue(lastHeartbeat + " for second bp service actor is higher than 5s",
        lastHeartbeatSent2 < 5L);
  }

}
