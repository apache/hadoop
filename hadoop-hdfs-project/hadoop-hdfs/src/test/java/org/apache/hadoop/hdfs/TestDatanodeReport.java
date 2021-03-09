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

import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeAdminProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.CombinedHostFileManager;
import org.apache.hadoop.hdfs.server.blockmanagement.HostConfigManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.util.HostsFileWriter;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test ensures the all types of data node report work correctly.
 */
public class TestDatanodeReport {
  static final Logger LOG = LoggerFactory.getLogger(TestDatanodeReport.class);
  final static private Configuration conf = new HdfsConfiguration();
  final static private int NUM_OF_DATANODES = 4;

  /**
   * This test verifies upgrade domain is set according to the JSON host file.
   */
  @Test
  public void testDatanodeReportWithUpgradeDomain() throws Exception {
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500); // 0.5s
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setClass(DFSConfigKeys.DFS_NAMENODE_HOSTS_PROVIDER_CLASSNAME_KEY,
        CombinedHostFileManager.class, HostConfigManager.class);
    HostsFileWriter hostsFileWriter = new HostsFileWriter();
    hostsFileWriter.initialize(conf, "temp/datanodeReport");

    MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    final DFSClient client = cluster.getFileSystem().dfs;
    final String ud1 = "ud1";
    final String ud2 = "ud2";

    try {
      //wait until the cluster is up
      cluster.waitActive();

      DatanodeAdminProperties datanode = new DatanodeAdminProperties();
      datanode.setHostName(cluster.getDataNodes().get(0).getDatanodeId().getHostName());
      datanode.setUpgradeDomain(ud1);
      hostsFileWriter.initIncludeHosts(
          new DatanodeAdminProperties[]{datanode});
      client.refreshNodes();
      DatanodeInfo[] all = client.datanodeReport(DatanodeReportType.ALL);
      assertEquals(all[0].getUpgradeDomain(), ud1);

      datanode.setUpgradeDomain(null);
      hostsFileWriter.initIncludeHosts(
          new DatanodeAdminProperties[]{datanode});
      client.refreshNodes();
      all = client.datanodeReport(DatanodeReportType.ALL);
      assertEquals(all[0].getUpgradeDomain(), null);

      datanode.setUpgradeDomain(ud2);
      hostsFileWriter.initIncludeHosts(
          new DatanodeAdminProperties[]{datanode});
      client.refreshNodes();
      all = client.datanodeReport(DatanodeReportType.ALL);
      assertEquals(all[0].getUpgradeDomain(), ud2);
    } finally {
      cluster.shutdown();
    }
  }

  /**
   * This test attempts to different types of datanode report.
   */
  @Test
  public void testDatanodeReport() throws Exception {
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 500); // 0.5s
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    MiniDFSCluster cluster = 
      new MiniDFSCluster.Builder(conf).numDataNodes(NUM_OF_DATANODES).build();
    try {
      //wait until the cluster is up
      cluster.waitActive();
      final String bpid = cluster.getNamesystem().getBlockPoolId();
      final List<DataNode> datanodes = cluster.getDataNodes();
      final DFSClient client = cluster.getFileSystem().dfs;

      assertReports(NUM_OF_DATANODES, DatanodeReportType.ALL, client, datanodes, bpid);
      assertReports(NUM_OF_DATANODES, DatanodeReportType.LIVE, client, datanodes, bpid);
      assertReports(0, DatanodeReportType.DEAD, client, datanodes, bpid);

      // bring down one datanode
      final DataNode last = datanodes.get(datanodes.size() - 1);
      LOG.info("XXX shutdown datanode " + last.getDatanodeUuid());
      last.shutdown();

      DatanodeInfo[] nodeInfo = client.datanodeReport(DatanodeReportType.DEAD);
      while (nodeInfo.length != 1) {
        try {
          Thread.sleep(500);
        } catch (Exception e) {
        }
        nodeInfo = client.datanodeReport(DatanodeReportType.DEAD);
      }

      assertReports(NUM_OF_DATANODES, DatanodeReportType.ALL, client, datanodes, null);
      assertReports(NUM_OF_DATANODES - 1, DatanodeReportType.LIVE, client, datanodes, null);
      assertReports(1, DatanodeReportType.DEAD, client, datanodes, null);

      Thread.sleep(5000);
      assertCounter("ExpiredHeartbeats", 1, getMetrics("FSNamesystem"));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testDatanodeReportMissingBlock() throws Exception {
    conf.setLong(DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY, 1L);
    conf.setLong(HdfsClientConfigKeys.Retry.WINDOW_BASE_KEY, 1);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_OF_DATANODES).build();
    try {
      // wait until the cluster is up
      cluster.waitActive();
      DistributedFileSystem fs = cluster.getFileSystem();
      Path p = new Path("/testDatanodeReportMissingBlock");
      DFSTestUtil.writeFile(fs, p, new String("testdata"));
      LocatedBlock lb = fs.getClient().getLocatedBlocks(p.toString(), 0).get(0);
      assertEquals(3, lb.getLocations().length);
      ExtendedBlock b = lb.getBlock();
      cluster.corruptBlockOnDataNodesByDeletingBlockFile(b);
      try {
        DFSTestUtil.readFile(fs, p);
        Assert.fail("Must throw exception as the block doesn't exists on disk");
      } catch (IOException e) {
        // all bad datanodes
      }
      cluster.triggerHeartbeats(); // IBR delete ack
      lb = fs.getClient().getLocatedBlocks(p.toString(), 0).get(0);
      assertEquals(0, lb.getLocations().length);
    } finally {
      cluster.shutdown();
    }
  }

  final static Comparator<StorageReport> CMP = new Comparator<StorageReport>() {
    @Override
    public int compare(StorageReport left, StorageReport right) {
      return left.getStorage().getStorageID().compareTo(
            right.getStorage().getStorageID());
    }
  };

  static void assertReports(int numDatanodes, DatanodeReportType type,
      DFSClient client, List<DataNode> datanodes, String bpid) throws IOException {
    final DatanodeInfo[] infos = client.datanodeReport(type);
    assertEquals(numDatanodes, infos.length);
    final DatanodeStorageReport[] reports = client.getDatanodeStorageReport(type);
    assertEquals(numDatanodes, reports.length);
    
    for(int i = 0; i < infos.length; i++) {
      assertEquals(infos[i], reports[i].getDatanodeInfo());
      
      final DataNode d = findDatanode(infos[i].getDatanodeUuid(), datanodes);
      if (bpid != null) {
        //check storage
        final StorageReport[] computed = reports[i].getStorageReports();
        Arrays.sort(computed, CMP);
        final StorageReport[] expected = d.getFSDataset().getStorageReports(bpid);
        Arrays.sort(expected, CMP);
  
        assertEquals(expected.length, computed.length);
        for(int j = 0; j < expected.length; j++) {
          assertEquals(expected[j].getStorage().getStorageID(),
                       computed[j].getStorage().getStorageID());
        }
      }
    }
  }
  
  static DataNode findDatanode(String id, List<DataNode> datanodes) {
    for(DataNode d : datanodes) {
      if (d.getDatanodeUuid().equals(id)) {
        return d;
      }
    }
    throw new IllegalStateException("Datnode " + id + " not in datanode list: "
        + datanodes);
  }
}