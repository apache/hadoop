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
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Supplier;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Class for testing {@link NameNodeStatusMXBean} implementation.
 */
public class TestNameNodeStatusMXBean {

  public static final Log LOG = LogFactory.getLog(
      TestNameNodeStatusMXBean.class);

  @Test(timeout = 120000L)
  public void testNameNodeStatusMXBean() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;

    try {
      cluster = new MiniDFSCluster.Builder(conf).build();
      cluster.waitActive();

      NameNode nn = cluster.getNameNode();

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeStatus");

      // Get attribute "NNRole"
      String nnRole = (String)mbs.getAttribute(mxbeanName, "NNRole");
      Assert.assertEquals(nn.getNNRole(), nnRole);

      // Get attribute "State"
      String state = (String)mbs.getAttribute(mxbeanName, "State");
      Assert.assertEquals(nn.getState(), state);

      // Get attribute "HostAndPort"
      String hostAndPort = (String)mbs.getAttribute(mxbeanName, "HostAndPort");
      Assert.assertEquals(nn.getHostAndPort(), hostAndPort);

      // Get attribute "SecurityEnabled"
      boolean securityEnabled = (boolean)mbs.getAttribute(mxbeanName,
          "SecurityEnabled");
      Assert.assertEquals(nn.isSecurityEnabled(), securityEnabled);

      // Get attribute "LastHATransitionTime"
      long lastHATransitionTime = (long)mbs.getAttribute(mxbeanName,
          "LastHATransitionTime");
      Assert.assertEquals(nn.getLastHATransitionTime(), lastHATransitionTime);

      // Get attribute "BytesWithFutureGenerationStamps"
      long bytesWithFutureGenerationStamps = (long)mbs.getAttribute(
          mxbeanName, "BytesWithFutureGenerationStamps");
      Assert.assertEquals(nn.getBytesWithFutureGenerationStamps(),
          bytesWithFutureGenerationStamps);

      // Get attribute "SlowPeersReport"
      String slowPeersReport = (String)mbs.getAttribute(mxbeanName,
          "SlowPeersReport");
      Assert.assertEquals(nn.getSlowPeersReport(), slowPeersReport);

      // Get attribute "SlowDisksReport"
      String slowDisksReport = (String)mbs.getAttribute(mxbeanName,
          "SlowDisksReport");
      Assert.assertEquals(nn.getSlowDisksReport(), slowDisksReport);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @Test
  public void testNameNodeMXBeanSlowDisksEnabled() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(
        DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
        100);
    conf.setTimeDuration(
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        1000, TimeUnit.MILLISECONDS);
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();

    try {
      List<DataNode> datanodes = cluster.getDataNodes();
      Assert.assertEquals(datanodes.size(), 1);
      DataNode datanode = datanodes.get(0);
      String slowDiskPath = "test/data1/slowVolume";
      datanode.getDiskMetrics().addSlowDiskForTesting(slowDiskPath, null);

      NameNode nn = cluster.getNameNode();
      DatanodeManager datanodeManager = nn.getNamesystem().getBlockManager()
          .getDatanodeManager();

      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName mxbeanName = new ObjectName(
          "Hadoop:service=NameNode,name=NameNodeStatus");

      GenericTestUtils.waitFor(new Supplier<Boolean>() {
        @Override
        public Boolean get() {
          return (datanodeManager.getSlowDisksReport() != null);
        }
      }, 1000, 100000);

      String slowDisksReport = (String)mbs.getAttribute(
          mxbeanName, "SlowDisksReport");
      Assert.assertEquals(datanodeManager.getSlowDisksReport(),
          slowDisksReport);
      Assert.assertTrue(slowDisksReport.contains(slowDiskPath));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
