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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import static org.junit.Assert.assertEquals;

/**
 * Class for testing {@link DataNodeMXBean} implementation
 */
public class TestDataNodeMXBean {
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
      Assert.assertEquals(datanode.getHttpPort(),httpPort);
      // get attribute "NamenodeAddresses"
      String namenodeAddresses = (String)mbs.getAttribute(mxbeanName, 
          "NamenodeAddresses");
      Assert.assertEquals(datanode.getNamenodeAddresses(),namenodeAddresses);
      // get attribute "getVolumeInfo"
      String volumeInfo = (String)mbs.getAttribute(mxbeanName, "VolumeInfo");
      Assert.assertEquals(replaceDigits(datanode.getVolumeInfo()),
          replaceDigits(volumeInfo));
      // Ensure mxbean's XceiverCount is same as the DataNode's
      // live value.
      int xceiverCount = (Integer)mbs.getAttribute(mxbeanName,
          "XceiverCount");
      Assert.assertEquals(datanode.getXceiverCount(), xceiverCount);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  private static String replaceDigits(final String s) {
    return s.replaceAll("[0-9]+", "_DIGITS_");
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
      // Wait till replica gets deleted on disk.
      Thread.sleep(5000);
      assertEquals("After delete one file", 4,
              getTotalNumBlocks(mbs, mxbeanName));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  @SuppressWarnings("unchecked")
  int getTotalNumBlocks(MBeanServer mbs, ObjectName mxbeanName)
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
}
