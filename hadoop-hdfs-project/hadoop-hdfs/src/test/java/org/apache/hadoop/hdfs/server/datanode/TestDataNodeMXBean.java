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
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Assert;
import org.junit.Test;

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
      // Ensure mxbean's XmitsInProgress is same as the DataNode's
      // live value.
      int xmitsInProgress =
          (Integer) mbs.getAttribute(mxbeanName, "XmitsInProgress");
      Assert.assertEquals(datanode.getXmitsInProgress(), xmitsInProgress);
    } finally {
      if (cluster != null) {cluster.shutdown();}
    }
  }
  
  private static String replaceDigits(final String s) {
    return s.replaceAll("[0-9]+", "_DIGITS_");
  }
}
