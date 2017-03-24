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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.TestDataNodeMXBean;
import org.junit.Assert;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Class for testing {@link NameNodeStatusMXBean} implementation.
 */
public class TestNameNodeStatusMXBean {

  public static final Log LOG = LogFactory.getLog(
      TestNameNodeStatusMXBean.class);

  @Test(timeout = 120000L)
  public void testDataNodeMXBean() throws Exception {
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
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
