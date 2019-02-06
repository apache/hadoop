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

package org.apache.hadoop.ozone.scm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Class which tests the SCMNodeManagerInfo Bean.
 */
public class TestSCMNodeManagerMXBean {
  public static final Log LOG = LogFactory.getLog(TestSCMMXBean.class);
  private static int numOfDatanodes = 3;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static MBeanServer mbs;

  @BeforeClass
  public static void init() throws IOException, TimeoutException,
      InterruptedException {
    conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_STALENODE_INTERVAL, "60000ms");
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    mbs = ManagementFactory.getPlatformMBeanServer();
  }

  @Test
  public void testDiskUsage() throws Exception {
    ObjectName bean = new ObjectName(
        "Hadoop:service=SCMNodeManager,"
            + "name=SCMNodeManagerInfo");

    TabularData data = (TabularData) mbs.getAttribute(bean, "NodeInfo");
    Map<String, Long> datanodeInfo = scm.getScmNodeManager().getNodeInfo();
    verifyEquals(data, datanodeInfo);
  }

  @Test
  public void testNodeCount() throws Exception {
    ObjectName bean = new ObjectName(
        "Hadoop:service=SCMNodeManager,"
            + "name=SCMNodeManagerInfo");

    TabularData data = (TabularData) mbs.getAttribute(bean, "NodeCount");
    Map<String, Integer> nodeCount = scm.getScmNodeManager().getNodeCount();
    Map<String, Long> nodeCountLong = new HashMap<>();
    nodeCount.forEach((k, v) -> nodeCountLong.put(k, new Long(v)));
    verifyEquals(data, nodeCountLong);
  }

  private void verifyEquals(TabularData actualData, Map<String, Long>
      expectedData) {
    if (actualData == null || expectedData == null) {
      fail("Data should not be null.");
    }
    for (Object obj : actualData.values()) {
      assertTrue(obj instanceof CompositeData);
      CompositeData cds = (CompositeData) obj;
      assertEquals(2, cds.values().size());
      Iterator<?> it = cds.values().iterator();
      String key = it.next().toString();
      String value = it.next().toString();
      long num = Long.parseLong(value);
      assertTrue(expectedData.containsKey(key));
      assertEquals(expectedData.remove(key).longValue(), num);
    }
    assertTrue(expectedData.isEmpty());
  }

}
