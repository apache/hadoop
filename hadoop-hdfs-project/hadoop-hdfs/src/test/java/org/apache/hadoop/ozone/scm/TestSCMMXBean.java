/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.scm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.scm.node.NodeManager;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

/**
 *
 * This class is to test JMX management interface for scm information.
 */
public class TestSCMMXBean {

  public static final Log LOG = LogFactory.getLog(TestSCMMXBean.class);
  private static int numOfDatanodes = 1;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static MBeanServer mbs;

  @BeforeClass
  public static void init() throws IOException, TimeoutException,
      InterruptedException {
    conf = new OzoneConfiguration();
    cluster = new MiniOzoneCluster.Builder(conf)
        .numDataNodes(numOfDatanodes)
        .setHandlerType(OzoneConsts.OZONE_HANDLER_DISTRIBUTED)
        .build();
    cluster.waitOzoneReady();
    scm = cluster.getStorageContainerManager();
    mbs = ManagementFactory.getPlatformMBeanServer();
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.cleanupWithLogger(null, cluster);
  }

  @Test
  public void testSCMMXBean() throws Exception {
    ObjectName bean = new ObjectName(
        "Hadoop:service=StorageContainerManager,"
            + "name=StorageContainerManagerInfo,"
            + "component=ServerRuntime");

    String dnRpcPort = (String)mbs.getAttribute(bean,
        "DatanodeRpcPort");
    assertEquals(scm.getDatanodeRpcPort(), dnRpcPort);


    String clientRpcPort = (String)mbs.getAttribute(bean,
        "ClientRpcPort");
    assertEquals(scm.getClientRpcPort(), clientRpcPort);

    TabularData nodeCountObj = (TabularData)mbs.getAttribute(bean,
        "NodeCount");
    verifyEquals(nodeCountObj, scm.getNodeCount());
  }

  @Test
  public void testSCMNodeManagerMXBean() throws Exception {
    final NodeManager scmNm = scm.getScmNodeManager();
    ObjectName bean = new ObjectName(
        "Hadoop:service=SCMNodeManager,name=SCMNodeManagerInfo");

    Integer minChillNodes = (Integer)mbs.getAttribute(bean,
        "MinimumChillModeNodes");
    assertEquals(scmNm.getMinimumChillModeNodes(),
        minChillNodes.intValue());

    boolean isOutOfChillMode = (boolean)mbs.getAttribute(bean,
        "OutOfChillMode");
    assertEquals(scmNm.isOutOfChillMode(), isOutOfChillMode);

    String chillStatus = (String)mbs.getAttribute(bean,
        "ChillModeStatus");
    assertEquals(scmNm.getChillModeStatus(), chillStatus);

    TabularData nodeCountObj = (TabularData)mbs.getAttribute(bean,
        "NodeCount");
    verifyEquals(nodeCountObj, scm.getScmNodeManager().getNodeCount());
  }

  /**
   * An internal function used to compare a TabularData returned
   * by JMX with the expected data in a Map.
   */
  private void verifyEquals(TabularData data1,
      Map<String, Integer> data2) {
    if (data1 == null || data2 == null) {
      fail("Data should not be null.");
    }
    for (Object obj : data1.values()) {
      // Each TabularData is a set of CompositeData
      assertTrue(obj instanceof CompositeData);
      CompositeData cds = (CompositeData) obj;
      assertEquals(2, cds.values().size());
      Iterator<?> it = cds.values().iterator();
      String key = it.next().toString();
      String value = it.next().toString();
      int num = Integer.parseInt(value);
      assertTrue(data2.containsKey(key));
      assertEquals(data2.get(key).intValue(), num);
    }
  }
}
