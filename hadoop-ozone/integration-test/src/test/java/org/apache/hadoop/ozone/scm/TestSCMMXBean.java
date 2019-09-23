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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.placement.metrics.ContainerStat;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    mbs = ManagementFactory.getPlatformMBeanServer();
  }

  @AfterClass
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
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

    ConcurrentMap<String, ContainerStat> map = scm.getContainerReportCache();
    ContainerStat stat = new ContainerStat(1, 2, 3, 4, 5, 6, 7);
    map.put("nodeID", stat);
    TabularData data = (TabularData) mbs.getAttribute(
        bean, "ContainerReport");

    // verify report info
    assertEquals(1, data.values().size());
    for (Object obj : data.values()) {
      assertTrue(obj instanceof CompositeData);
      CompositeData d = (CompositeData) obj;
      Iterator<?> it = d.values().iterator();
      String key = it.next().toString();
      String value = it.next().toString();
      assertEquals("nodeID", key);
      assertEquals(stat.toJsonString(), value);
    }

    boolean inSafeMode = (boolean) mbs.getAttribute(bean,
        "InSafeMode");
    assertEquals(scm.isInSafeMode(), inSafeMode);

    double containerThreshold = (double) mbs.getAttribute(bean,
        "SafeModeCurrentContainerThreshold");
    assertEquals(scm.getCurrentContainerThreshold(), containerThreshold, 0);
  }

  @Test
  public void testSCMContainerStateCount() throws Exception {

    ObjectName bean = new ObjectName(
        "Hadoop:service=StorageContainerManager,"
            + "name=StorageContainerManagerInfo,"
            + "component=ServerRuntime");
    TabularData data = (TabularData) mbs.getAttribute(
        bean, "ContainerStateCount");
    Map<String, Integer> containerStateCount = scm.getContainerStateCount();
    verifyEquals(data, containerStateCount);

    // Do some changes like allocate containers and change the container states
    ContainerManager scmContainerManager = scm.getContainerManager();

    List<ContainerInfo> containerInfoList = new ArrayList<>();
    for (int i=0; i < 10; i++) {
      containerInfoList.add(scmContainerManager.allocateContainer(HddsProtos
          .ReplicationType.STAND_ALONE, HddsProtos.ReplicationFactor.ONE,
          UUID.randomUUID().toString()));
    }
    long containerID;
    for (int i=0; i < 10; i++) {
      if (i % 2 == 0) {
        containerID = containerInfoList.get(i).getContainerID();
        scmContainerManager.updateContainerState(
            new ContainerID(containerID), HddsProtos.LifeCycleEvent.FINALIZE);
        assertEquals(scmContainerManager.getContainer(new ContainerID(
            containerID)).getState(), HddsProtos.LifeCycleState.CLOSING);
      } else {
        containerID = containerInfoList.get(i).getContainerID();
        scmContainerManager.updateContainerState(
            new ContainerID(containerID), HddsProtos.LifeCycleEvent.FINALIZE);
        scmContainerManager.updateContainerState(
            new ContainerID(containerID), HddsProtos.LifeCycleEvent.CLOSE);
        assertEquals(scmContainerManager.getContainer(new ContainerID(
            containerID)).getState(), HddsProtos.LifeCycleState.CLOSED);
      }

    }

    data = (TabularData) mbs.getAttribute(
        bean, "ContainerStateCount");
    containerStateCount = scm.getContainerStateCount();

    containerStateCount.forEach((k, v) -> {
      if(k == HddsProtos.LifeCycleState.CLOSING.toString()) {
        assertEquals((int)v, 5);
      } else if (k == HddsProtos.LifeCycleState.CLOSED.toString()) {
        assertEquals((int)v, 5);
      } else  {
        // Remaining all container state count should be zero.
        assertEquals((int)v, 0);
      }
    });

    verifyEquals(data, containerStateCount);

  }


  /**
   * An internal function used to compare a TabularData returned
   * by JMX with the expected data in a Map.
   */
  private void verifyEquals(TabularData actualData,
      Map<String, Integer> expectedData) {
    if (actualData == null || expectedData == null) {
      fail("Data should not be null.");
    }
    for (Object obj : actualData.values()) {
      // Each TabularData is a set of CompositeData
      assertTrue(obj instanceof CompositeData);
      CompositeData cds = (CompositeData) obj;
      assertEquals(2, cds.values().size());
      Iterator<?> it = cds.values().iterator();
      String key = it.next().toString();
      String value = it.next().toString();
      int num = Integer.parseInt(value);
      assertTrue(expectedData.containsKey(key));
      assertEquals(expectedData.remove(key).intValue(), num);
    }
    assertTrue(expectedData.isEmpty());
  }
}
