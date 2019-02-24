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

package org.apache.hadoop.ozone.scm.pipeline;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test cases to verify the metrics exposed by SCMPipelineManager via MXBean.
 */
public class TestPipelineManagerMXBean {

  private MiniOzoneCluster cluster;
  private static MBeanServer mbs;

  @Before
  public void init()
      throws IOException, TimeoutException, InterruptedException {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    mbs = ManagementFactory.getPlatformMBeanServer();
  }

  /**
   * Verifies SCMPipelineManagerInfo metrics.
   *
   * @throws Exception
   */
  @Test
  public void testPipelineInfo() throws Exception {
    ObjectName bean = new ObjectName(
        "Hadoop:service=SCMPipelineManager,name=SCMPipelineManagerInfo");

    TabularData data = (TabularData) mbs.getAttribute(bean, "PipelineInfo");
    Map<String, Integer> datanodeInfo = cluster.getStorageContainerManager()
        .getPipelineManager().getPipelineInfo();
    verifyEquals(data, datanodeInfo);
  }

  private void verifyEquals(TabularData actualData, Map<String, Integer>
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

  @After
  public void teardown() {
    cluster.shutdown();
  }
}
