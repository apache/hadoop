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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static junit.framework.TestCase.assertNotNull;

/**
 * Metrics related RM HA testing. Metrics are mostly static singletons. To
 * avoid interference with other RM HA tests, separating metric tests for RM HA
 * into a separate file temporarily.
 */
public class TestRMHAMetrics {
  private Configuration configuration;

  private static final String RM1_ADDRESS = "1.1.1.1:1";
  private static final String RM1_NODE_ID = "rm1";

  private static final String RM2_ADDRESS = "0.0.0.0:0";
  private static final String RM2_NODE_ID = "rm2";

  @Before
  public void setUp() throws Exception {
    configuration = new Configuration();
    UserGroupInformation.setConfiguration(configuration);
    configuration.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
    configuration.set(YarnConfiguration.RM_HA_IDS, RM1_NODE_ID + ","
        + RM2_NODE_ID);
    for (String confKey : YarnConfiguration
        .getServiceAddressConfKeys(configuration)) {
      configuration.set(HAUtil.addSuffix(confKey, RM1_NODE_ID), RM1_ADDRESS);
      configuration.set(HAUtil.addSuffix(confKey, RM2_NODE_ID), RM2_ADDRESS);
    }

    ClusterMetrics.destroy();
    QueueMetrics.clearQueueMetrics();
    DefaultMetricsSystem.shutdown();
  }

  @Test(timeout = 300000)
  public void testMetricsAfterTransitionToStandby() throws Exception {
    configuration.setBoolean(YarnConfiguration.AUTO_FAILOVER_ENABLED, false);
    Configuration conf = new YarnConfiguration(configuration);
    MockRM rm = new MockRM(conf);
    rm.init(conf);

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName mxbeanName =
        new ObjectName("Hadoop:service=ResourceManager,name=RMInfo");

    Assert.assertEquals("initializing",
        (String) mbs.getAttribute(mxbeanName, "State"));

    rm.start();
    Assert.assertEquals("standby",
        (String) mbs.getAttribute(mxbeanName, "State"));

    rm.transitionToActive();
    Assert
        .assertEquals("active",
            (String) mbs.getAttribute(mxbeanName, "State"));

    rm.transitionToStandby(true);
    Assert.assertEquals("standby",
        (String) mbs.getAttribute(mxbeanName, "State"));

    assertNotNull(DefaultMetricsSystem.instance().getSource("JvmMetrics"));
    assertNotNull(DefaultMetricsSystem.instance().getSource("UgiMetrics"));

    rm.stop();
  }
}
