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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.junit.Assert;

import java.util.Set;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueHelpers.setupQueueConfiguration;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class CapacitySchedulerTestUtilities {
  public static final int GB = 1024;

  private CapacitySchedulerTestUtilities() {
  }

  @SuppressWarnings("unchecked")
  public static <E> Set<E> toSet(E... elements) {
    return Sets.newHashSet(elements);
  }

  public static void checkPendingResource(MockRM rm, String queueName, int memory,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertEquals(
        memory,
        queue.getQueueResourceUsage()
            .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
            .getMemorySize());
  }


  public static void checkPendingResourceGreaterThanZero(MockRM rm, String queueName,
      String label) {
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();
    CSQueue queue = cs.getQueue(queueName);
    Assert.assertTrue(queue.getQueueResourceUsage()
        .getPending(label == null ? RMNodeLabelsManager.NO_LABEL : label)
        .getMemorySize() > 0);
  }

  public static void waitforNMRegistered(ResourceScheduler scheduler, int nodecount,
      int timesec) throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timesec * 1000) {
      if (scheduler.getNumClusterNodes() < nodecount) {
        Thread.sleep(100);
      } else {
        break;
      }
    }
  }

  public static ResourceManager createResourceManager() throws Exception {
    ResourceUtils.resetResourceTypes(new Configuration());
    DefaultMetricsSystem.setMiniClusterMode(true);
    ResourceManager resourceManager = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };
    CapacitySchedulerConfiguration csConf
        = new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        CapacityScheduler.class, ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    ((AsyncDispatcher) resourceManager.getRMContext().getDispatcher()).start();
    return resourceManager;
  }

  public static RMContext createMockRMContext() {
    RMContext mockContext = mock(RMContext.class);
    when(mockContext.getConfigurationProvider()).thenReturn(
        new LocalConfigurationProvider());
    return mockContext;
  }

  public static void stopResourceManager(ResourceManager resourceManager) throws Exception {
    if (resourceManager != null) {
      QueueMetrics.clearQueueMetrics();
      DefaultMetricsSystem.shutdown();
      resourceManager.stop();
    }
  }
}
