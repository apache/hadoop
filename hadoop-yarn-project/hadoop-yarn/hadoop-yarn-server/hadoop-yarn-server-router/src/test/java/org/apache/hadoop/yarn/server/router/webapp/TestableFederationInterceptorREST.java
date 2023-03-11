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

package org.apache.hadoop.yarn.server.router.webapp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.router.webapp.BaseRouterWebServicesTest.QUEUE_DEDICATED_FULL;
import static org.apache.hadoop.yarn.server.router.webapp.BaseRouterWebServicesTest.QUEUE_DEFAULT_FULL;
import static org.apache.hadoop.yarn.server.router.webapp.BaseRouterWebServicesTest.QUEUE_DEFAULT;
import static org.apache.hadoop.yarn.server.router.webapp.BaseRouterWebServicesTest.QUEUE_DEDICATED;

/**
 * Extends the FederationInterceptorREST and overrides methods to provide a
 * testable implementation of FederationInterceptorREST.
 */
public class TestableFederationInterceptorREST
    extends FederationInterceptorREST {

  private List<SubClusterId> badSubCluster = new ArrayList<>();
  private MockRM mockRM = null;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestableFederationInterceptorREST.class);

  /**
   * For testing purpose, some subclusters has to be down to simulate particular
   * scenarios as RM Failover, network issues. For this reason we keep track of
   * these bad subclusters. This method make the subcluster unusable.
   *
   * @param badSC the subcluster to make unusable
   */
  protected void registerBadSubCluster(SubClusterId badSC) {

    // Adding in the cache the bad SubCluster, in this way we can stop them
    getOrCreateInterceptorForSubCluster(badSC, "1.2.3.4:4");

    badSubCluster.add(badSC);
    MockDefaultRequestInterceptorREST interceptor =
        (MockDefaultRequestInterceptorREST) super.getInterceptorForSubCluster(
            badSC);
    interceptor.setRunning(false);
  }

  protected void setupResourceManager() throws IOException {

    if (mockRM != null) {
      return;
    }

    try {

      DefaultMetricsSystem.setMiniClusterMode(true);
      CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

      // Define default queue
      conf.setCapacity(QUEUE_DEFAULT_FULL, 20);
      // Define dedicated queues
      String[] queues = new String[]{QUEUE_DEFAULT, QUEUE_DEDICATED};
      conf.setQueues(CapacitySchedulerConfiguration.ROOT, queues);
      conf.setCapacity(QUEUE_DEDICATED_FULL, 80);
      conf.setReservable(QUEUE_DEDICATED_FULL, true);

      conf.setClass(YarnConfiguration.RM_SCHEDULER,
          CapacityScheduler.class, ResourceScheduler.class);
      conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
      conf.setBoolean(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED, false);

      mockRM = new MockRM(conf);
      mockRM.start();
      mockRM.registerNode("127.0.0.1:5678", 100*1024, 100);

      Map<SubClusterId, DefaultRequestInterceptorREST> interceptors = super.getInterceptors();
      for (DefaultRequestInterceptorREST item : interceptors.values()) {
        MockDefaultRequestInterceptorREST interceptor = (MockDefaultRequestInterceptorREST) item;
        interceptor.setMockRM(mockRM);
      }
    } catch (Exception e) {
      LOG.error("setupResourceManager failed.", e);
      throw new IOException(e);
    }
  }

  @Override
  public void shutdown() {
    if (mockRM != null) {
      mockRM.stop();
      mockRM = null;
    }
    super.shutdown();
  }

  public MockRM getMockRM() {
    return mockRM;
  }
}