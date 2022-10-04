/*
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
package org.apache.hadoop.yarn.server.resourcemanager.federation;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import static org.apache.hadoop.metrics2.lib.Interns.info;

@Metrics(about = "Metrics for FederationStateStoreClient", context = "federation")
public final class FederationStateStoreClientMetrics {

  private static final MetricsInfo RECORD_INFO =
      info("FederationStateStoreClientMetrics", "Metrics for the RM FederationStateStore Client");

  private static volatile FederationStateStoreClientMetrics instance = null;
  private MetricsRegistry registry;

  @Metric("# of failed getCurrentVersion count")
  private MutableGaugeLong failedGetCurrentVersionCount;
  @Metric("# of failed loadVersion count")
  private MutableGaugeLong failedLoadVersionCount;
  @Metric("# of failed getPolicyConfiguration count")
  private MutableGaugeLong failedGetPolicyConfigurationCount;
  @Metric("# of failed setPolicyConfiguration count")
  private MutableGaugeLong failedSetPolicyConfigurationCount;
  @Metric("# of failed getPoliciesConfigurations count")
  private MutableGaugeLong failedGetPoliciesConfigurationsCount;
  @Metric("# of failed registerSubCluster count")
  private MutableGaugeLong failedRegisterSubClusterCount;
  @Metric("# of failed deregisterSubCluster count")
  private MutableGaugeLong failedDeregisterSubClusterCount;
  @Metric("# of failed subClusterHeartbeat count")
  private MutableGaugeLong failedSubClusterHeartbeatCount;
  @Metric("# of failed getSubCluster count")
  private MutableGaugeLong failedGetSubClusterCount;
  @Metric("# of failed getSubClusters count")
  private MutableGaugeLong failedGetSubClustersCount;
  @Metric("# of failed addApplicationHomeSubCluster count")
  private MutableGaugeLong failedAddApplicationHomeSubClusterCount;
  @Metric("# of failed updateApplicationHomeSubCluster count")
  private MutableGaugeLong failedUpdateApplicationHomeSubClusterCount;
  @Metric("# of failed getApplicationHomeSubCluster count")
  private MutableGaugeLong failedGetApplicationHomeSubClusterCount;


  /**
   * Initialize the singleton instance.
   *
   * @return the singleton
   */
  public static FederationStateStoreClientMetrics getMetrics() {
    synchronized (FederationStateStoreClientMetrics.class) {
      if (instance == null) {
        instance = DefaultMetricsSystem.instance().register("FederationStateStoreClientMetrics",
            "Metrics for the RM FederationStateStore Client",
            new FederationStateStoreClientMetrics());
      }
    }
    return instance;
  }

  private FederationStateStoreClientMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "RM FederationStateStore Client");
  }

  public void incrFailedGetCurrentVersionCount() {
    failedGetCurrentVersionCount.incr();
  }

  public void incrFailedLoadVersionCount() {
    failedLoadVersionCount.incr();
  }

  public void incrFailedGetPolicyConfigurationCount() {
    failedGetPolicyConfigurationCount.incr();
  }

  public void incrFailedSetPolicyConfigurationCount() {
    failedSetPolicyConfigurationCount.incr();
  }

  public void incrFailedGetPoliciesConfigurationsCount() {
    failedGetPoliciesConfigurationsCount.incr();
  }

  public void incrFailedRegisterSubClusterCount() {
    failedRegisterSubClusterCount.incr();
  }

  public void incrFailedDeregisterSubClusterCount() {
    failedDeregisterSubClusterCount.incr();
  }

  public void incrFailedSubClusterHeartbeatCount() {
    failedSubClusterHeartbeatCount.incr();
  }

  public void incrFailedGetSubClusterCount() {
    failedGetSubClusterCount.incr();
  }

  public void incrFailedGetSubClustersCount() {
    failedGetSubClustersCount.incr();
  }

  public void incrFailedAddApplicationHomeSubClusterCount() {
    failedAddApplicationHomeSubClusterCount.incr();
  }

  public void incrFailedUpdateApplicationHomeSubClusterCount() {
    failedUpdateApplicationHomeSubClusterCount.incr();
  }

  public void incrFailedGetApplicationHomeSubClusterCount() {
    failedGetApplicationHomeSubClusterCount.incr();
  }

  public long getFailedGetCurrentVersionCount() {
    return failedGetCurrentVersionCount.value();
  }
}
