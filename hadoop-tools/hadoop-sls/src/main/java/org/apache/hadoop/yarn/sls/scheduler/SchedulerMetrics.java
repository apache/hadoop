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

package org.apache.hadoop.yarn.sls.scheduler;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
        .ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
        .SchedulerAppReport;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

@Private
@Unstable
public abstract class SchedulerMetrics {
  protected ResourceScheduler scheduler;
  protected Set<String> trackedQueues;
  protected MetricRegistry metrics;
  protected Set<String> appTrackedMetrics;
  protected Set<String> queueTrackedMetrics;
  
  public SchedulerMetrics() {
    appTrackedMetrics = new HashSet<String>();
    appTrackedMetrics.add("live.containers");
    appTrackedMetrics.add("reserved.containers");
    queueTrackedMetrics = new HashSet<String>();
  }
  
  public void init(ResourceScheduler scheduler, MetricRegistry metrics) {
    this.scheduler = scheduler;
    this.trackedQueues = new HashSet<String>();
    this.metrics = metrics;
  }
  
  public void trackApp(final ApplicationAttemptId appAttemptId,
                       String oldAppId) {
    metrics.register("variable.app." + oldAppId + ".live.containers",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          SchedulerAppReport app = scheduler.getSchedulerAppInfo(appAttemptId);
          return app.getLiveContainers().size();
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".reserved.containers",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          SchedulerAppReport app = scheduler.getSchedulerAppInfo(appAttemptId);
          return app.getReservedContainers().size();
        }
      }
    );
  }
  
  public void untrackApp(ApplicationAttemptId appAttemptId,
      String oldAppId) {
    for (String m : appTrackedMetrics) {
      metrics.remove("variable.app." + oldAppId + "." + m);
    }
  }

  public abstract void trackQueue(String queueName);
  
  public void untrackQueue(String queueName) {
    for (String m : queueTrackedMetrics) {
      metrics.remove("variable.queue." + queueName + "." + m);
    }
  }
  
  public boolean isTracked(String queueName) {
    return trackedQueues.contains(queueName);
  }
  
  public Set<String> getAppTrackedMetrics() {
    return appTrackedMetrics;
  }
  public Set<String> getQueueTrackedMetrics() {
    return queueTrackedMetrics;
  }
}
