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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.sls.SLSRunner;

import com.codahale.metrics.Gauge;

@Private
@Unstable
public class FairSchedulerMetrics extends SchedulerMetrics {

  private int totalMemoryMB = Integer.MAX_VALUE;
  private int totalVCores = Integer.MAX_VALUE;
  private boolean maxReset = false;

  @VisibleForTesting
  public enum Metric {
    DEMAND("demand"),
    USAGE("usage"),
    MINSHARE("minshare"),
    MAXSHARE("maxshare"),
    FAIRSHARE("fairshare");

    private String value;

    Metric(String value) {
      this.value = value;
    }

    @VisibleForTesting
    public String getValue() {
      return value;
    }
  }

  public FairSchedulerMetrics() {
    super();

    for (Metric metric: Metric.values()) {
      appTrackedMetrics.add(metric.value + ".memory");
      appTrackedMetrics.add(metric.value + ".vcores");
      queueTrackedMetrics.add(metric.value + ".memory");
      queueTrackedMetrics.add(metric.value + ".vcores");
    }
  }

  private long getMemorySize(Schedulable schedulable, Metric metric) {
    if (schedulable != null) {
      switch (metric) {
      case DEMAND:
        return schedulable.getDemand().getMemorySize();
      case USAGE:
        return schedulable.getResourceUsage().getMemorySize();
      case MINSHARE:
        return schedulable.getMinShare().getMemorySize();
      case MAXSHARE:
        return schedulable.getMaxShare().getMemorySize();
      case FAIRSHARE:
        return schedulable.getFairShare().getMemorySize();
      default:
        return 0L;
      }
    }

    return 0L;
  }

  private int getVirtualCores(Schedulable schedulable, Metric metric) {
    if (schedulable != null) {
      switch (metric) {
      case DEMAND:
        return schedulable.getDemand().getVirtualCores();
      case USAGE:
        return schedulable.getResourceUsage().getVirtualCores();
      case MINSHARE:
        return schedulable.getMinShare().getVirtualCores();
      case MAXSHARE:
        return schedulable.getMaxShare().getVirtualCores();
      case FAIRSHARE:
        return schedulable.getFairShare().getVirtualCores();
      default:
        return 0;
      }
    }

    return 0;
  }

  private void registerAppMetrics(ApplicationId appId, String oldAppId,
      Metric metric) {
    metrics.register(
        "variable.app." + oldAppId + "." + metric.value + ".memory",
        new Gauge<Long>() {
          @Override
          public Long getValue() {
            return getMemorySize((FSAppAttempt)getSchedulerAppAttempt(appId),
                metric);
          }
        }
    );

    metrics.register(
        "variable.app." + oldAppId + "." + metric.value + ".vcores",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return getVirtualCores((FSAppAttempt)getSchedulerAppAttempt(appId),
                metric);
          }
        }
    );
  }

  @Override
  public void trackApp(ApplicationId appId, String oldAppId) {
    super.trackApp(appId, oldAppId);

    for (Metric metric: Metric.values()) {
      registerAppMetrics(appId, oldAppId, metric);
    }
  }

  private void registerQueueMetrics(FSQueue queue, Metric metric) {
    metrics.register(
        "variable.queue." + queue.getName() + "." + metric.value + ".memory",
        new Gauge<Long>() {
          @Override
          public Long getValue() {
            return getMemorySize(queue, metric);
          }
        }
    );
    metrics.register(
        "variable.queue." + queue.getName() + "." + metric.value + ".vcores",
        new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return getVirtualCores(queue, metric);
          }
        }
    );
  }

  @Override
  protected void registerQueueMetrics(String queueName) {
    super.registerQueueMetrics(queueName);

    FairScheduler fair = (FairScheduler) scheduler;
    final FSQueue queue = fair.getQueueManager().getQueue(queueName);
    registerQueueMetrics(queue, Metric.DEMAND);
    registerQueueMetrics(queue, Metric.USAGE);
    registerQueueMetrics(queue, Metric.MINSHARE);
    registerQueueMetrics(queue, Metric.FAIRSHARE);

    metrics.register("variable.queue." + queueName + ".maxshare.memory",
      new Gauge<Long>() {
        @Override
        public Long getValue() {
          if (! maxReset
              && SLSRunner.getSimulateInfoMap().containsKey("Number of nodes")
              && SLSRunner.getSimulateInfoMap().containsKey("Node memory (MB)")
              && SLSRunner.getSimulateInfoMap().containsKey("Node VCores")) {
            int numNMs = Integer.parseInt(SLSRunner.getSimulateInfoMap()
                .get("Number of nodes").toString());
            int numMemoryMB = Integer.parseInt(SLSRunner.getSimulateInfoMap()
                .get("Node memory (MB)").toString());
            int numVCores = Integer.parseInt(SLSRunner.getSimulateInfoMap()
                .get("Node VCores").toString());

            totalMemoryMB = numNMs * numMemoryMB;
            totalVCores = numNMs * numVCores;
            maxReset = false;
          }

          return Math.min(queue.getMaxShare().getMemorySize(), totalMemoryMB);
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".maxshare.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return Math.min(queue.getMaxShare().getVirtualCores(), totalVCores);
        }
      }
    );
  }
}
