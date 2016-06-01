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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair
    .FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;

import com.codahale.metrics.Gauge;
import org.apache.hadoop.yarn.sls.SLSRunner;

@Private
@Unstable
public class FairSchedulerMetrics extends SchedulerMetrics {

  private int totalMemoryMB = Integer.MAX_VALUE;
  private int totalVCores = Integer.MAX_VALUE;
  private boolean maxReset = false;

  public FairSchedulerMetrics() {
    super();
    appTrackedMetrics.add("demand.memory");
    appTrackedMetrics.add("demand.vcores");
    appTrackedMetrics.add("usage.memory");
    appTrackedMetrics.add("usage.vcores");
    appTrackedMetrics.add("minshare.memory");
    appTrackedMetrics.add("minshare.vcores");
    appTrackedMetrics.add("maxshare.memory");
    appTrackedMetrics.add("maxshare.vcores");
    appTrackedMetrics.add("fairshare.memory");
    appTrackedMetrics.add("fairshare.vcores");
    queueTrackedMetrics.add("demand.memory");
    queueTrackedMetrics.add("demand.vcores");
    queueTrackedMetrics.add("usage.memory");
    queueTrackedMetrics.add("usage.vcores");
    queueTrackedMetrics.add("minshare.memory");
    queueTrackedMetrics.add("minshare.vcores");
    queueTrackedMetrics.add("maxshare.memory");
    queueTrackedMetrics.add("maxshare.vcores");
    queueTrackedMetrics.add("fairshare.memory");
    queueTrackedMetrics.add("fairshare.vcores");
  }
  
  @Override
  public void trackApp(ApplicationAttemptId appAttemptId, String oldAppId) {
    super.trackApp(appAttemptId, oldAppId);
    FairScheduler fair = (FairScheduler) scheduler;
    final FSAppAttempt app = fair.getSchedulerApp(appAttemptId);
    metrics.register("variable.app." + oldAppId + ".demand.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return app.getDemand().getMemory();
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".demand.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return app.getDemand().getVirtualCores();
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".usage.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return app.getResourceUsage().getMemory();
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".usage.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return app.getResourceUsage().getVirtualCores();
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".minshare.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return app.getMinShare().getMemory();
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".minshare.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return app.getMinShare().getMemory();
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".maxshare.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return Math.min(app.getMaxShare().getMemory(), totalMemoryMB);
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".maxshare.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return Math.min(app.getMaxShare().getVirtualCores(), totalVCores);
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".fairshare.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return app.getFairShare().getVirtualCores();
        }
      }
    );
    metrics.register("variable.app." + oldAppId + ".fairshare.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return app.getFairShare().getVirtualCores();
        }
      }
    );
  }

  @Override
  public void trackQueue(String queueName) {
    trackedQueues.add(queueName);
    FairScheduler fair = (FairScheduler) scheduler;
    final FSQueue queue = fair.getQueueManager().getQueue(queueName);
    metrics.register("variable.queue." + queueName + ".demand.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return queue.getDemand().getMemory();
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".demand.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return queue.getDemand().getVirtualCores();
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".usage.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return queue.getResourceUsage().getMemory();
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".usage.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return queue.getResourceUsage().getVirtualCores();
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".minshare.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return queue.getMinShare().getMemory();
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".minshare.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return queue.getMinShare().getVirtualCores();
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".maxshare.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          if (! maxReset &&
                  SLSRunner.simulateInfoMap.containsKey("Number of nodes") &&
                  SLSRunner.simulateInfoMap.containsKey("Node memory (MB)") &&
                  SLSRunner.simulateInfoMap.containsKey("Node VCores")) {
            int numNMs = Integer.parseInt(
                  SLSRunner.simulateInfoMap.get("Number of nodes").toString());
            int numMemoryMB = Integer.parseInt(
                  SLSRunner.simulateInfoMap.get("Node memory (MB)").toString());
            int numVCores = Integer.parseInt(
                  SLSRunner.simulateInfoMap.get("Node VCores").toString());

            totalMemoryMB = numNMs * numMemoryMB;
            totalVCores = numNMs * numVCores;
            maxReset = false;
          }

          return Math.min(queue.getMaxShare().getMemory(), totalMemoryMB);
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
    metrics.register("variable.queue." + queueName + ".fairshare.memory",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return queue.getFairShare().getMemory();
        }
      }
    );
    metrics.register("variable.queue." + queueName + ".fairshare.vcores",
      new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return queue.getFairShare().getVirtualCores();
        }
      }
    );
  }

  @Override
  public void untrackQueue(String queueName) {
    trackedQueues.remove(queueName);
    metrics.remove("variable.queue." + queueName + ".demand.memory");
    metrics.remove("variable.queue." + queueName + ".demand.vcores");
    metrics.remove("variable.queue." + queueName + ".usage.memory");
    metrics.remove("variable.queue." + queueName + ".usage.vcores");
    metrics.remove("variable.queue." + queueName + ".minshare.memory");
    metrics.remove("variable.queue." + queueName + ".minshare.vcores");
    metrics.remove("variable.queue." + queueName + ".maxshare.memory");
    metrics.remove("variable.queue." + queueName + ".maxshare.vcores");
    metrics.remove("variable.queue." + queueName + ".fairshare.memory");
    metrics.remove("variable.queue." + queueName + ".fairshare.vcores");
  }
}
