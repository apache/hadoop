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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopScheduledThreadPoolExecutor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This computes a snapshot of aggregated actual resource utilization across
 * Applications, Users and Queues. Queue aggregation will be performed
 * only at the LeafQueue level.
 * The snapshot calculation interval is set to the Node heartbeat interval.
 * It is assumed that all nodes would have heartbeat-ed to the RM in that
 * interval.
 */
public class ResourceUtilizationAggregator extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(ResourceUtilizationAggregator.class);

  private static final Function<Object, ResourceUtilization> RU_GENERATOR =
      (x -> ResourceUtilization.newZero());

  private final RMContext rmContext;
  private final ScheduledExecutorService scheduledExecutor;
  private volatile Map<ApplicationId, ResourceUtilization>
      stalePerAppUtilization = new HashMap<>();
  private volatile Map<String, ResourceUtilization> stalePerUserUtilization =
      new HashMap<>();
  private volatile Map<Queue, ResourceUtilization> stalePerQueueUtilization =
      new HashMap<>();

  private AggregationTask aggTask = null;

  final class AggregationTask implements Runnable {
    @Override
    public void run() {
      ConcurrentMap<NodeId, RMNode> rmNodes = rmContext.getRMNodes();
      Map<ApplicationId, ResourceUtilization> perAppUtilization =
          new HashMap<>();
      Map<String, ResourceUtilization> perUserUtilization =
          new HashMap<>();
      Map<Queue, ResourceUtilization> perQueueUtilization =
          new HashMap<>();
      rmNodes.values().stream()
          .filter(n -> !n.getState().isUnusable())
          .forEach(rmNode -> {
            Map<ApplicationId, ResourceUtilization> aggAppUtilizations =
                rmNode.getAggregatedAppUtilizations();
            if (aggAppUtilizations != null) {
              aggAppUtilizations.forEach((appId, appResUtilPerNode) -> {
                RMApp rmApp = rmContext.getRMApps().get(appId);
                if (rmApp != null) {
                  SchedulerApplicationAttempt appAttempt =
                      ((AbstractYarnScheduler) rmContext.getScheduler())
                          .getApplicationAttempt(
                              rmApp.getCurrentAppAttempt().getAppAttemptId());
                  if (appAttempt != null) {
                    Queue queue = appAttempt.getQueue();
                    perQueueUtilization.computeIfAbsent(queue, RU_GENERATOR)
                        .addTo(appResUtilPerNode);
                    perAppUtilization.computeIfAbsent(appId, RU_GENERATOR)
                        .addTo(appResUtilPerNode);
                    String user = appAttempt.getUser();
                    if (user != null) {
                      perUserUtilization.computeIfAbsent(user, RU_GENERATOR)
                          .addTo(appResUtilPerNode);
                    } else {
                      LOG.warn("No user found for application attempt [{}]!!",
                          appAttempt.getApplicationAttemptId());
                    }
                  } else {
                    LOG.warn("No App Attempt for application [{}]!!", appId);
                  }
                } else {
                  LOG.warn("Invalid Application [{}] received !!", appId);
                }
              });
            }
          });
      stalePerAppUtilization = perAppUtilization;
      stalePerQueueUtilization = perQueueUtilization;
      stalePerUserUtilization = perUserUtilization;
    }
  }

  /**
   * Construct the service.
   */
  public ResourceUtilizationAggregator(RMContext rmContext) {
    super("Resource Utilization Aggregator");
    this.rmContext = rmContext;
    this.scheduledExecutor = new HadoopScheduledThreadPoolExecutor(1);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    long aggInterval = conf.getLong(
        YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    this.aggTask = new AggregationTask();
    this.scheduledExecutor.scheduleAtFixedRate(aggTask,
        aggInterval, aggInterval, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  void kickoffAggregation() {
    this.aggTask.run();
  }

  /**
   * Return aggregated Resource Utilization for the User.
   * @param user User.
   * @return Resource Utilization.
   */
  public ResourceUtilization getUserResourceUtilization(String user) {
    return stalePerUserUtilization.computeIfAbsent(user, RU_GENERATOR);
  }

  /**
   * Return aggregated Resource Utilization for the Queue. Currently,
   * user is expected to provide the Leaf Queue. Aggregation across
   * the queue hierarchy is not supported since queue traversal is
   * not consistent across schedulers.
   * @param queue Queue.
   * @return Resource Utilization.
   */
  public ResourceUtilization getQueueResourceUtilization(Queue queue) {
    return stalePerQueueUtilization.computeIfAbsent(queue, RU_GENERATOR);
  }

  /**
   * Return aggregated Resource Utilization for the application.
   * @param applicationId Application Id.
   * @return Resource Utilization.
   */
  public ResourceUtilization getAppResourceUtilization(
      ApplicationId applicationId) {
    return stalePerAppUtilization.computeIfAbsent(applicationId, RU_GENERATOR);
  }
}
