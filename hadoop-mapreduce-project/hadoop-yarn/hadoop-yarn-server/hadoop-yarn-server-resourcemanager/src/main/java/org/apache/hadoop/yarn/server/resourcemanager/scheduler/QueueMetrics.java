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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import static org.apache.hadoop.metrics2.lib.Interns.info;
import static org.apache.hadoop.yarn.server.resourcemanager.resource.Resources.multiply;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;

@InterfaceAudience.Private
@Metrics(context="yarn")
public class QueueMetrics {
  @Metric("# of apps submitted") MutableCounterInt appsSubmitted;
  @Metric("# of running apps") MutableGaugeInt appsRunning;
  @Metric("# of pending apps") MutableGaugeInt appsPending;
  @Metric("# of apps completed") MutableCounterInt appsCompleted;
  @Metric("# of apps killed") MutableCounterInt appsKilled;
  @Metric("# of apps failed") MutableCounterInt appsFailed;

  @Metric("Allocated memory in GiB") MutableGaugeInt allocatedGB;
  @Metric("# of allocated containers") MutableGaugeInt allocatedContainers;
  @Metric("Aggregate # of allocated containers") MutableCounterLong aggregateContainersAllocated;
  @Metric("Aggregate # of released containers") MutableCounterLong aggregateContainersReleased;
  @Metric("Available memory in GiB") MutableGaugeInt availableGB;
  @Metric("Pending memory allocation in GiB") MutableGaugeInt pendingGB;
  @Metric("# of pending containers") MutableGaugeInt pendingContainers;
  @Metric("# of reserved memory in GiB") MutableGaugeInt reservedGB;
  @Metric("# of reserved containers") MutableGaugeInt reservedContainers;

  static final Logger LOG = LoggerFactory.getLogger(QueueMetrics.class);
  static final int GB = 1024; // resource.memory is in MB
  static final MetricsInfo RECORD_INFO = info("QueueMetrics",
      "Metrics for the resource scheduler");
  static final MetricsInfo QUEUE_INFO = info("Queue", "Metrics by queue");
  static final MetricsInfo USER_INFO = info("User", "Metrics by user");
  static final Splitter Q_SPLITTER =
      Splitter.on('.').omitEmptyStrings().trimResults();

  final MetricsRegistry registry;
  final String queueName;
  final QueueMetrics parent;
  final MetricsSystem metricsSystem;
  private final Map<String, QueueMetrics> users;

  QueueMetrics(MetricsSystem ms, String queueName, Queue parent, boolean enableUserMetrics) {
    registry = new MetricsRegistry(RECORD_INFO);
    this.queueName = queueName;
    this.parent = parent != null ? parent.getMetrics() : null;
    this.users = enableUserMetrics ? new HashMap<String, QueueMetrics>()
                                   : null;
    metricsSystem = ms;
  }

  QueueMetrics tag(MetricsInfo info, String value) {
    registry.tag(info, value);
    return this;
  }

  static StringBuilder sourceName(String queueName) {
    StringBuilder sb = new StringBuilder(RECORD_INFO.name());
    int i = 0;
    for (String node : Q_SPLITTER.split(queueName)) {
      sb.append(",q").append(i++).append('=').append(node);
    }
    return sb;
  }

  public synchronized
  static QueueMetrics forQueue(String queueName, Queue parent,
                               boolean enableUserMetrics) {
    return forQueue(DefaultMetricsSystem.instance(), queueName, parent,
                    enableUserMetrics);
  }

  public static QueueMetrics forQueue(MetricsSystem ms, String queueName,
                                      Queue parent, boolean enableUserMetrics) {
    QueueMetrics metrics = new QueueMetrics(ms, queueName, parent,
        enableUserMetrics).tag(QUEUE_INFO, queueName);
    return ms == null ? metrics : ms.register(sourceName(queueName).toString(),
        "Metrics for queue: " + queueName, metrics);
  }

  public synchronized QueueMetrics getUserMetrics(String userName) {
    if (users == null) {
      return null;
    }
    QueueMetrics metrics = users.get(userName);
    if (metrics == null) {
      metrics = new QueueMetrics(metricsSystem, queueName, null, false);
      users.put(userName, metrics);
      metricsSystem.register(
          sourceName(queueName).append(",user=").append(userName).toString(),
          "Metrics for user '"+ userName +"' in queue '"+ queueName +"'",
          metrics.tag(QUEUE_INFO, queueName).tag(USER_INFO, userName));
    }
    return metrics;
  }

  public void submitApp(String user) {
    appsSubmitted.incr();
    appsPending.incr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.submitApp(user);
    }
    if (parent != null) {
      parent.submitApp(user);
    }
  }

  public void incrAppsRunning(String user) {
    appsRunning.incr();
    appsPending.decr();
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.incrAppsRunning(user);
    }
    if (parent != null) {
      parent.incrAppsRunning(user);
    }
  }

  public void finishApp(AppSchedulingInfo app,
      RMAppAttemptState rmAppAttemptFinalState) {
    switch (rmAppAttemptFinalState) {
      case KILLED: appsKilled.incr(); break;
      case FAILED: appsFailed.incr(); break;
      default: appsCompleted.incr();  break;
    }
    if (app.isPending()) {
      appsPending.decr();
    } else {
      appsRunning.decr();
    }
    QueueMetrics userMetrics = getUserMetrics(app.getUser());
    if (userMetrics != null) {
      userMetrics.finishApp(app, rmAppAttemptFinalState);
    }
    if (parent != null) {
      parent.finishApp(app, rmAppAttemptFinalState);
    }
  }

  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   * @param limit resource limit
   */
  public void setAvailableResourcesToQueue(Resource limit) {
    availableGB.set(limit.getMemory()/GB);
  }

  /**
   * Set available resources. To be called by scheduler periodically as
   * resources become available.
   * @param user
   * @param limit resource limit
   */
  public void setAvailableResourcesToUser(String user, Resource limit) {
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.setAvailableResourcesToQueue(limit);
    }
  }

  /**
   * Increment pending resource metrics
   * @param user
   * @param containers
   * @param res the TOTAL delta of resources note this is different from
   *            the other APIs which use per container resource
   */
  public void incrPendingResources(String user, int containers, Resource res) {
    _incrPendingResources(containers, res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.incrPendingResources(user, containers, res);
    }
    if (parent != null) {
      parent.incrPendingResources(user, containers, res);
    }
  }

  private void _incrPendingResources(int containers, Resource res) {
    pendingContainers.incr(containers);
    pendingGB.incr(res.getMemory()/GB);
  }

  public void decrPendingResources(String user, int containers, Resource res) {
    _decrPendingResources(containers, res);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.decrPendingResources(user, containers, res);
    }
    if (parent != null) {
      parent.decrPendingResources(user, containers, res);
    }
  }

  private void _decrPendingResources(int containers, Resource res) {
    pendingContainers.decr(containers);
    pendingGB.decr(res.getMemory()/GB);
  }

  public void allocateResources(String user, int containers, Resource res) {
    allocatedContainers.incr(containers);
    aggregateContainersAllocated.incr(containers);
    allocatedGB.incr(res.getMemory()/GB * containers);
    _decrPendingResources(containers, multiply(res, containers));
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.allocateResources(user, containers, res);
    }
    if (parent != null) {
      parent.allocateResources(user, containers, res);
    }
  }

  public void releaseResources(String user, int containers, Resource res) {
    allocatedContainers.decr(containers);
    aggregateContainersReleased.incr(containers);
    allocatedGB.decr(res.getMemory()/GB * containers);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.releaseResources(user, containers, res);
    }
    if (parent != null) {
      parent.releaseResources(user, containers, res);
    }
  }

  public void reserveResource(String user, Resource res) {
    reservedContainers.incr();
    reservedGB.incr(res.getMemory()/GB);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.reserveResource(user, res);
    }
    if (parent != null) {
      parent.reserveResource(user, res);
    }
  }

  public void unreserveResource(String user, Resource res) {
    reservedContainers.decr();
    reservedGB.decr(res.getMemory()/GB);
    QueueMetrics userMetrics = getUserMetrics(user);
    if (userMetrics != null) {
      userMetrics.unreserveResource(user, res);
    }
    if (parent != null) {
      parent.unreserveResource(user, res);
    }
  }

  public int getAppsSubmitted() {
    return appsSubmitted.value();
  }

  public int getAppsRunning() {
    return appsRunning.value();
  }

  public int getAppsPending() {
    return appsPending.value();
  }

  public int getAppsCompleted() {
    return appsCompleted.value();
  }

  public int getAppsKilled() {
    return appsKilled.value();
  }

  public int getAppsFailed() {
    return appsFailed.value();
  }

  public int getAllocatedGB() {
    return allocatedGB.value();
  }

  public int getAllocatedContainers() {
    return allocatedContainers.value();
  }

  public int getAvailableGB() {
    return availableGB.value();
  }  

  public int getPendingGB() {
    return pendingGB.value();
  }

  public int getPendingContainers() {
    return pendingContainers.value();
  }
  
  public int getReservedGB() {
    return reservedGB.value();
  }

  public int getReservedContainers() {
    return reservedContainers.value();
  }
}
