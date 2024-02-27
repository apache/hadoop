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

package org.apache.hadoop.yarn.api.records;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.util.Records;

/**
 * QueueInfo is a report of the runtime information of the queue.
 * <p>
 * It includes information such as:
 * <ul>
 *   <li>Queue name.</li>
 *   <li>Capacity of the queue.</li>
 *   <li>Maximum capacity of the queue.</li>
 *   <li>Current capacity of the queue.</li>
 *   <li>Child queues.</li>
 *   <li>Running applications.</li>
 *   <li>{@link QueueState} of the queue.</li>
 *   <li>{@link QueueConfigurations} of the queue.</li>
 * </ul>
 *
 * @see QueueState
 * @see QueueConfigurations
 * @see ApplicationClientProtocol#getQueueInfo(org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest)
 */
@Public
@Stable
public abstract class QueueInfo {
  
  @Private
  @Unstable
  public static QueueInfo newInstance(String queueName,
      String queuePath, float capacity,
      float maximumCapacity, float currentCapacity,
      List<QueueInfo> childQueues, List<ApplicationReport> applications,
      QueueState queueState, Set<String> accessibleNodeLabels,
      String defaultNodeLabelExpression, QueueStatistics queueStatistics,
      boolean preemptionDisabled, float weight,
      int maxParallelApps) {
    QueueInfo queueInfo = Records.newRecord(QueueInfo.class);
    queueInfo.setQueueName(queueName);
    queueInfo.setQueuePath(queuePath);
    queueInfo.setCapacity(capacity);
    queueInfo.setMaximumCapacity(maximumCapacity);
    queueInfo.setCurrentCapacity(currentCapacity);
    queueInfo.setChildQueues(childQueues);
    queueInfo.setApplications(applications);
    queueInfo.setQueueState(queueState);
    queueInfo.setAccessibleNodeLabels(accessibleNodeLabels);
    queueInfo.setDefaultNodeLabelExpression(defaultNodeLabelExpression);
    queueInfo.setQueueStatistics(queueStatistics);
    queueInfo.setPreemptionDisabled(preemptionDisabled);
    queueInfo.setWeight(weight);
    queueInfo.setMaxParallelApps(maxParallelApps);
    return queueInfo;
  }

  @Private
  @Unstable
  public static QueueInfo newInstance(String queueName,
      String queuePath, float capacity,
      float maximumCapacity, float currentCapacity,
      List<QueueInfo> childQueues, List<ApplicationReport> applications,
      QueueState queueState, Set<String> accessibleNodeLabels,
      String defaultNodeLabelExpression, QueueStatistics queueStatistics,
      boolean preemptionDisabled, float weight, int maxParallelApps,
      Map<String, QueueConfigurations> queueConfigurations) {
    QueueInfo queueInfo = QueueInfo.newInstance(queueName, queuePath, capacity,
        maximumCapacity, currentCapacity,
        childQueues, applications,
        queueState, accessibleNodeLabels,
        defaultNodeLabelExpression, queueStatistics,
        preemptionDisabled, weight, maxParallelApps);
    queueInfo.setQueueConfigurations(queueConfigurations);
    return queueInfo;
  }

  @Private
  @Unstable
  public static QueueInfo newInstance(String queueName,
      String queuePath, float capacity,
      float maximumCapacity, float currentCapacity,
      List<QueueInfo> childQueues, List<ApplicationReport> applications,
      QueueState queueState, Set<String> accessibleNodeLabels,
      String defaultNodeLabelExpression, QueueStatistics queueStatistics,
      boolean preemptionDisabled, float weight, int maxParallelApps,
      Map<String, QueueConfigurations> queueConfigurations,
      boolean intraQueuePreemptionDisabled) {
    QueueInfo queueInfo = QueueInfo.newInstance(queueName, queuePath, capacity,
        maximumCapacity, currentCapacity,
        childQueues, applications,
        queueState, accessibleNodeLabels,
        defaultNodeLabelExpression, queueStatistics,
        preemptionDisabled, weight, maxParallelApps, queueConfigurations);
    queueInfo.setIntraQueuePreemptionDisabled(intraQueuePreemptionDisabled);
    return queueInfo;
  }

  /**
   * Get the <em>name</em> of the queue.
   * @return <em>name</em> of the queue
   */
  @Public
  @Stable
  public abstract String getQueueName();
  
  @Private
  @Unstable
  public abstract void setQueueName(String queueName);

  /**
   * Get the <em>path</em> of the queue.
   * @return <em>path</em> of the queue
   */
  @Public
  @Stable
  public abstract String getQueuePath();

  @Private
  @Unstable
  public abstract void setQueuePath(String queuePath);
  
  /**
   * Get the <em>configured capacity</em> of the queue.
   * @return <em>configured capacity</em> of the queue
   */
  @Public
  @Stable
  public abstract float getCapacity();
  
  @Private
  @Unstable
  public abstract void setCapacity(float capacity);

  /**
   * Get the <em>configured weight</em> of the queue.
   * @return <em>configured weight</em> of the queue
   */
  @Public
  @Stable
  public abstract float getWeight();

  @Private
  @Unstable
  public abstract void setWeight(float weight);

  /**
   * Get the <em>configured max parallel apps</em> of the queue.
   * @return <em>configured max parallel apps</em> of the queue
   */
  @Public
  @Stable
  public abstract int getMaxParallelApps();

  @Private
  @Unstable
  public abstract void setMaxParallelApps(int maxParallelApps);
  
  /**
   * Get the <em>maximum capacity</em> of the queue.
   * @return <em>maximum capacity</em> of the queue
   */
  @Public
  @Stable
  public abstract float getMaximumCapacity();
  
  @Private
  @Unstable
  public abstract void setMaximumCapacity(float maximumCapacity);
  
  /**
   * Get the <em>current capacity</em> of the queue.
   * @return <em>current capacity</em> of the queue
   */
  @Public
  @Stable
  public abstract float getCurrentCapacity();
  
  @Private
  @Unstable
  public abstract void setCurrentCapacity(float currentCapacity);
  
  /**
   * Get the <em>child queues</em> of the queue.
   * @return <em>child queues</em> of the queue
   */
  @Public
  @Stable
  public abstract List<QueueInfo> getChildQueues();
  
  @Private
  @Unstable
  public abstract void setChildQueues(List<QueueInfo> childQueues);
  
  /**
   * Get the <em>running applications</em> of the queue.
   * @return <em>running applications</em> of the queue
   */
  @Public
  @Stable
  public abstract List<ApplicationReport> getApplications();
  
  @Private
  @Unstable
  public abstract void setApplications(List<ApplicationReport> applications);
  
  /**
   * Get the <code>QueueState</code> of the queue.
   * @return <code>QueueState</code> of the queue
   */
  @Public
  @Stable
  public abstract QueueState getQueueState();
  
  @Private
  @Unstable
  public abstract void setQueueState(QueueState queueState);
  
  /**
   * Get the <code>accessible node labels</code> of the queue.
   * @return <code>accessible node labels</code> of the queue
   */
  @Public
  @Stable
  public abstract Set<String> getAccessibleNodeLabels();
  
  /**
   * Set the <code>accessible node labels</code> of the queue.
   * @param labels node label expression of the queue.
   */
  @Private
  @Unstable
  public abstract void setAccessibleNodeLabels(Set<String> labels);
  
  /**
   * Get the <code>default node label expression</code> of the queue, this takes
   * affect only when the <code>ApplicationSubmissionContext</code> and
   * <code>ResourceRequest</code> don't specify their
   * <code>NodeLabelExpression</code>.
   * 
   * @return <code>default node label expression</code> of the queue
   */
  @Public
  @Stable
  public abstract String getDefaultNodeLabelExpression();
  
  @Public
  @Stable
  public abstract void setDefaultNodeLabelExpression(
      String defaultLabelExpression);

  /**
   * Get the <code>queue stats</code> for the queue
   *
   * @return <code>queue stats</code> of the queue
   */
  @Public
  @Unstable
  public abstract QueueStatistics getQueueStatistics();

  /**
   * Set the queue statistics for the queue
   * 
   * @param queueStatistics
   *          the queue statistics
   */
  @Public
  @Unstable
  public abstract void setQueueStatistics(QueueStatistics queueStatistics);

  /**
   * Get the <em>preemption status</em> of the queue.
   * @return if property is not in proto, return null;
   *        otherwise, return <em>preemption status</em> of the queue
   */
  @Public
  @Stable
  public abstract Boolean getPreemptionDisabled();

  @Private
  @Unstable
  public abstract void setPreemptionDisabled(boolean preemptionDisabled);

  /**
   * Get the per-node-label queue configurations of the queue.
   *
   * @return the per-node-label queue configurations of the queue.
   */
  @Public
  @Stable
  public abstract Map<String, QueueConfigurations> getQueueConfigurations();

  /**
   * Set the per-node-label queue configurations for the queue.
   *
   * @param queueConfigurations
   *          the queue configurations
   */
  @Private
  @Unstable
  public abstract void setQueueConfigurations(
      Map<String, QueueConfigurations> queueConfigurations);


  /**
   * Get the intra-queue preemption status of the queue.
   * @return if property is not in proto, return null;
   *        otherwise, return intra-queue preemption status of the queue
   */
  @Public
  @Stable
  public abstract Boolean getIntraQueuePreemptionDisabled();

  @Private
  @Unstable
  public abstract void setIntraQueuePreemptionDisabled(
      boolean intraQueuePreemptionDisabled);

  /**
   * Get Scheduler type.
   *
   * @return SchedulerType.
   */
  @Public
  @Stable
  public abstract String getSchedulerType();

  /**
   * Set Scheduler type.
   * @param schedulerType scheduler Type.
   */
  @Private
  @Unstable
  public abstract void setSchedulerType(String schedulerType);

  /**
   * Get the minimum resource VCore.
   * @return minimum resource VCore.
   */
  @Public
  @Stable
  public abstract int getMinResourceVCore();

  /**
   * Set the minimum resource VCore.
   * @param vCore minimum resource VCore.
   */
  @Private
  @Unstable
  public abstract void setMinResourceVCore(int vCore);

  /**
   * Get the minimum resource Memory.
   * @return minimum resource Memory.
   */
  @Public
  @Stable
  public abstract long getMinResourceMemory();

  /**
   * Set the minimum resource Memory.
   * @param memory minimum resource Memory.
   */
  @Private
  @Unstable
  public abstract void setMinResourceMemory(long memory);

  /**
   * Get the maximum resource VCore.
   * @return maximum resource VCore.
   */
  @Public
  @Stable
  public abstract int getMaxResourceVCore();

  /**
   * Set the maximum resource Memory.
   * @param vCore maximum resource VCore.
   */
  @Private
  @Unstable
  public abstract void setMaxResourceVCore(int vCore);

  /**
   * Get the maximum resource Memory.
   * @return maximum resource Memory.
   */
  @Public
  @Stable
  public abstract long getMaxResourceMemory();

  /**
   * Set the maximum resource Memory.
   * @param memory maximum resource Memory.
   */
  @Private
  @Unstable
  public abstract void setMaxResourceMemory(long memory);

  /**
   * Get the reserved resource VCore.
   * @return reserved resource VCore.
   */
  @Public
  @Stable
  public abstract int getReservedResourceVCore();

  /**
   * Set the reserved resource VCore.
   * @param vCore reserved resource VCore.
   */
  @Private
  @Unstable
  public abstract void setReservedResourceVCore(int vCore);

  /**
   * Get the reserved resource Memory.
   * @return reserved resource Memory.
   */
  @Public
  @Stable
  public abstract long getReservedResourceMemory();

  /**
   * Set the reserved resource Memory.
   * @param memory reserved resource Memory.
   */
  @Private
  @Unstable
  public abstract void setReservedResourceMemory(long memory);

  /**
   * Get the SteadyFairShare VCore.
   * @return SteadyFairShare VCore.
   */
  @Public
  @Stable
  public abstract int getSteadyFairShareVCore();

  /**
   * Set the SteadyFairShare VCore.
   * @param vCore SteadyFairShare VCore.
   */
  @Private
  @Unstable
  public abstract void setSteadyFairShareVCore(int vCore);

  /**
   * Get the SteadyFairShare Memory.
   * @return SteadyFairShare Memory.
   */
  @Public
  @Stable
  public abstract long getSteadyFairShareMemory();

  /**
   * Set the SteadyFairShare Memory.
   * @param memory SteadyFairShare Memory.
   */
  @Private
  @Unstable
  public abstract void setSteadyFairShareMemory(long memory);

  /**
   * Get the SubClusterId.
   * @return the SubClusterId.
   */
  @Public
  @Stable
  public abstract String getSubClusterId();

  /**
   * Set the SubClusterId.
   * @param subClusterId the SubClusterId.
   */
  @Private
  @Unstable
  public abstract void setSubClusterId(String subClusterId);

  /**
   * Get the MaxRunningApp.
   * @return The number of MaxRunningApp.
   */
  @Public
  @Stable
  public abstract int getMaxRunningApp();

  @Private
  @Unstable
  public abstract void setMaxRunningApp(int maxRunningApp);
}
