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
  public static QueueInfo newInstance(String queueName, float capacity,
      float maximumCapacity, float currentCapacity,
      List<QueueInfo> childQueues, List<ApplicationReport> applications,
      QueueState queueState, Set<String> accessibleNodeLabels,
      String defaultNodeLabelExpression, QueueStatistics queueStatistics,
      boolean preemptionDisabled) {
    QueueInfo queueInfo = Records.newRecord(QueueInfo.class);
    queueInfo.setQueueName(queueName);
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
    return queueInfo;
  }

  @Private
  @Unstable
  public static QueueInfo newInstance(String queueName, float capacity,
      float maximumCapacity, float currentCapacity,
      List<QueueInfo> childQueues, List<ApplicationReport> applications,
      QueueState queueState, Set<String> accessibleNodeLabels,
      String defaultNodeLabelExpression, QueueStatistics queueStatistics,
      boolean preemptionDisabled,
      Map<String, QueueConfigurations> queueConfigurations) {
    QueueInfo queueInfo = QueueInfo.newInstance(queueName, capacity,
        maximumCapacity, currentCapacity,
        childQueues, applications,
        queueState, accessibleNodeLabels,
        defaultNodeLabelExpression, queueStatistics,
        preemptionDisabled);
    queueInfo.setQueueConfigurations(queueConfigurations);
    return queueInfo;
  }

  @Private
  @Unstable
  public static QueueInfo newInstance(String queueName, float capacity,
      float maximumCapacity, float currentCapacity,
      List<QueueInfo> childQueues, List<ApplicationReport> applications,
      QueueState queueState, Set<String> accessibleNodeLabels,
      String defaultNodeLabelExpression, QueueStatistics queueStatistics,
      boolean preemptionDisabled,
      Map<String, QueueConfigurations> queueConfigurations,
      boolean intraQueuePreemptionDisabled) {
    QueueInfo queueInfo = QueueInfo.newInstance(queueName, capacity,
        maximumCapacity, currentCapacity,
        childQueues, applications,
        queueState, accessibleNodeLabels,
        defaultNodeLabelExpression, queueStatistics,
        preemptionDisabled, queueConfigurations);
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
}
