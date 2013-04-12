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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.ClientRMProtocol;

/**
 * <p>QueueInfo is a report of the runtime information of the queue.</p>
 * 
 * <p>It includes information such as:
 *   <ul>
 *     <li>Queue name.</li>
 *     <li>Capacity of the queue.</li>
 *     <li>Maximum capacity of the queue.</li>
 *     <li>Current capacity of the queue.</li>
 *     <li>Child queues.</li>
 *     <li>Running applications.</li>
 *     <li>{@link QueueState} of the queue.</li>
 *   </ul>
 * </p>
 *
 * @see QueueState
 * @see ClientRMProtocol#getQueueInfo(org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest)
 */
@Public
@Stable
public interface QueueInfo {
  /**
   * Get the <em>name</em> of the queue.
   * @return <em>name</em> of the queue
   */
  @Public
  @Stable
  String getQueueName();
  
  @Private
  @Unstable
  void setQueueName(String queueName);
  
  /**
   * Get the <em>configured capacity</em> of the queue.
   * @return <em>configured capacity</em> of the queue
   */
  @Public
  @Stable
  float getCapacity();
  
  @Private
  @Unstable
  void setCapacity(float capacity);
  
  /**
   * Get the <em>maximum capacity</em> of the queue.
   * @return <em>maximum capacity</em> of the queue
   */
  @Public
  @Stable
  float getMaximumCapacity();
  
  @Private
  @Unstable
  void setMaximumCapacity(float maximumCapacity);
  
  /**
   * Get the <em>current capacity</em> of the queue.
   * @return <em>current capacity</em> of the queue
   */
  @Public
  @Stable
  float getCurrentCapacity();
  
  @Private
  @Unstable
  void setCurrentCapacity(float currentCapacity);
  
  /**
   * Get the <em>child queues</em> of the queue.
   * @return <em>child queues</em> of the queue
   */
  @Public
  @Stable
  List<QueueInfo> getChildQueues();
  
  @Private
  @Unstable
  void setChildQueues(List<QueueInfo> childQueues);
  
  /**
   * Get the <em>running applications</em> of the queue.
   * @return <em>running applications</em> of the queue
   */
  @Public
  @Stable
  List<ApplicationReport> getApplications();
  
  @Private
  @Unstable
  void setApplications(List<ApplicationReport> applications);
  
  /**
   * Get the <code>QueueState</code> of the queue.
   * @return <code>QueueState</code> of the queue
   */
  @Public
  @Stable
  QueueState getQueueState();
  
  @Private
  @Unstable
  void setQueueState(QueueState queueState);
}
