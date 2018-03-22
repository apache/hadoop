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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

@Evolving
@LimitedPrivate("yarn")
public interface Queue {
  /**
   * Get the queue name
   * @return queue name
   */
  String getQueueName();

  /**
   * Get the queue metrics
   * @return the queue metrics
   */
  QueueMetrics getMetrics();

  /**
   * Get queue information
   * @param includeChildQueues include child queues?
   * @param recursive recursively get child queue information?
   * @return queue information
   */
  QueueInfo getQueueInfo(boolean includeChildQueues, boolean recursive);
  
  /**
   * Get queue ACLs for given <code>user</code>.
   * @param user username
   * @return queue ACLs for user
   */
  List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user);

  boolean hasAccess(QueueACL acl, UserGroupInformation user);
  
  public AbstractUsersManager getAbstractUsersManager();

  /**
   * Recover the state of the queue for a given container.
   * @param clusterResource the resource of the cluster
   * @param schedulerAttempt the application for which the container was allocated
   * @param rmContainer the container that was recovered.
   */
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer);
  
  /**
   * Get labels can be accessed of this queue
   * labels={*}, means this queue can access any label
   * labels={ }, means this queue cannot access any label except node without label
   * labels={a, b, c} means this queue can access a or b or c  
   * @return labels
   */
  public Set<String> getAccessibleNodeLabels();
  
  /**
   * Get default label expression of this queue. If label expression of
   * ApplicationSubmissionContext and label expression of Resource Request not
   * set, this will be used.
   * 
   * @return default label expression
   */
  public String getDefaultNodeLabelExpression();

  /**
   * When new outstanding resource is asked, calling this will increase pending
   * resource in a queue.
   * 
   * @param nodeLabel asked by application
   * @param resourceToInc new resource asked
   */
  public void incPendingResource(String nodeLabel, Resource resourceToInc);
  
  /**
   * When an outstanding resource is fulfilled or canceled, calling this will
   * decrease pending resource in a queue.
   * 
   * @param nodeLabel
   *          asked by application
   * @param resourceToDec
   *          new resource asked
   */
  public void decPendingResource(String nodeLabel, Resource resourceToDec);

  /**
   * Get the Default Application Priority for this queue
   *
   * @return default application priority
   */
  public Priority getDefaultApplicationPriority();

  /**
   * Increment Reserved Capacity
   *
   * @param partition
   *          asked by application
   * @param reservedRes
   *          reserved resource asked
   */
  public void incReservedResource(String partition, Resource reservedRes);

  /**
   * Decrement Reserved Capacity
   *
   * @param partition
   *          asked by application
   * @param reservedRes
   *          reserved resource asked
   */
  public void decReservedResource(String partition, Resource reservedRes);
}
