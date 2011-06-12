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

package org.apache.hadoop.yarn.server.resourcemanager.resourcetracker;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceListener;


/**
 * The read-only interface for cluster resource
 */
public interface ClusterTracker extends Recoverable{
  
  /**
   * Get all node info
   * @return a list of node info
   */
  List<NodeInfo> getAllNodeInfo();
  
  /**
   * Get cluster metrics from the resource tracker.
   * @return the cluster metrics for the cluster.
   */
  YarnClusterMetrics getClusterMetrics();
  
  /**
   * the application that is finished.
   * @param applicationId the applicaiton that finished
   * @param nodesToNotify  the nodes that need to be notified.
   */
  void finishedApplication(ApplicationId applicationId, List<NodeInfo> nodesToNotify);
  
  /**
   * Release a container
   * @param container the container to be released
   */
  boolean releaseContainer(Container container);
  
  /**
   * Adding listener to be notified of node updates.
   * @param listener
   */
  public void addListener(ResourceListener listener);
}
