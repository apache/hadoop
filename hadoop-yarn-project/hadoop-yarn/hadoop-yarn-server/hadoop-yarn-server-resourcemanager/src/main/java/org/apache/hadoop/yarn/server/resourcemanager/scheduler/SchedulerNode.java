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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Represents a YARN Cluster Node from the viewpoint of the scheduler.
 */
@Private
@Unstable
public abstract class SchedulerNode {

  /**
   * Get the name of the node for scheduling matching decisions.
   * <p/>
   * Typically this is the 'hostname' reported by the node, but it could be 
   * configured to be 'hostname:port' reported by the node via the 
   * {@link YarnConfiguration#RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} constant.
   * The main usecase of this is Yarn minicluster to be able to differentiate
   * node manager instances by their port number.
   * 
   * @return name of the node for scheduling matching decisions.
   */
  public abstract String getNodeName();
  
  /**
   * Get rackname.
   * @return rackname
   */
  public abstract String getRackName();
  
  /**
   * Get used resources on the node.
   * @return used resources on the node
   */
  public abstract Resource getUsedResource();

  /**
   * Get available resources on the node.
   * @return available resources on the node
   */
  public abstract Resource getAvailableResource();

  /**
   * Get number of active containers on the node.
   * @return number of active containers on the node
   */
  public abstract int getNumContainers();
  
  /**
   * Apply delta resource on node's available resource.
   * @param deltaResource the delta of resource need to apply to node
   */
  public abstract void applyDeltaOnAvailableResource(Resource deltaResource);

  /**
   * Get total resources on the node.
   * @return total resources on the node.
   */
  public abstract Resource getTotalResource();
  
  /**
   * Get the ID of the node which contains both its hostname and port.
   * @return the ID of the node
   */
  public abstract NodeId getNodeID();

}
