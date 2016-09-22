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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

@Private
@Unstable
public class FSSchedulerNode extends SchedulerNode {

  private static final Log LOG = LogFactory.getLog(FSSchedulerNode.class);

  private FSAppAttempt reservedAppSchedulable;
  private final Set<RMContainer> containersForPreemption =
      new ConcurrentSkipListSet<>();

  public FSSchedulerNode(RMNode node, boolean usePortForNodeName) {
    super(node, usePortForNodeName);
  }

  @Override
  public synchronized void reserveResource(
      SchedulerApplicationAttempt application, SchedulerRequestKey schedulerKey,
      RMContainer container) {
    // Check if it's already reserved
    RMContainer reservedContainer = getReservedContainer();
    if (reservedContainer != null) {
      // Sanity check
      if (!container.getContainer().getNodeId().equals(getNodeID())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container +
            " on node " + container.getReservedNode() + 
            " when currently" + " reserved resource " + reservedContainer +
            " on node " + reservedContainer.getReservedNode());
      }
      
      // Cannot reserve more than one application on a given node!
      if (!reservedContainer.getContainer().getId().getApplicationAttemptId()
          .equals(container.getContainer().getId().getApplicationAttemptId())) {
        throw new IllegalStateException("Trying to reserve" +
            " container " + container + 
            " for application " + application.getApplicationId() + 
            " when currently" +
            " reserved container " + reservedContainer +
            " on node " + this);
      }

      LOG.info("Updated reserved container " + container.getContainer().getId()
          + " on node " + this + " for application "
          + application.getApplicationId());
    } else {
      LOG.info("Reserved container " + container.getContainer().getId()
          + " on node " + this + " for application "
          + application.getApplicationId());
    }
    setReservedContainer(container);
    this.reservedAppSchedulable = (FSAppAttempt) application;
  }

  @Override
  public synchronized void unreserveResource(
      SchedulerApplicationAttempt application) {
    // Cannot unreserve for wrong application...
    ApplicationAttemptId reservedApplication = 
        getReservedContainer().getContainer().getId().getApplicationAttemptId(); 
    if (!reservedApplication.equals(
        application.getApplicationAttemptId())) {
      throw new IllegalStateException("Trying to unreserve " +  
          " for application " + application.getApplicationId() + 
          " when currently reserved " + 
          " for application " + reservedApplication.getApplicationId() + 
          " on node " + this);
    }
    
    setReservedContainer(null);
    this.reservedAppSchedulable = null;
  }

  synchronized FSAppAttempt getReservedAppSchedulable() {
    return reservedAppSchedulable;
  }

  /**
   * Mark {@code containers} as being considered for preemption so they are
   * not considered again. A call to this requires a corresponding call to
   * {@link #removeContainerForPreemption} to ensure we do not mark a
   * container for preemption and never consider it again and avoid memory
   * leaks.
   *
   * @param containers container to mark
   */
  void addContainersForPreemption(Collection<RMContainer> containers) {
    containersForPreemption.addAll(containers);
  }

  /**
   * @return set of containers marked for preemption.
   */
  Set<RMContainer> getContainersForPreemption() {
    return containersForPreemption;
  }

  /**
   * Remove container from the set of containers marked for preemption.
   *
   * @param container container to remove
   */
  void removeContainerForPreemption(RMContainer container) {
    containersForPreemption.remove(container);
  }
}
