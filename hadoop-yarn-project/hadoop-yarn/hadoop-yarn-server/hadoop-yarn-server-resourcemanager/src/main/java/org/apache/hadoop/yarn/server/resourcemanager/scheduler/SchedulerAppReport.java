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

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * Represents an application attempt, and the resources that the attempt is 
 * using.
 */
@Evolving
@LimitedPrivate("yarn")
public class SchedulerAppReport {
  
  private final Collection<RMContainer> live;
  private final Collection<RMContainer> reserved;
  private final boolean pending;
  
  public SchedulerAppReport(SchedulerApplicationAttempt app) {
    this.live = app.getLiveContainers();
    this.reserved = app.getReservedContainers();
    this.pending = app.isPending();
  }
  
  /**
   * Get the list of live containers
   * @return All of the live containers
   */
  public Collection<RMContainer> getLiveContainers() {
    return live;
  }
  
  /**
   * Get the list of reserved containers
   * @return All of the reserved containers.
   */
  public Collection<RMContainer> getReservedContainers() {
    return reserved;
  }
  
  /**
   * Is this application pending?
   * @return true if it is else false.
   */
  public boolean isPending() {
    return pending;
  }
}
