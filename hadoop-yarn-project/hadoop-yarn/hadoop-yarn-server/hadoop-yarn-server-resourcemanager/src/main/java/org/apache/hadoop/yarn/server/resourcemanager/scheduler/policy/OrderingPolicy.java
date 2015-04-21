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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;


/**
 * OrderingPolicy is used by the scheduler to order SchedulableEntities for
 * container assignment and preemption
 */
public interface OrderingPolicy<S extends SchedulableEntity> {
  /*
   * Note: OrderingPolicy depends upon external
   * synchronization of all use of the SchedulableEntity Collection and
   * Iterators for correctness and to avoid concurrent modification issues
   */
  
  /**
   * Get the collection of SchedulableEntities which are managed by this
   * OrderingPolicy - should include processes returned by the Assignment and
   * Preemption iterator with no guarantees regarding order
   */
  public Collection<S> getSchedulableEntities();
  
  /**
   * Return an iterator over the collection of SchedulableEntities which orders
   * them for container assignment
   */
  public Iterator<S> getAssignmentIterator();
  
  /**
   * Return an iterator over the collection of SchedulableEntities which orders
   * them for preemption
   */
  public Iterator<S> getPreemptionIterator();
  
  /**
   * Add a SchedulableEntity to be managed for allocation and preemption 
   * ordering
   */
  public void addSchedulableEntity(S s);
  
  /**
   * Remove a SchedulableEntity from management for allocation and preemption 
   * ordering
   */
  public boolean removeSchedulableEntity(S s);
  
  /**
   * Add a collection of SchedulableEntities to be managed for allocation 
   * and preemption ordering
   */
  public void addAllSchedulableEntities(Collection<S> sc);
  
  /**
   * Get the number of SchedulableEntities managed for allocation and
   * preemption ordering
   */
  public int getNumSchedulableEntities();
  
  /**
   * Provides configuration information for the policy from the scheduler
   * configuration
   */
  public void configure(Map<String, String> conf);
  
  /**
   * The passed SchedulableEntity has been allocated the passed Container,
   * take appropriate action (depending on comparator, a reordering of the
   * SchedulableEntity may be required)
   */
  public void containerAllocated(S schedulableEntity, 
    RMContainer r);
  
  /**
   * The passed SchedulableEntity has released the passed Container,
   * take appropriate action (depending on comparator, a reordering of the
   * SchedulableEntity may be required)
   */
  public void containerReleased(S schedulableEntity, 
    RMContainer r);
  
  /**
   * Display information regarding configuration & status
   */
  public String getInfo();
  
}
