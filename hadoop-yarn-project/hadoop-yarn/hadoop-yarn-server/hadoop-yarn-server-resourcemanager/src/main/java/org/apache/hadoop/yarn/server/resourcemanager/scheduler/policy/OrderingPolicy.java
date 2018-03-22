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
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;


/**
 * OrderingPolicy is used by the scheduler to order SchedulableEntities for
 * container assignment and preemption.
 * @param <S> the type of {@link SchedulableEntity} that will be compared
 */
public interface OrderingPolicy<S extends SchedulableEntity> {
  /*
   * Note: OrderingPolicy depends upon external
   * synchronization of all use of the SchedulableEntity Collection and
   * Iterators for correctness and to avoid concurrent modification issues
   */

  /**
   * Get the collection of {@link SchedulableEntity} Objects which are managed
   * by this OrderingPolicy - should include processes returned by the
   * Assignment and Preemption iterator with no guarantees regarding order.
   * @return a collection of {@link SchedulableEntity} objects
   */
  public Collection<S> getSchedulableEntities();

  /**
   * Return an iterator over the collection of {@link SchedulableEntity}
   * objects which orders them for container assignment.
   * @return an iterator over the collection of {@link SchedulableEntity}
   * objects
   */
  public Iterator<S> getAssignmentIterator();

  /**
   * Return an iterator over the collection of {@link SchedulableEntity}
   * objects which orders them for preemption.
   * @return an iterator over the collection of {@link SchedulableEntity}
   */
  public Iterator<S> getPreemptionIterator();

  /**
   * Add a {@link SchedulableEntity} to be managed for allocation and preemption
   * ordering.
   * @param s the {@link SchedulableEntity} to add
   */
  public void addSchedulableEntity(S s);

  /**
   * Remove a {@link SchedulableEntity} from management for allocation and
   * preemption ordering.
   * @param s the {@link SchedulableEntity} to remove
   * @return whether the {@link SchedulableEntity} was present before this
   * operation
   */
  public boolean removeSchedulableEntity(S s);

  /**
   * Add a collection of {@link SchedulableEntity} objects to be managed for
   * allocation and preemption ordering.
   * @param sc the collection of {@link SchedulableEntity} objects to add
   */
  public void addAllSchedulableEntities(Collection<S> sc);

  /**
   * Get the number of {@link SchedulableEntity} objects managed for allocation
   * and preemption ordering.
   * @return the number of {@link SchedulableEntity} objects
   */
  public int getNumSchedulableEntities();

  /**
   * Provides configuration information for the policy from the scheduler
   * configuration.
   * @param conf a map of scheduler configuration properties and values
   */
  public void configure(Map<String, String> conf);

  /**
   * Notify the {@code OrderingPolicy} that the {@link SchedulableEntity}
   * has been allocated the given {@link RMContainer}, enabling the
   * {@code OrderingPolicy} to take appropriate action. Depending on the
   * comparator, a reordering of the {@link SchedulableEntity} may be required.
   * @param schedulableEntity the {@link SchedulableEntity}
   * @param r the allocated {@link RMContainer}
   */
  public void containerAllocated(S schedulableEntity, RMContainer r);

  /**
   * Notify the {@code OrderingPolicy} that the {@link SchedulableEntity}
   * has released the given {@link RMContainer}, enabling the
   * {@code OrderingPolicy} to take appropriate action. Depending on the
   * comparator, a reordering of the {@link SchedulableEntity} may be required.
   * @param schedulableEntity the {@link SchedulableEntity}
   * @param r the released {@link RMContainer}
   */
  public void containerReleased(S schedulableEntity, RMContainer r);

  /**
   * Notify the {@code OrderingPolicy} that the demand for the
   * {@link SchedulableEntity} has been updated, enabling the
   * {@code OrderingPolicy} to reorder the {@link SchedulableEntity} if needed.
   * @param schedulableEntity the updated {@link SchedulableEntity}
   */
  void demandUpdated(S schedulableEntity);

  /**
   * Return information regarding configuration and status.
   * @return configuration and status information
   */
  public String getInfo();

}
