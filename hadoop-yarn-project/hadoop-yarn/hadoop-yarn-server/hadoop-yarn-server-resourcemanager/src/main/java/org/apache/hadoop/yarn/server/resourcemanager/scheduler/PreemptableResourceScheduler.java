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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * Interface for a scheduler that supports preemption/killing
 *
 */
public interface PreemptableResourceScheduler extends ResourceScheduler {

  /**
   * If the scheduler support container reservations, this method is used to
   * ask the scheduler to drop the reservation for the given container.
   * @param container Reference to reserved container allocation.
   */
  void dropContainerReservation(RMContainer container);

  /**
   * Ask the scheduler to obtain back the container from a specific application
   * by issuing a preemption request
   * @param aid the application from which we want to get a container back
   * @param container the container we want back
   */
  void preemptContainer(ApplicationAttemptId aid, RMContainer container);

  /**
   * Ask the scheduler to forcibly interrupt the container given as input
   * @param container
   */
  void killContainer(RMContainer container);

}
