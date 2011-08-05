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
package org.apache.hadoop.ha;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * Protocol interface that provides High Availability related primitives to
 * monitor and fail-over the service.
 * 
 * This interface could be used by HA frameworks to manage the service.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface HAServiceProtocol extends VersionedProtocol {
  /**
   * Initial version of the protocol
   */
  public static final long versionID = 1L;

  /**
   * Monitor the health of service. This periodically called by the HA
   * frameworks to monitor the health of the service.
   * 
   * Service is expected to perform checks to ensure it is functional.
   * If the service is not healthy due to failure or partial failure,
   * it is expected to throw {@link HealthCheckFailedException}.
   * The definition of service not healthy is left to the service.
   * 
   * Note that when health check of an Active service fails,
   * failover to standby may be done.
   * 
   * @throws HealthCheckFailedException
   *           if the health check of a service fails.
   */
  public void monitorHealth() throws HealthCheckFailedException;

  /**
   * Request service to transition to active state. No operation, if the
   * service is already in active state.
   * 
   * @throws ServiceFailedException
   *           if transition from standby to active fails.
   */
  public void transitionToActive() throws ServiceFailedException;

  /**
   * Request service to transition to standby state. No operation, if the
   * service is already in standby state.
   * 
   * @throws ServiceFailedException
   *           if transition from active to standby fails.
   */
  public void transitionToStandby() throws ServiceFailedException;
}
