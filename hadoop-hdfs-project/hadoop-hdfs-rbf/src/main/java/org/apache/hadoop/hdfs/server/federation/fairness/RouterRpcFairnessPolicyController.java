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

package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Interface to define handlers assignment for specific name services.
 * This is needed to allow handlers provide a certain QoS for all name services
 * configured. Implementations can choose to define any algorithm which
 * would help maintain QoS. This is different when compared to FairCallQueue
 * semantics as fairness has a separate context in router based federation.
 * An implementation for example, could allocate a dedicated set of handlers
 * per name service and allow handlers to continue making downstream name
 * node calls if permissions are available, another implementation could use
 * preemption semantics and dynamically increase or decrease handlers
 * assigned per name service.
 */

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface RouterRpcFairnessPolicyController {
  /**
   * Request permission for a specific name service to continue the call and
   * connect to downstream name node. Controllers based on policies defined
   * and allocations done at start-up through assignHandlersToNameservices,
   * may provide a permission or reject the request by throwing exception.
   *
   * @param nsId NS id for which a permission to continue is requested.
   * @return true or false based on whether permit is given.
   */
  boolean acquirePermit(String nsId);

  /**
   * Handler threads are expected to invoke this method that signals
   * controller to release the resources allocated to the thread for the
   * particular name service. This would mean permissions getting available
   * for other handlers to request for this specific name service.
   *
   * @param nsId Name service id for which permission release request is made.
   */
  void releasePermit(String nsId);

  /**
   * Shutdown steps to stop accepting new permission requests and clean-up.
   */
  void shutdown();

  /**
   * Returns the JSON string of the available handler for each name service.
   *
   * @return the JSON string of the available handler for each name service.
   */
  String getAvailableHandlerOnPerNs();
}
