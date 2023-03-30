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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An interface for Abfs Throttling Interface.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface AbfsThrottlingIntercept {

  /**
   * Updates the metrics for successful and failed read and write operations.
   * @param operationType Only applicable for read and write operations.
   * @param abfsHttpOperation Used for status code and data transferred.
   */
  void updateMetrics(AbfsRestOperationType operationType,
      AbfsHttpOperation abfsHttpOperation);

  /**
   * Called before the request is sent.  Client-side throttling
   * uses this to suspend the request, if necessary, to minimize errors and
   * maximize throughput.
   * @param operationType Only applicable for read and write operations.
   * @param abfsCounters Used for counters.
   */
  void sendingRequest(AbfsRestOperationType operationType,
      AbfsCounters abfsCounters);

}
