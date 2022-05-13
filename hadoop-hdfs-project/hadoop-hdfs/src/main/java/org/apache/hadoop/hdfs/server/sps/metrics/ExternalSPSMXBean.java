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
package org.apache.hadoop.hdfs.server.sps.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * This is the JMX management interface for ExternalSPS information.
 * End users shouldn't be implementing these interfaces, and instead
 * access this information through the JMX APIs.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public interface ExternalSPSMXBean {

  /**
   * Gets the queue size of StorageMovementNeeded.
   *
   * @return the queue size of StorageMovementNeeded.
   */
  int getProcessingQueueSize();

  /**
   * Gets the count of movement finished blocks.
   *
   * @return the count of movement finished blocks.
   */
  int getMovementFinishedBlocksCount();

  /**
   * Gets the count of attempted items.
   *
   * @return the count of attempted items.
   */
  int getAttemptedItemsCount();
}