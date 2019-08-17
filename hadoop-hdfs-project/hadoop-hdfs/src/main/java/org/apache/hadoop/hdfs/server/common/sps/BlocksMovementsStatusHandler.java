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

package org.apache.hadoop.hdfs.server.common.sps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Blocks movements status handler, which can be used to collect details of the
 * completed block movements.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface BlocksMovementsStatusHandler {

  /**
   * Collect all the storage movement attempt finished blocks.
   *
   * @param moveAttemptFinishedBlk
   *          storage movement attempt finished block
   */
  void handle(BlockMovementAttemptFinished moveAttemptFinishedBlk);
}
