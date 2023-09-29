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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface BlockPlacementStatus {

  /**
   * Boolean value to identify if replicas of this block satisfy requirement of 
   * placement policy
   * @return if replicas satisfy placement policy's requirement 
   */
  public boolean isPlacementPolicySatisfied();
  
  /**
   * Get description info for log or printed in case replicas are failed to meet
   * requirement of placement policy
   * @return description in case replicas are failed to meet requirement of
   * placement policy
   */
  public String getErrorDescription();

  /**
   * Return the number of additional replicas needed to ensure the block
   * placement policy is satisfied.
   * @return The number of new replicas needed to satisify the placement policy
   * or zero if no extra are needed
   */
  int getAdditionalReplicasRequired();

}
