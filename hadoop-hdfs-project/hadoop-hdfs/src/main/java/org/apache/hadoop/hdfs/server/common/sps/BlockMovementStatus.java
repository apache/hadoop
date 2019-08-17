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
 * Block movement status code.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum BlockMovementStatus {
  /** Success. */
  DN_BLK_STORAGE_MOVEMENT_SUCCESS(0),
  /**
   * Failure due to generation time stamp mismatches or network errors
   * or no available space.
   */
  DN_BLK_STORAGE_MOVEMENT_FAILURE(-1);

  // TODO: need to support different type of failures. Failure due to network
  // errors, block pinned, no space available etc.

  private final int code;

  BlockMovementStatus(int code) {
    this.code = code;
  }

  /**
   * @return the status code.
   */
  int getStatusCode() {
    return code;
  }
}
