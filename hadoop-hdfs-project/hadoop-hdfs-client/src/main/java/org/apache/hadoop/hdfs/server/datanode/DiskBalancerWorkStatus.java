/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Helper class that reports how much work has has been done by the node.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DiskBalancerWorkStatus {
  private final int result;
  private final String planID;
  private final String status;
  private final String currentState;

  /**
   * Constructs a workStatus Object.
   *
   * @param result       - int
   * @param planID       - Plan ID
   * @param status       - Current Status
   * @param currentState - Current State
   */
  public DiskBalancerWorkStatus(int result, String planID, String status,
                                String currentState) {
    this.result = result;
    this.planID = planID;
    this.status = status;
    this.currentState = currentState;
  }

  /**
   * Returns result.
   *
   * @return long
   */
  public int getResult() {
    return result;
  }

  /**
   * Returns planID.
   *
   * @return String
   */
  public String getPlanID() {
    return planID;
  }

  /**
   * Returns Status.
   *
   * @return String
   */
  public String getStatus() {
    return status;
  }

  /**
   * Gets current Status.
   *
   * @return - Json String
   */
  public String getCurrentState() {
    return currentState;
  }
}
