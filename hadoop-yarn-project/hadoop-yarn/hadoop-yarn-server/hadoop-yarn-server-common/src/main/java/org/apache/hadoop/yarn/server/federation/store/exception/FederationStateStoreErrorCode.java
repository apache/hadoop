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

package org.apache.hadoop.yarn.server.federation.store.exception;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * <p>
 * Logical error codes from <code>FederationStateStore</code>.
 * </p>
 */
@Public
@Unstable
public enum FederationStateStoreErrorCode {

  MEMBERSHIP_INSERT_FAIL(1101, "Fail to insert a tuple into Membership table."),

  MEMBERSHIP_DELETE_FAIL(1102, "Fail to delete a tuple from Membership table."),

  MEMBERSHIP_SINGLE_SELECT_FAIL(1103,
      "Fail to select a tuple from Membership table."),

  MEMBERSHIP_MULTIPLE_SELECT_FAIL(1104,
      "Fail to select multiple tuples from Membership table."),

  MEMBERSHIP_UPDATE_DEREGISTER_FAIL(1105,
      "Fail to update/deregister a tuple in Membership table."),

  MEMBERSHIP_UPDATE_HEARTBEAT_FAIL(1106,
      "Fail to update/heartbeat a tuple in Membership table."),

  APPLICATIONS_INSERT_FAIL(1201,
      "Fail to insert a tuple into ApplicationsHomeSubCluster table."),

  APPLICATIONS_DELETE_FAIL(1202,
      "Fail to delete a tuple from ApplicationsHomeSubCluster table"),

  APPLICATIONS_SINGLE_SELECT_FAIL(1203,
      "Fail to select a tuple from ApplicationsHomeSubCluster table."),

  APPLICATIONS_MULTIPLE_SELECT_FAIL(1204,
      "Fail to select multiple tuple from ApplicationsHomeSubCluster table."),

  APPLICATIONS_UPDATE_FAIL(1205,
      "Fail to update a tuple in ApplicationsHomeSubCluster table."),

  POLICY_INSERT_FAIL(1301, "Fail to insert a tuple into Policy table."),

  POLICY_DELETE_FAIL(1302, "Fail to delete a tuple from Membership table."),

  POLICY_SINGLE_SELECT_FAIL(1303, "Fail to select a tuple from Policy table."),

  POLICY_MULTIPLE_SELECT_FAIL(1304,
      "Fail to select multiple tuples from Policy table."),

  POLICY_UPDATE_FAIL(1305, "Fail to update a tuple in Policy table.");

  private final int id;
  private final String msg;

  FederationStateStoreErrorCode(int id, String msg) {
    this.id = id;
    this.msg = msg;
  }

  /**
   * Get the error code related to the FederationStateStore failure.
   *
   * @return the error code related to the FederationStateStore failure.
   */
  public int getId() {
    return this.id;
  }

  /**
   * Get the error message related to the FederationStateStore failure.
   *
   * @return the error message related to the FederationStateStore failure.
   */
  public String getMsg() {
    return this.msg;
  }

  @Override
  public String toString() {
    return "\nError Code: " + this.id + "\nError Message: " + this.msg;
  }
}