/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.yarn.util.Records;

/**
 * The response the services get when request is sent to put a node into
 * maintenance ON/OFF.
 */
public abstract class SetMaintenanceModeResponse {
  public static SetMaintenanceModeResponse newInstance(
      long runningContainersCount,
      long elapsedTimeSinceLastRetentionInMilliseconds) {
    SetMaintenanceModeResponse response =
        Records.newRecord(SetMaintenanceModeResponse.class);
    response.setRunningContainersCount(runningContainersCount);
    response.setElapsedTimeSinceLastRetentionInMilliseconds(
        elapsedTimeSinceLastRetentionInMilliseconds);
    return response;
  }

  /**
   * Gets the running containers on the node.
   *
   * @return
   */
  public abstract long getRunningContainersCount();

  public abstract void setRunningContainersCount(long value);

  public abstract long getElapsedTimeSinceLastRetentionInMilliseconds();

  public abstract void setElapsedTimeSinceLastRetentionInMilliseconds(
      long value);
}
