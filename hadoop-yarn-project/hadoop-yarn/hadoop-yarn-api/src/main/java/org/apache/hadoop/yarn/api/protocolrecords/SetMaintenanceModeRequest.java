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
 * This is used by DecommissioningProviders to set a node into Maintenance Mode.
 * This class is used by the interface MaintenanceTracker.
 */
public abstract class SetMaintenanceModeRequest {
  /**
   * Instantiates new SetMaintenanceModeRequest which accepts maintenance flag.
   *
   * @param maintenance
   * @param minimumMaintenanceTime
   * @return
   */
  public static SetMaintenanceModeRequest newInstance(boolean maintenance,
      long minimumMaintenanceTime) {
    SetMaintenanceModeRequest request =
        Records.newRecord(SetMaintenanceModeRequest.class);
    request.setMaintenance(maintenance);
    request.setMinimumMaintenanceTime(minimumMaintenanceTime);
    return request;
  }

  /**
   * Returns maintenance state, returns true if ON else false.
   *
   * @return
   */
  public abstract boolean isMaintenance();

  public abstract void setMaintenance(boolean maintenance);

  /**
   * Gets minimum maintenance time if set in request or default
   * value is returned.
   *
   * @return
   */
  public abstract long getMinimumMaintenanceTime();

  public abstract void setMinimumMaintenanceTime(long value);
}

