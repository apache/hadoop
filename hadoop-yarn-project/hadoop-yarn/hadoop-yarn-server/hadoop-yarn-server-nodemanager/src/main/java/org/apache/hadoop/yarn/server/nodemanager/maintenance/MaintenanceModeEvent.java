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

package org.apache.hadoop.yarn.server.nodemanager.maintenance;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * DecommissioningMode event is used to move NM into maintenance mode.
 */
public class MaintenanceModeEvent
    extends AbstractEvent<MaintenanceModeEventType> {

  private final String maintenanceModeProviderName;

  /**
   * Instantiates a new maintenance mode event.
   *
   * @param eventType    the event type
   * @param providerName maintenance mode provider name
   */
  public MaintenanceModeEvent(MaintenanceModeEventType eventType,
      String providerName) {
    super(eventType);
    maintenanceModeProviderName = providerName;
  }

  /**
   * Gets the maintenance mode provider name.
   *
   * @return the maintenance mode provider name
   */
  public String getMaintenanceModeProviderName() {
    return maintenanceModeProviderName;
  }
}
