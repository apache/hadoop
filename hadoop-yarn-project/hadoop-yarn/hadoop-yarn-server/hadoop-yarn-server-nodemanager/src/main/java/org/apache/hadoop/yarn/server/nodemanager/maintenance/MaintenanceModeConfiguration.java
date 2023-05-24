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

import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MaintenanceModeConfiguration {

  /**
   * Maintenance consumer heartbeat expiry time
   */
  public static final String NM_MAINTENANCE_MODE_CONSUMER_HEARTBEAT_EXPIRY_SECONDS =
      YarnConfiguration.NM_PREFIX
          + "maintenance.mode.consumer.heartbeat.expiry.seconds";
  public static final int NM_MAINTENANCE_MODE_CONSUMER_DEFAULT_HEARTBEAT_EXPIRY_SECONDS =
      5 * 60;

  /**
   * Maintenance provider heartbeat interval
   */
  public static final String NM_MAINTENANCE_MODE_PROVIDER_HEARTBEAT_INTERVAL_SECONDS =
      YarnConfiguration.NM_PREFIX
          + "maintenance.mode.provider.heartbeat.interval.seconds";
  public static final int NM_MAINTENANCE_MODE_PROVIDER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS =
      30;

  /**
   * Environment flag in container launch context that determines whether the
   * container should be counted when in maintenance mode.
   */
  public static final String CONTAINER_DRAIN_FLAG =
      "yarn.nodemanager.yarnpp.container-drain-flag";

  /**
   * Default flag.
   */
  public static final String CONTAINER_DRAIN_FLAG_DEFAULT =
      "container_drain_enabled";
}