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
package org.apache.hadoop.hdds;

import org.apache.hadoop.utils.db.DBProfile;

/**
 * This class contains constants for configuration keys and default values
 * used in hdds.
 */
public final class HddsConfigKeys {

  /**
   * Do not instantiate.
   */
  private HddsConfigKeys() {
  }

  public static final String HDDS_HEARTBEAT_INTERVAL =
      "hdds.heartbeat.interval";
  public static final String HDDS_HEARTBEAT_INTERVAL_DEFAULT =
      "30s";

  public static final String HDDS_NODE_REPORT_INTERVAL =
      "hdds.node.report.interval";
  public static final String HDDS_NODE_REPORT_INTERVAL_DEFAULT =
      "60s";

  public static final String HDDS_CONTAINER_REPORT_INTERVAL =
      "hdds.container.report.interval";
  public static final String HDDS_CONTAINER_REPORT_INTERVAL_DEFAULT =
      "60s";

  public static final String HDDS_PIPELINE_REPORT_INTERVAL =
          "hdds.pipeline.report.interval";
  public static final String HDDS_PIPELINE_REPORT_INTERVAL_DEFAULT =
          "60s";

  public static final String HDDS_COMMAND_STATUS_REPORT_INTERVAL =
      "hdds.command.status.report.interval";
  public static final String HDDS_COMMAND_STATUS_REPORT_INTERVAL_DEFAULT =
      "60s";

  public static final String HDDS_CONTAINER_ACTION_MAX_LIMIT =
      "hdds.container.action.max.limit";
  public static final int HDDS_CONTAINER_ACTION_MAX_LIMIT_DEFAULT =
      20;

  public static final String HDDS_PIPELINE_ACTION_MAX_LIMIT =
      "hdds.pipeline.action.max.limit";
  public static final int HDDS_PIPELINE_ACTION_MAX_LIMIT_DEFAULT =
      20;

  // Configuration to allow volume choosing policy.
  public static final String HDDS_DATANODE_VOLUME_CHOOSING_POLICY =
      "hdds.datanode.volume.choosing.policy";

  // DB Profiles used by ROCKDB instances.
  public static final String HDDS_DB_PROFILE = "hdds.db.profile";
  public static final DBProfile HDDS_DEFAULT_DB_PROFILE = DBProfile.SSD;

  // Once a container usage crosses this threshold, it is eligible for
  // closing.
  public static final String HDDS_CONTAINER_CLOSE_THRESHOLD =
      "hdds.container.close.threshold";
  public static final float HDDS_CONTAINER_CLOSE_THRESHOLD_DEFAULT = 0.9f;

  public static final String HDDS_SCM_CHILLMODE_ENABLED =
      "hdds.scm.chillmode.enabled";
  public static final boolean HDDS_SCM_CHILLMODE_ENABLED_DEFAULT = true;

  // % of containers which should have at least one reported replica
  // before SCM comes out of chill mode.
  public static final String HDDS_SCM_CHILLMODE_THRESHOLD_PCT =
      "hdds.scm.chillmode.threshold.pct";
  public static final double HDDS_SCM_CHILLMODE_THRESHOLD_PCT_DEFAULT = 0.99;

  public static final String HDDS_LOCK_MAX_CONCURRENCY =
      "hdds.lock.max.concurrency";
  public static final int HDDS_LOCK_MAX_CONCURRENCY_DEFAULT = 100;

}
