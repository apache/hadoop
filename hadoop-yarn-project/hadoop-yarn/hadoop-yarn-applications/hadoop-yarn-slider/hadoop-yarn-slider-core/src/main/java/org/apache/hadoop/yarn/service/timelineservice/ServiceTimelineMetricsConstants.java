/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.service.timelineservice;

/**
 * Constants which are stored as key in ATS
 */
public final class ServiceTimelineMetricsConstants {

  public static final String URI = "URI";

  public static final String NAME = "NAME";

  public static final String STATE = "STATE";

  public static final String EXIT_STATUS_CODE = "EXIT_STATUS_CODE";

  public static final String EXIT_REASON = "EXIT_REASON";

  public static final String DIAGNOSTICS_INFO = "DIAGNOSTICS_INFO";

  public static final String LAUNCH_TIME = "LAUNCH_TIME";

  public static final String QUICK_LINKS = "QUICK_LINKS";

  public static final String LAUNCH_COMMAND = "LAUNCH_COMMAND";

  public static final String TOTAL_CONTAINERS = "NUMBER_OF_CONTAINERS";

  public static final String RUNNING_CONTAINERS =
      "NUMBER_OF_RUNNING_CONTAINERS";

  /**
   * Artifacts constants.
   */
  public static final String ARTIFACT_ID = "ARTIFACT_ID";

  public static final String ARTIFACT_TYPE = "ARTIFACT_TYPE";

  public static final String ARTIFACT_URI = "ARTIFACT_URI";

  /**
   * Resource constants.
   */
  public static final String RESOURCE_CPU = "RESOURCE_CPU";

  public static final String RESOURCE_MEMORY = "RESOURCE_MEMORY";

  public static final String RESOURCE_PROFILE = "RESOURCE_PROFILE";

  /**
   * component instance constants.
   */
  public static final String IP = "IP";

  public static final String HOSTNAME = "HOSTNAME";

  public static final String BARE_HOST = "BARE_HOST";

  public static final String COMPONENT_NAME = "COMPONENT_NAME";

  public static final String COMPONENT_INSTANCE_NAME = "COMPONENT_INSTANCE_NAME";

  /**
   * component constants.
   */
  public static final String DEPENDENCIES = "DEPENDENCIES";

  public static final String DESCRIPTION = "DESCRIPTION";

  public static final String RUN_PRIVILEGED_CONTAINER =
      "RUN_PRIVILEGED_CONTAINER";

  public static final String PLACEMENT_POLICY = "PLACEMENT_POLICY";

}
