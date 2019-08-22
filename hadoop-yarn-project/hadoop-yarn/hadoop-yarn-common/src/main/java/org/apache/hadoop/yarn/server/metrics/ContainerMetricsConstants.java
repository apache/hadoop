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

package org.apache.hadoop.yarn.server.metrics;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

@Private
@Unstable
public class ContainerMetricsConstants {

  public static final String ENTITY_TYPE = "YARN_CONTAINER";

  // Event of this type will be emitted by NM.
  public static final String CREATED_EVENT_TYPE = "YARN_CONTAINER_CREATED";

  // Event of this type will be emitted by RM.
  public static final String CREATED_IN_RM_EVENT_TYPE =
      "YARN_RM_CONTAINER_CREATED";

  // Event of this type will be emitted by NM.
  public static final String PAUSED_EVENT_TYPE = "YARN_CONTAINER_PAUSED";

  // Event of this type will be emitted by NM.
  public static final String RESUMED_EVENT_TYPE = "YARN_CONTAINER_RESUMED";

  // Event of this type will be emitted by NM.
  public static final String KILLED_EVENT_TYPE = "YARN_CONTAINER_KILLED";

  // Event of this type will be emitted by NM.
  public static final String FINISHED_EVENT_TYPE = "YARN_CONTAINER_FINISHED";

  // Event of this type will be emitted by RM.
  public static final String FINISHED_IN_RM_EVENT_TYPE =
      "YARN_RM_CONTAINER_FINISHED";

  public static final String CONTAINER_FINISHED_TIME =
      "YARN_CONTAINER_FINISHED_TIME";

  public static final String PARENT_PRIMARIY_FILTER = "YARN_CONTAINER_PARENT";

  public static final String ALLOCATED_MEMORY_INFO =
      "YARN_CONTAINER_ALLOCATED_MEMORY";

  public static final String ALLOCATED_VCORE_INFO =
      "YARN_CONTAINER_ALLOCATED_VCORE";

  public static final String ALLOCATED_HOST_INFO =
      "YARN_CONTAINER_ALLOCATED_HOST";

  public static final String ALLOCATED_PORT_INFO =
      "YARN_CONTAINER_ALLOCATED_PORT";

  public static final String ALLOCATED_PRIORITY_INFO =
      "YARN_CONTAINER_ALLOCATED_PRIORITY";

  public static final String DIAGNOSTICS_INFO =
      "YARN_CONTAINER_DIAGNOSTICS_INFO";

  public static final String EXIT_STATUS_INFO =
      "YARN_CONTAINER_EXIT_STATUS";

  public static final String STATE_INFO =
      "YARN_CONTAINER_STATE";

  public static final String ALLOCATED_HOST_HTTP_ADDRESS_INFO =
      "YARN_CONTAINER_ALLOCATED_HOST_HTTP_ADDRESS";

  public static final String ALLOCATED_EXPOSED_PORTS =
      "YARN_CONTAINER_ALLOCATED_EXPOSED_PORTS";

  // Event of this type will be emitted by NM.
  public static final String LOCALIZATION_START_EVENT_TYPE =
      "YARN_NM_CONTAINER_LOCALIZATION_STARTED";

  // Event of this type will be emitted by NM.
  public static final String LOCALIZATION_FINISHED_EVENT_TYPE =
      "YARN_NM_CONTAINER_LOCALIZATION_FINISHED";
}
