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

  public static final String CREATED_EVENT_TYPE = "YARN_CONTAINER_CREATED";

  public static final String FINISHED_EVENT_TYPE = "YARN_CONTAINER_FINISHED";

  public static final String PARENT_PRIMARIY_FILTER = "YARN_CONTAINER_PARENT";

  public static final String ALLOCATED_MEMORY_ENTITY_INFO =
      "YARN_CONTAINER_ALLOCATED_MEMORY";

  public static final String ALLOCATED_VCORE_ENTITY_INFO =
      "YARN_CONTAINER_ALLOCATED_VCORE";

  public static final String ALLOCATED_HOST_ENTITY_INFO =
      "YARN_CONTAINER_ALLOCATED_HOST";

  public static final String ALLOCATED_PORT_ENTITY_INFO =
      "YARN_CONTAINER_ALLOCATED_PORT";

  public static final String ALLOCATED_PRIORITY_ENTITY_INFO =
      "YARN_CONTAINER_ALLOCATED_PRIORITY";

  public static final String DIAGNOSTICS_INFO_EVENT_INFO =
      "YARN_CONTAINER_DIAGNOSTICS_INFO";

  public static final String EXIT_STATUS_EVENT_INFO =
      "YARN_CONTAINER_EXIT_STATUS";

  public static final String STATE_EVENT_INFO =
      "YARN_CONTAINER_STATE";

  public static final String ALLOCATED_HOST_HTTP_ADDRESS_ENTITY_INFO =
      "YARN_CONTAINER_ALLOCATED_HOST_HTTP_ADDRESS";
}
