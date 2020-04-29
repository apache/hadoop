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
public class ApplicationMetricsConstants {

  public static final String ENTITY_TYPE =
      "YARN_APPLICATION";

  public static final String CREATED_EVENT_TYPE =
      "YARN_APPLICATION_CREATED";

  public static final String FINISHED_EVENT_TYPE =
      "YARN_APPLICATION_FINISHED";

  public static final String LAUNCHED_EVENT_TYPE =
      "YARN_APPLICATION_LAUNCHED";

  public static final String ACLS_UPDATED_EVENT_TYPE =
      "YARN_APPLICATION_ACLS_UPDATED";

  public static final String UPDATED_EVENT_TYPE =
      "YARN_APPLICATION_UPDATED";

  public static final String STATE_UPDATED_EVENT_TYPE =
      "YARN_APPLICATION_STATE_UPDATED";

  public static final String NAME_ENTITY_INFO =
      "YARN_APPLICATION_NAME";

  public static final String TYPE_ENTITY_INFO =
      "YARN_APPLICATION_TYPE";

  public static final String USER_ENTITY_INFO =
      "YARN_APPLICATION_USER";

  public static final String QUEUE_ENTITY_INFO =
      "YARN_APPLICATION_QUEUE";

  public static final String SUBMITTED_TIME_ENTITY_INFO =
      "YARN_APPLICATION_SUBMITTED_TIME";

  public static final String APP_VIEW_ACLS_ENTITY_INFO =
      "YARN_APPLICATION_VIEW_ACLS";

  public static final String DIAGNOSTICS_INFO_EVENT_INFO =
      "YARN_APPLICATION_DIAGNOSTICS_INFO";

  public static final String FINAL_STATUS_EVENT_INFO =
      "YARN_APPLICATION_FINAL_STATUS";

  public static final String STATE_EVENT_INFO =
      "YARN_APPLICATION_STATE";

  public static final String APP_CPU_METRICS =
      "YARN_APPLICATION_CPU";

  public static final String APP_MEM_METRICS =
      "YARN_APPLICATION_MEMORY";

  public static final String APP_RESOURCE_PREEMPTED_CPU =
      "YARN_APPLICATION_RESOURCE_PREEMPTED_CPU";

  public static final String APP_RESOURCE_PREEMPTED_MEM =
      "YARN_APPLICATION_RESOURCE_PREEMPTED_MEMORY";

  public static final String APP_NON_AM_CONTAINER_PREEMPTED =
      "YARN_APPLICATION_NON_AM_CONTAINER_PREEMPTED";

  public static final String APP_AM_CONTAINER_PREEMPTED =
      "YARN_APPLICATION_AM_CONTAINER_PREEMPTED";

  public static final String APP_CPU_PREEMPT_METRICS =
      "YARN_APPLICATION_CPU_PREEMPT_METRIC";

  public static final String APP_MEM_PREEMPT_METRICS =
      "YARN_APPLICATION_MEM_PREEMPT_METRIC";

  public static final String LATEST_APP_ATTEMPT_EVENT_INFO =
      "YARN_APPLICATION_LATEST_APP_ATTEMPT";

  public static final String YARN_APP_CALLER_CONTEXT =
      "YARN_APPLICATION_CALLER_CONTEXT";

  public static final String YARN_APP_CALLER_SIGNATURE =
      "YARN_APPLICATION_CALLER_SIGNATURE";

  public static final String APP_TAGS_INFO = "YARN_APPLICATION_TAGS";

  public static final String UNMANAGED_APPLICATION_ENTITY_INFO =
      "YARN_APPLICATION_UNMANAGED_APPLICATION";

  public static final String APPLICATION_PRIORITY_INFO =
      "YARN_APPLICATION_PRIORITY";

  public static final String APP_NODE_LABEL_EXPRESSION =
      "YARN_APP_NODE_LABEL_EXPRESSION";

  public static final String AM_NODE_LABEL_EXPRESSION =
      "YARN_AM_NODE_LABEL_EXPRESSION";

  public static final String AM_CONTAINER_LAUNCH_COMMAND =
      "YARN_AM_CONTAINER_LAUNCH_COMMAND";
}
