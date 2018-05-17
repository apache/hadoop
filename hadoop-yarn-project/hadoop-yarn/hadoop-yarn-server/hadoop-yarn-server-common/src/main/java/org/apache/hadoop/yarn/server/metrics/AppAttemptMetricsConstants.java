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
public class AppAttemptMetricsConstants {

  public static final String ENTITY_TYPE =
      "YARN_APPLICATION_ATTEMPT";

  public static final String REGISTERED_EVENT_TYPE =
      "YARN_APPLICATION_ATTEMPT_REGISTERED";

  public static final String FINISHED_EVENT_TYPE =
      "YARN_APPLICATION_ATTEMPT_FINISHED";

  public static final String PARENT_PRIMARY_FILTER =
      "YARN_APPLICATION_ATTEMPT_PARENT";
      
  public static final String TRACKING_URL_INFO =
      "YARN_APPLICATION_ATTEMPT_TRACKING_URL";

  public static final String ORIGINAL_TRACKING_URL_INFO =
      "YARN_APPLICATION_ATTEMPT_ORIGINAL_TRACKING_URL";

  public static final String HOST_INFO =
      "YARN_APPLICATION_ATTEMPT_HOST";

  public static final String RPC_PORT_INFO =
      "YARN_APPLICATION_ATTEMPT_RPC_PORT";

  public static final String MASTER_CONTAINER_INFO =
      "YARN_APPLICATION_ATTEMPT_MASTER_CONTAINER";

  public static final String DIAGNOSTICS_INFO =
      "YARN_APPLICATION_ATTEMPT_DIAGNOSTICS_INFO";

  public static final String FINAL_STATUS_INFO =
      "YARN_APPLICATION_ATTEMPT_FINAL_STATUS";

  public static final String STATE_INFO =
      "YARN_APPLICATION_ATTEMPT_STATE";

  public static final String MASTER_NODE_ADDRESS =
      "YARN_APPLICATION_ATTEMPT_MASTER_NODE_ADDRESS";

  public static final String MASTER_NODE_ID =
      "YARN_APPLICATION_ATTEMPT_MASTER_NODE_ID";
}
