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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

/*
 * Collection of diagnostics.
 */
public class ActivityDiagnosticConstant {
  // EMPTY means it does not have any diagnostic to display.
  // In order not to show "diagnostic" line in frontend,
  // we set the value to null.
  public final static String EMPTY = null;

  /*
   * Initial check diagnostics
   */
  public final static String INIT_CHECK_SINGLE_NODE_REMOVED =
      "Initial check: node has been removed from scheduler";
  public final static String INIT_CHECK_SINGLE_NODE_RESOURCE_INSUFFICIENT =
      "Initial check: node resource is insufficient for minimum allocation";
  public final static String INIT_CHECK_PARTITION_RESOURCE_INSUFFICIENT =
      "Initial check: insufficient resource in partition";

  /*
   * Queue level diagnostics
   */
  public final static String QUEUE_NOT_ABLE_TO_ACCESS_PARTITION =
      "Queue is not able to access partition";
  public final static String QUEUE_HIT_MAX_CAPACITY_LIMIT =
      "Queue hits max-capacity limit";
  public final static String QUEUE_HIT_USER_MAX_CAPACITY_LIMIT =
      "Queue hits user max-capacity limit";
  public final static String QUEUE_DO_NOT_HAVE_ENOUGH_HEADROOM =
      "Queue does not have enough headroom for inner highest-priority request";

  public final static String QUEUE_DO_NOT_NEED_MORE_RESOURCE =
      "Queue does not need more resource";
  public final static String QUEUE_SKIPPED_TO_RESPECT_FIFO = "Queue skipped "
      + "to respect FIFO of applications";
  public final static String QUEUE_SKIPPED_BECAUSE_SINGLE_NODE_RESERVED =
      "Queue skipped because node has been reserved";
  public final static String
      QUEUE_SKIPPED_BECAUSE_SINGLE_NODE_RESOURCE_INSUFFICIENT =
      "Queue skipped because node resource is insufficient";

  /*
   * Application level diagnostics
   */
  public final static String APPLICATION_FAIL_TO_ALLOCATE =
      "Application fails to allocate";
  public final static String APPLICATION_COULD_NOT_GET_CONTAINER =
      "Application couldn't get container for allocation";

  public final static String APPLICATION_DO_NOT_NEED_RESOURCE =
      "Application does not need more resource";

  /*
   * Request level diagnostics
   */
  public final static String REQUEST_SKIPPED_BECAUSE_NULL_ANY_REQUEST =
      "Request skipped because off-switch request is null";
  public final static String REQUEST_SKIPPED_IN_IGNORE_EXCLUSIVITY_MODE =
      "Request skipped in Ignore Exclusivity mode for AM allocation";
  public final static String REQUEST_SKIPPED_BECAUSE_OF_RESERVATION =
      "Request skipped based on reservation algo";
  public final static String
      REQUEST_SKIPPED_BECAUSE_NON_PARTITIONED_PARTITION_FIRST =
      "Request skipped because non-partitioned resource request should be "
          + "scheduled to non-partitioned partition first";
  public final static String REQUEST_DO_NOT_NEED_RESOURCE =
      "Request does not need more resource";

  /*
   * Node level diagnostics
   */
  public final static String
      NODE_SKIPPED_BECAUSE_OF_NO_OFF_SWITCH_AND_LOCALITY_VIOLATION =
      "Node skipped because node/rack locality cannot be satisfied";
  public final static String NODE_SKIPPED_BECAUSE_OF_OFF_SWITCH_DELAY =
      "Node skipped because of off-switch delay";
  public final static String NODE_SKIPPED_BECAUSE_OF_RELAX_LOCALITY =
      "Node skipped because relax locality is not allowed";
  public final static String NODE_TOTAL_RESOURCE_INSUFFICIENT_FOR_REQUEST =
      "Node's total resource is insufficient for request";
  public final static String NODE_DO_NOT_HAVE_SUFFICIENT_RESOURCE =
      "Node does not have sufficient resource for request";
  public final static String NODE_IS_BLACKLISTED = "Node is blacklisted";
  public final static String
      NODE_DO_NOT_MATCH_PARTITION_OR_PLACEMENT_CONSTRAINTS =
      "Node does not match partition or placement constraints";
  public final static String
      NODE_CAN_NOT_FIND_CONTAINER_TO_BE_UNRESERVED_WHEN_NEEDED =
      "Node can't find a container to be unreserved when needed";
}
