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
  public final static String NOT_ABLE_TO_ACCESS_PARTITION =
      "Not able to access partition";
  public final static String QUEUE_DO_NOT_NEED_MORE_RESOURCE =
      "Queue does not need more resource";
  public final static String QUEUE_MAX_CAPACITY_LIMIT =
      "Hit queue max-capacity limit";
  public final static String USER_CAPACITY_MAXIMUM_LIMIT =
      "Hit user capacity maximum limit";
  public final static String SKIP_BLACK_LISTED_NODE = "Skip black listed node";
  public final static String PRIORITY_SKIPPED = "Priority skipped";
  public final static String PRIORITY_SKIPPED_BECAUSE_NULL_ANY_REQUEST =
      "Priority skipped because off-switch request is null";
  public final static String
      PRIORITY_SKIPPED_BECAUSE_NODE_PARTITION_DOES_NOT_MATCH_REQUEST =
      "Priority skipped because partition of node doesn't match request";
  public final static String SKIP_PRIORITY_BECAUSE_OF_RELAX_LOCALITY =
      "Priority skipped because of relax locality is not allowed";
  public final static String SKIP_IN_IGNORE_EXCLUSIVITY_MODE =
      "Skipping assigning to Node in Ignore Exclusivity mode";
  public final static String DO_NOT_NEED_ALLOCATIONATTEMPTINFOS =
      "Doesn't need containers based on reservation algo!";
  public final static String QUEUE_SKIPPED_HEADROOM =
      "Queue skipped because of headroom";
  public final static String NON_PARTITIONED_PARTITION_FIRST =
      "Non-partitioned resource request should be scheduled to "
          + "non-partitioned partition first";
  public final static String SKIP_NODE_LOCAL_REQUEST =
      "Skip node-local request";
  public final static String SKIP_RACK_LOCAL_REQUEST =
      "Skip rack-local request";
  public final static String SKIP_OFF_SWITCH_REQUEST =
      "Skip offswitch request";
  public final static String REQUEST_CAN_NOT_ACCESS_NODE_LABEL =
      "Resource request can not access the label";
  public final static String NOT_SUFFICIENT_RESOURCE =
      "Node does not have sufficient resource for request";
  public final static String LOCALITY_SKIPPED = "Locality skipped";
  public final static String FAIL_TO_ALLOCATE = "Fail to allocate";
  public final static String COULD_NOT_GET_CONTAINER =
      "Couldn't get container for allocation";
  public final static String APPLICATION_DO_NOT_NEED_RESOURCE =
      "Application does not need more resource";
  public final static String APPLICATION_PRIORITY_DO_NOT_NEED_RESOURCE =
      "Application priority does not need more resource";
  public final static String SKIPPED_ALL_PRIORITIES =
      "All priorities are skipped of the app";
  public final static String RESPECT_FIFO = "To respect FIFO of applications, "
      + "skipped following applications in the queue";
}
